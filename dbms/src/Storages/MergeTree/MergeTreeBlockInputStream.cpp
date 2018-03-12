#include <Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeBaseBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeReader.h>
#include <Core/Defines.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int NOT_IMPLEMENTED;
}


MergeTreeBlockInputStream::MergeTreeBlockInputStream(
    MergeTreeData & storage_,
    const MergeTreeData::DataPartPtr & owned_data_part_,
    size_t max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    Names column_names,
    const MarkRanges & mark_ranges_,
    bool use_uncompressed_cache_,
    ExpressionActionsPtr prewhere_actions_,
    String prewhere_column_,
    bool check_columns,
    size_t min_bytes_to_use_direct_io_,
    size_t max_read_buffer_size_,
    bool save_marks_in_cache_,
    const Names & virt_column_names,
    size_t part_index_in_query_,
    bool quiet)
    :
    MergeTreeBaseBlockInputStream{storage_, prewhere_actions_, prewhere_column_, max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_, min_bytes_to_use_direct_io_,
        max_read_buffer_size_, use_uncompressed_cache_, save_marks_in_cache_, virt_column_names},
    ordered_names{column_names},
    data_part{owned_data_part_},
    part_columns_lock(data_part->columns_lock),
    all_mark_ranges(mark_ranges_),
    part_index_in_query(part_index_in_query_),
    check_columns(check_columns),
    path(data_part->getFullPath())
{
    /// Let's estimate total number of rows for progress bar.
    size_t total_rows = 0;
    for (const auto & range : all_mark_ranges)
        total_rows += range.end - range.begin;
    total_rows *= storage.index_granularity;

    if (!quiet)
        LOG_TRACE(log, "Reading " << all_mark_ranges.size() << " ranges from part " << data_part->name
        << ", approx. " << total_rows
        << (all_mark_ranges.size() > 1
        ? ", up to " + toString((all_mark_ranges.back().end - all_mark_ranges.front().begin) * storage.index_granularity)
        : "")
        << " rows starting from " << all_mark_ranges.front().begin * storage.index_granularity);

    addTotalRowsApprox(total_rows);

    header = storage.getSampleBlockForColumns(ordered_names);

    /// Types may be different during ALTER (when this stream is used to perform an ALTER).
    /// NOTE: We may use similar code to implement non blocking ALTERs.
    for (const auto & name_type : data_part->columns)
    {
        if (header.has(name_type.name))
        {
            auto & elem = header.getByName(name_type.name);
            if (!elem.type->equals(*name_type.type))
            {
                elem.type = name_type.type;
                elem.column = elem.type->createColumn();
            }
        }
    }

    injectVirtualColumns(header);
}


Block MergeTreeBlockInputStream::getHeader() const
{
    return header;
}


bool MergeTreeBlockInputStream::getNewTask()
try
{
    /// Produce only one task
    if (!is_first_task)
    {
        finish();
        return false;
    }
    is_first_task = false;

    Names pre_column_names, column_names = ordered_names;
    bool remove_prewhere_column = false;

    /// inject columns required for defaults evaluation
    bool should_reorder = !injectRequiredColumns(storage, data_part, column_names).empty();

    if (prewhere_actions)
    {
        pre_column_names = prewhere_actions->getRequiredColumns();

        if (pre_column_names.empty())
            pre_column_names.push_back(column_names[0]);

        const auto injected_pre_columns = injectRequiredColumns(storage, data_part, pre_column_names);
        if (!injected_pre_columns.empty())
            should_reorder = true;

        const NameSet pre_name_set(pre_column_names.begin(), pre_column_names.end());
        /// If the expression in PREWHERE is not a column of the table, you do not need to output a column with it
        ///  (from storage expect to receive only the columns of the table).
        remove_prewhere_column = !pre_name_set.count(prewhere_column_name);

        Names post_column_names;
        for (const auto & name : column_names)
            if (!pre_name_set.count(name))
                post_column_names.push_back(name);

        column_names = post_column_names;
    }

    /// will be used to distinguish between PREWHERE and WHERE columns when applying filter
    column_name_set = NameSet{column_names.begin(), column_names.end()};

    if (check_columns)
    {
        /// Under owned_data_part->columns_lock we check that all requested columns are of the same type as in the table.
        /// This may be not true in case of ALTER MODIFY.
        if (!pre_column_names.empty())
            storage.check(data_part->columns, pre_column_names);
        if (!column_names.empty())
            storage.check(data_part->columns, column_names);

        pre_columns = storage.columns.getList().addTypes(pre_column_names);
        columns = storage.columns.getList().addTypes(column_names);
    }
    else
    {
        pre_columns = data_part->columns.addTypes(pre_column_names);
        columns = data_part->columns.addTypes(column_names);
    }

    /** @note you could simply swap `reverse` in if and else branches of MergeTreeDataSelectExecutor,
     * and remove this reverse. */
    MarkRanges remaining_mark_ranges = all_mark_ranges;
    std::reverse(remaining_mark_ranges.begin(), remaining_mark_ranges.end());

    auto size_predictor = (preferred_block_size_bytes == 0) ? nullptr
                          : std::make_unique<MergeTreeBlockSizePredictor>(data_part, ordered_names, data_part->storage.getSampleBlock());

    task = std::make_unique<MergeTreeReadTask>(data_part, remaining_mark_ranges, part_index_in_query, ordered_names,
                                               column_name_set, columns, pre_columns, remove_prewhere_column, should_reorder,
                                               std::move(size_predictor));

    if (!reader)
    {
        if (use_uncompressed_cache)
            owned_uncompressed_cache = storage.context.getUncompressedCache();

        owned_mark_cache = storage.context.getMarkCache();

        reader = std::make_unique<MergeTreeReader>(
            path, data_part, columns, owned_uncompressed_cache.get(),
            owned_mark_cache.get(), save_marks_in_cache, storage,
            all_mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size);

        if (prewhere_actions)
            pre_reader = std::make_unique<MergeTreeReader>(
                path, data_part, pre_columns, owned_uncompressed_cache.get(),
                owned_mark_cache.get(), save_marks_in_cache, storage,
                all_mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size);
    }

    return true;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part->name);
    throw;
}


void MergeTreeBlockInputStream::finish()
{
    /** Close the files (before destroying the object).
    * When many sources are created, but simultaneously reading only a few of them,
    * buffers don't waste memory.
    */
    reader.reset();
    pre_reader.reset();
    part_columns_lock.unlock();
    data_part.reset();
}


MergeTreeBlockInputStream::~MergeTreeBlockInputStream() = default;


}
