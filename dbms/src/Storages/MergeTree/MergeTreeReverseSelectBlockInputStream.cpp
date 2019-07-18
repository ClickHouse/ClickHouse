#include <Storages/MergeTree/MergeTreeReverseSelectBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeBaseSelectBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeReader.h>
#include <Core/Defines.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}


MergeTreeReverseSelectBlockInputStream::MergeTreeReverseSelectBlockInputStream(
    const MergeTreeData & storage_,
    const MergeTreeData::DataPartPtr & owned_data_part_,
    UInt64 max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    Names column_names,
    const MarkRanges & mark_ranges_,
    bool use_uncompressed_cache_,
    const PrewhereInfoPtr & prewhere_info_,
    bool check_columns,
    size_t min_bytes_to_use_direct_io_,
    size_t max_read_buffer_size_,
    bool save_marks_in_cache_,
    const Names & virt_column_names_,
    size_t part_index_in_query_,
    bool quiet)
    :
    MergeTreeBaseSelectBlockInputStream{storage_, prewhere_info_, max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_, min_bytes_to_use_direct_io_,
        max_read_buffer_size_, use_uncompressed_cache_, save_marks_in_cache_, virt_column_names_},
    required_columns{column_names},
    data_part{owned_data_part_},
    part_columns_lock(data_part->columns_lock),
    all_mark_ranges(mark_ranges_),
    part_index_in_query(part_index_in_query_),
    check_columns(check_columns),
    path(data_part->getFullPath())
{
    /// Let's estimate total number of rows for progress bar.
    for (const auto & range : all_mark_ranges)
        total_marks_count += range.end - range.begin;

    size_t total_rows = data_part->index_granularity.getTotalRows();

    if (!quiet)
        LOG_TRACE(log, "Reading " << all_mark_ranges.size() << " ranges in reverse order from part " << data_part->name
        << ", approx. " << total_rows
        << (all_mark_ranges.size() > 1
        ? ", up to " + toString(data_part->index_granularity.getRowsCountInRanges(all_mark_ranges))
        : "")
        << " rows starting from " << data_part->index_granularity.getMarkStartingRow(all_mark_ranges.front().begin));

    addTotalRowsApprox(total_rows);
    header = storage.getSampleBlockForColumns(required_columns);

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

    executePrewhereActions(header, prewhere_info);
    injectVirtualColumns(header);

    ordered_names = getHeader().getNames();

    Names pre_column_names;

    /// inject columns required for defaults evaluation
    should_reorder = !injectRequiredColumns(storage, data_part, required_columns).empty();

    if (prewhere_info)
    {
        if (prewhere_info->alias_actions)
            pre_column_names = prewhere_info->alias_actions->getRequiredColumns();
        else
            pre_column_names = prewhere_info->prewhere_actions->getRequiredColumns();

        if (pre_column_names.empty())
            pre_column_names.push_back(required_columns[0]);

        const auto injected_pre_columns = injectRequiredColumns(storage, data_part, pre_column_names);
        if (!injected_pre_columns.empty())
            should_reorder = true;

        const NameSet pre_name_set(pre_column_names.begin(), pre_column_names.end());

        Names post_column_names;
        for (const auto & name : required_columns)
            if (!pre_name_set.count(name))
                post_column_names.push_back(name);

        required_columns = post_column_names;
    }

    /// will be used to distinguish between PREWHERE and WHERE columns when applying filter
    column_name_set = NameSet{required_columns.begin(), required_columns.end()};

    if (check_columns)
    {
        /// Under owned_data_part->columns_lock we check that all requested columns are of the same type as in the table.
        /// This may be not true in case of ALTER MODIFY.
        if (!pre_column_names.empty())
            storage.check(data_part->columns, pre_column_names);
        if (!required_columns.empty())
            storage.check(data_part->columns, required_columns);

        const NamesAndTypesList & physical_columns = storage.getColumns().getAllPhysical();
        pre_columns = physical_columns.addTypes(pre_column_names);
        columns = physical_columns.addTypes(column_names);
    }
    else
    {
        pre_columns = data_part->columns.addTypes(pre_column_names);
        columns = data_part->columns.addTypes(required_columns);
    }

    if (use_uncompressed_cache)
        owned_uncompressed_cache = storage.global_context.getUncompressedCache();

    owned_mark_cache = storage.global_context.getMarkCache();

    reader = std::make_unique<MergeTreeReader>(
        path, data_part, columns, owned_uncompressed_cache.get(),
        owned_mark_cache.get(), save_marks_in_cache, storage,
        all_mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size);

    if (prewhere_info)
        pre_reader = std::make_unique<MergeTreeReader>(
            path, data_part, pre_columns, owned_uncompressed_cache.get(),
            owned_mark_cache.get(), save_marks_in_cache, storage,
            all_mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size);
}


Block MergeTreeReverseSelectBlockInputStream::getHeader() const
{
    return header;
}


bool MergeTreeReverseSelectBlockInputStream::getNewTask()
try
{
    if ((blocks.empty() && all_mark_ranges.empty()) || total_marks_count == 0)
    {
        finish();
        return false;
    }

    /// We have some blocks to return in buffer. 
    /// Return true to continue reading, but actually don't create a task.
    if (all_mark_ranges.empty())
        return true;

    /// Read ranges from right to left.
    MarkRanges mark_ranges_for_task = { all_mark_ranges.back() };
    all_mark_ranges.pop_back();

    auto size_predictor = (preferred_block_size_bytes == 0)
        ? nullptr
        : std::make_unique<MergeTreeBlockSizePredictor>(data_part, ordered_names, data_part->storage.getSampleBlock());

    task = std::make_unique<MergeTreeReadTask>(
        data_part, mark_ranges_for_task, part_index_in_query, ordered_names, column_name_set, columns, pre_columns,
        prewhere_info && prewhere_info->remove_prewhere_column, should_reorder, std::move(size_predictor));

    return true;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part->name);
    throw;
}

Block MergeTreeReverseSelectBlockInputStream::readFromPart()
{
    Block res;

    if (!blocks.empty())
    {
        res = std::move(blocks.back());
        blocks.pop_back();
        return res;
    }

    if (!task->range_reader.isInitialized())
        initializeRangeReaders(*task);

    while (!task->isFinished())
    {
        Block block = readFromPartImpl();
        blocks.push_back(std::move(block));
    }

    if (blocks.empty())
        return {};

    res = std::move(blocks.back());
    blocks.pop_back();

    return res;
}

void MergeTreeReverseSelectBlockInputStream::finish()
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


MergeTreeReverseSelectBlockInputStream::~MergeTreeReverseSelectBlockInputStream() = default;


}
