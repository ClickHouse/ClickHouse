#include <Storages/MergeTree/MergeTreeSelectBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeBaseSelectBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeReader.h>
#include <Core/Defines.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}


MergeTreeSelectBlockInputStream::MergeTreeSelectBlockInputStream(
    const MergeTreeData & storage_,
    const MergeTreeData::DataPartPtr & owned_data_part_,
    UInt64 max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    Names required_columns_,
    const MarkRanges & mark_ranges_,
    bool use_uncompressed_cache_,
    const PrewhereInfoPtr & prewhere_info_,
    bool check_columns_,
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
    required_columns{required_columns_},
    data_part{owned_data_part_},
    part_columns_lock(data_part->columns_lock),
    all_mark_ranges(mark_ranges_),
    part_index_in_query(part_index_in_query_),
    check_columns(check_columns_),
    path(data_part->getFullPath())
{
    /// Let's estimate total number of rows for progress bar.
    for (const auto & range : all_mark_ranges)
        total_marks_count += range.end - range.begin;

    size_t total_rows = data_part->index_granularity.getRowsCountInRanges(all_mark_ranges);

    if (!quiet)
        LOG_TRACE(log, "Reading " << all_mark_ranges.size() << " ranges from part " << data_part->name
        << ", approx. " << total_rows
        << (all_mark_ranges.size() > 1
        ? ", up to " + toString(total_rows)
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
}


Block MergeTreeSelectBlockInputStream::getHeader() const
{
    return header;
}


bool MergeTreeSelectBlockInputStream::getNewTask()
try
{
    /// Produce no more than one task
    if (!is_first_task || total_marks_count == 0)
    {
        finish();
        return false;
    }
    is_first_task = false;

    task_columns = getReadTaskColumns(storage, data_part, required_columns, prewhere_info, check_columns);

    /** @note you could simply swap `reverse` in if and else branches of MergeTreeDataSelectExecutor,
     * and remove this reverse. */
    MarkRanges remaining_mark_ranges = all_mark_ranges;
    std::reverse(remaining_mark_ranges.begin(), remaining_mark_ranges.end());

    auto size_predictor = (preferred_block_size_bytes == 0)
        ? nullptr
        : std::make_unique<MergeTreeBlockSizePredictor>(data_part, ordered_names, data_part->storage.getSampleBlock());

    /// will be used to distinguish between PREWHERE and WHERE columns when applying filter
    const auto & column_names = task_columns.columns.getNames();
    column_name_set = NameSet{column_names.begin(), column_names.end()};

    task = std::make_unique<MergeTreeReadTask>(
        data_part, remaining_mark_ranges, part_index_in_query, ordered_names, column_name_set, task_columns.columns,
        task_columns.pre_columns, prewhere_info && prewhere_info->remove_prewhere_column,
        task_columns.should_reorder, std::move(size_predictor));

    if (!reader)
    {
        if (use_uncompressed_cache)
            owned_uncompressed_cache = storage.global_context.getUncompressedCache();

        owned_mark_cache = storage.global_context.getMarkCache();

        reader = std::make_unique<MergeTreeReader>(
            path, data_part, task_columns.columns, owned_uncompressed_cache.get(),
            owned_mark_cache.get(), save_marks_in_cache, storage,
            all_mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size);

        if (prewhere_info)
            pre_reader = std::make_unique<MergeTreeReader>(
                path, data_part, task_columns.pre_columns, owned_uncompressed_cache.get(),
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


void MergeTreeSelectBlockInputStream::finish()
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


MergeTreeSelectBlockInputStream::~MergeTreeSelectBlockInputStream() = default;


}
