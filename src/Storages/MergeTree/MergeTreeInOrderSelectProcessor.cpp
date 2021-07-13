#include <Storages/MergeTree/MergeTreeInOrderSelectProcessor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeInOrderSelectProcessor::MergeTreeInOrderSelectProcessor(
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeData::DataPartPtr & owned_data_part_,
    UInt64 max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    Names required_columns_,
    MarkRanges mark_ranges_,
    bool use_uncompressed_cache_,
    const PrewhereInfoPtr & prewhere_info_,
    ExpressionActionsSettings actions_settings,
    bool check_columns_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & virt_column_names_,
    bool has_limit_below_one_block_,
    bool quiet)
    : MergeTreeSelectProcessor{
        storage_, metadata_snapshot_, owned_data_part_, max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_,
        required_columns_, std::move(mark_ranges_), use_uncompressed_cache_, prewhere_info_,
        std::move(actions_settings), check_columns_, reader_settings_, virt_column_names_, has_limit_below_one_block_}
{
    if (!quiet)
        LOG_DEBUG(log, "Reading {} ranges in order from part {}, approx. {} rows starting from {}",
            all_mark_ranges.size(), data_part->name, total_rows,
            data_part->index_granularity.getMarkStartingRow(all_mark_ranges.front().begin));
}

bool MergeTreeInOrderSelectProcessor::getNewTask()
try
{
    if (all_mark_ranges.empty())
    {
        finish();
        return false;
    }

    auto size_predictor = (preferred_block_size_bytes == 0)
        ? nullptr
        : std::make_unique<MergeTreeBlockSizePredictor>(data_part, ordered_names, metadata_snapshot->getSampleBlock());

    MarkRanges mark_ranges_for_task;
    /// If we need to read few rows, set one range per task to reduce number of read data.
    if (has_limit_below_one_block)
    {
        mark_ranges_for_task = { std::move(all_mark_ranges.front()) };
        all_mark_ranges.pop_front();
    }
    else
    {
        mark_ranges_for_task = std::move(all_mark_ranges);
        all_mark_ranges.clear();
    }

    task = std::make_unique<MergeTreeReadTask>(
        data_part, mark_ranges_for_task, part_index_in_query, ordered_names, column_name_set, task_columns.columns,
        task_columns.pre_columns, prewhere_info && prewhere_info->remove_prewhere_column,
        task_columns.should_reorder, std::move(size_predictor));

    return true;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part->name);
    throw;
}

}
