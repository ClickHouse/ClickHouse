#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeSelectProcessor::MergeTreeSelectProcessor(
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
    bool check_columns_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & virt_column_names_,
    size_t part_index_in_query_,
    bool quiet)
    :
    MergeTreeBaseSelectProcessor{
        metadata_snapshot_->getSampleBlockForColumns(required_columns_, storage_.getVirtuals(), storage_.getStorageID()),
        storage_, metadata_snapshot_, prewhere_info_, max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_,
        reader_settings_, use_uncompressed_cache_, virt_column_names_},
    required_columns{std::move(required_columns_)},
    data_part{owned_data_part_},
    all_mark_ranges(std::move(mark_ranges_)),
    part_index_in_query(part_index_in_query_),
    check_columns(check_columns_)
{
    /// Let's estimate total number of rows for progress bar.
    for (const auto & range : all_mark_ranges)
        total_marks_count += range.end - range.begin;

    size_t total_rows = data_part->index_granularity.getRowsCountInRanges(all_mark_ranges);

    if (!quiet)
        LOG_TRACE(log, "Reading {} ranges from part {}, approx. {} rows starting from {}",
            all_mark_ranges.size(), data_part->name, total_rows,
            data_part->index_granularity.getMarkStartingRow(all_mark_ranges.front().begin));

    addTotalRowsApprox(total_rows);
    ordered_names = header_without_virtual_columns.getNames();
}


bool MergeTreeSelectProcessor::getNewTask()
try
{
    /// Produce no more than one task
    if (!is_first_task || total_marks_count == 0)
    {
        finish();
        return false;
    }
    is_first_task = false;

    task_columns = getReadTaskColumns(
        storage, metadata_snapshot, data_part,
        required_columns, prewhere_info, check_columns);

    auto size_predictor = (preferred_block_size_bytes == 0)
        ? nullptr
        : std::make_unique<MergeTreeBlockSizePredictor>(data_part, ordered_names, metadata_snapshot->getSampleBlock());

    /// will be used to distinguish between PREWHERE and WHERE columns when applying filter
    const auto & column_names = task_columns.columns.getNames();
    column_name_set = NameSet{column_names.begin(), column_names.end()};

    task = std::make_unique<MergeTreeReadTask>(
        data_part, all_mark_ranges, part_index_in_query, ordered_names, column_name_set, task_columns.columns,
        task_columns.pre_columns, prewhere_info && prewhere_info->remove_prewhere_column,
        task_columns.should_reorder, std::move(size_predictor));

    if (!reader)
    {
        if (use_uncompressed_cache)
            owned_uncompressed_cache = storage.global_context.getUncompressedCache();

        owned_mark_cache = storage.global_context.getMarkCache();

        reader = data_part->getReader(task_columns.columns, metadata_snapshot, all_mark_ranges,
            owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings);

        if (prewhere_info)
            pre_reader = data_part->getReader(task_columns.pre_columns, metadata_snapshot, all_mark_ranges,
                owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings);
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


void MergeTreeSelectProcessor::finish()
{
    /** Close the files (before destroying the object).
    * When many sources are created, but simultaneously reading only a few of them,
    * buffers don't waste memory.
    */
    reader.reset();
    pre_reader.reset();
    data_part.reset();
}


MergeTreeSelectProcessor::~MergeTreeSelectProcessor() = default;


}
