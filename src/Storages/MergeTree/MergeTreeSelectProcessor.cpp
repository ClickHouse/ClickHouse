#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Interpreters/Context.h>


namespace DB
{

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
    ExpressionActionsSettings actions_settings,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & virt_column_names_,
    size_t part_index_in_query_,
    bool has_limit_below_one_block_,
    std::optional<ParallelReadingExtension> extension_)
    : MergeTreeBaseSelectProcessor{
        metadata_snapshot_->getSampleBlockForColumns(required_columns_, storage_.getVirtuals(), storage_.getStorageID()),
        storage_, metadata_snapshot_, prewhere_info_, std::move(actions_settings), max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_,
        reader_settings_, use_uncompressed_cache_, virt_column_names_, extension_},
    required_columns{std::move(required_columns_)},
    data_part{owned_data_part_},
    sample_block(metadata_snapshot_->getSampleBlock()),
    all_mark_ranges(std::move(mark_ranges_)),
    part_index_in_query(part_index_in_query_),
    has_limit_below_one_block(has_limit_below_one_block_),
    total_rows(data_part->index_granularity.getRowsCountInRanges(all_mark_ranges))
{
    /// Actually it means that parallel reading from replicas enabled
    /// and we have to collaborate with initiator.
    /// In this case we won't set approximate rows, because it will be accounted multiple times
    if (!extension_.has_value())
        addTotalRowsApprox(total_rows);
    ordered_names = header_without_virtual_columns.getNames();
}

void MergeTreeSelectProcessor::initializeReaders()
{
    task_columns = getReadTaskColumns(
        storage, metadata_snapshot, data_part,
        required_columns, prewhere_info);

    /// Will be used to distinguish between PREWHERE and WHERE columns when applying filter
    const auto & column_names = task_columns.columns.getNames();
    column_name_set = NameSet{column_names.begin(), column_names.end()};

    if (use_uncompressed_cache)
        owned_uncompressed_cache = storage.getContext()->getUncompressedCache();

    owned_mark_cache = storage.getContext()->getMarkCache();

    reader = data_part->getReader(task_columns.columns, metadata_snapshot, all_mark_ranges,
        owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings);

    if (prewhere_info)
        pre_reader = data_part->getReader(task_columns.pre_columns, metadata_snapshot, all_mark_ranges,
            owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings);

}


void MergeTreeSelectProcessor::processNewTask()
{
    const size_t max_batch_size = estimateMaxBatchSizeForHugeRanges();

    size_t current_batch_size = 0;
    buffered_ranges.emplace_back();

    for (const auto & range : task->mark_ranges)
    {
        auto expand_if_needed = [&]
        {
            if (current_batch_size > max_batch_size)
            {
                buffered_ranges.emplace_back();
                current_batch_size = 0;
            }

        };

        expand_if_needed();

        if (range.end - range.begin < max_batch_size)
        {
            buffered_ranges.back().push_back(range);
            current_batch_size += range.end - range.begin;
            continue;
        }

        auto current_begin = range.begin;
        auto current_end = range.begin + max_batch_size;

        while (current_end < range.end)
        {
            auto current_range = MarkRange{current_begin, current_end};
            buffered_ranges.back().push_back(current_range);
            current_batch_size += current_end - current_begin;

            current_begin = current_end;
            current_end = current_end + max_batch_size;

            expand_if_needed();
        }

        if (range.end - current_begin > 0)
        {
            auto current_range = MarkRange{current_begin, range.end};

            buffered_ranges.back().push_back(current_range);
            current_batch_size += range.end - current_begin;

            /// Do not need to update current_begin and current_end

            expand_if_needed();
        }
    }

    if (buffered_ranges.back().empty())
        buffered_ranges.pop_back();
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
