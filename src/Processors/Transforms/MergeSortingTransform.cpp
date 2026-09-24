#include <Processors/Transforms/MergeSortingTransform.h>

#include <algorithm>

#include <Processors/Transforms/BufferingFileTransforms.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Common/Exception.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event ExternalSortMerge;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeSortingTransform::MergeSortingTransform(
    SharedHeader header,
    const SortDescription & description_,
    size_t max_merged_block_size_,
    size_t max_block_bytes_,
    UInt64 limit_,
    bool increase_sort_description_compile_attempts,
    size_t max_bytes_before_remerge_,
    double remerge_lowered_memory_bytes_ratio_,
    size_t max_bytes_in_block_before_external_sort_,
    size_t max_bytes_in_query_before_external_sort_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t min_free_disk_space_,
    size_t max_external_merge_fan_in_,
    TopKThresholdTrackerPtr threshold_tracker_)
    : SortingTransform(header, description_, max_merged_block_size_, limit_, increase_sort_description_compile_attempts)
    , max_bytes_before_remerge(max_bytes_before_remerge_)
    , remerge_lowered_memory_bytes_ratio(remerge_lowered_memory_bytes_ratio_)
    , max_bytes_in_block_before_external_sort(max_bytes_in_block_before_external_sort_)
    , max_bytes_in_query_before_external_sort(max_bytes_in_query_before_external_sort_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
    , max_block_bytes(max_block_bytes_)
    , max_external_merge_fan_in(max_external_merge_fan_in_)
    , threshold_tracker(threshold_tracker_)
{
}

IProcessor::PipelineUpdate MergeSortingTransform::updatePipeline()
{
    if (stage == Stage::Serialize)
    {
        if (!processors.empty())
        {
            outputs.emplace_back(header_without_constants, this);
            connect(outputs.back(), write_sink->getPort());
            inputs.emplace_back(Block(), this);
            connect(write_sink->getCompletionPort(), inputs.back());
            return {.to_add = std::move(processors), .to_remove = {}};
        }

        runs.emplace_back(write_sink->releaseFile());
        disconnect(outputs.back(), write_sink->getPort());
        disconnect(write_sink->getCompletionPort(), inputs.back());
        outputs.pop_back();
        inputs.pop_back();
        Processors finished{std::move(write_sink)};
        stage = Stage::Consume;
        return {.to_add = {}, .to_remove = std::move(finished)};
    }

    inputs.emplace_back(header_without_constants, this);
    connect(processors.front()->getOutputs().front(), inputs.back());
    return {.to_add = std::move(processors), .to_remove = {}};
}

IProcessor::Status MergeSortingTransform::prepareSerialize()
{
    auto status = SortingTransform::prepareSerialize();
    if (status != Status::Finished)
        return status;
    auto & completion = inputs.back();
    if (!completion.isFinished())
    {
        completion.setNeeded();
        return Status::NeedData;
    }
    return Status::UpdatePipeline;
}

void MergeSortingTransform::consume(Chunk chunk)
{

    /// Accumulate sorted input chunks in memory. When the configured spill thresholds are exceeded,
    /// merge the buffered chunks into a sorted stream and write it to a temporary file. At the end of
    /// input, `ExternalMergeSource` reduces excess files through intermediate merges, then merges the
    /// remaining files with the chunks still in memory. Without spill files, merge only in memory.

    /// If there were only const columns in sort description, then there is no need to sort.
    /// Return the chunk as is.
    if (description.empty())
    {
        generated_chunk = std::move(chunk);
        return;
    }

    removeConstColumns(chunk);
    compactReplicatedColumns(chunk);

    sum_rows_in_blocks += chunk.getNumRows();
    sum_bytes_in_blocks += chunk.allocatedBytes();
    chunks.push_back(std::move(chunk));

    /// Remerge buffered chunks when the row limit can discard enough rows to reduce memory use.
    if ((chunks.size() > 1
        && limit
        && limit * 2 < sum_rows_in_blocks   /// 2 is just a guess.
        && remerge_is_useful
        && max_bytes_before_remerge
        && sum_bytes_in_blocks > max_bytes_before_remerge) || (threshold_tracker && (static_cast<double>(sum_rows_in_blocks) > static_cast<double>(limit) * 1.5)))
    {
        remerge();
    }

    /// Spill buffered chunks as one sorted, compressed file when both enabled memory thresholds
    /// are exceeded. The temporary-file reservation below checks the available disk space.
    if (max_bytes_in_block_before_external_sort && sum_bytes_in_blocks > max_bytes_in_block_before_external_sort)
    {
        Int64 query_memory = getCurrentQueryMemoryUsage();
        if (!max_bytes_in_query_before_external_sort || query_memory > static_cast<Int64>(max_bytes_in_query_before_external_sort))
        {
            if (!tmp_data)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryDataOnDisk is not set for MergeSortingTransform");

            LOG_TRACE(log, "Will dump sorting block ({}, limit: {}) to disk (query memory: {}, limit: {})",
                formatReadableSizeWithBinarySuffix(sum_bytes_in_blocks),
                formatReadableSizeWithBinarySuffix(max_bytes_in_block_before_external_sort),
                formatReadableSizeWithBinarySuffix(query_memory),
                formatReadableSizeWithBinarySuffix(max_bytes_in_query_before_external_sort));

            /// Reserving the buffered size plus `min_free_disk_space` enforces the free-space requirement.
            size_t reserve_size = sum_bytes_in_blocks + min_free_disk_space;
            SharedHeader shared_header_without_constants = std::make_shared<const Block>(header_without_constants);
            TemporaryBlockStreamHolder tmp_stream(shared_header_without_constants, tmp_data, reserve_size);
            size_t max_merged_block_size = this->max_merged_block_size;
            if (max_block_bytes > 0 && sum_rows_in_blocks > 0 && sum_bytes_in_blocks > 0)
            {
                auto avg_row_bytes = sum_bytes_in_blocks / sum_rows_in_blocks;

                /// Keep at least 128 rows per merged block when adjusting for the preferred byte size.
                max_merged_block_size = std::max(std::min(max_merged_block_size, max_block_bytes / avg_row_bytes), 128UL);
            }
            merge_sorter = std::make_unique<MergeSorter>(shared_header_without_constants, std::move(chunks), description, max_merged_block_size, limit);
            write_sink = std::make_shared<BufferingToFileSink>(shared_header_without_constants, std::move(tmp_stream), log);
            processors.emplace_back(write_sink);
            if (runs.empty())
                external_merge_block_size = max_merged_block_size;

            stage = Stage::Serialize;
            sum_bytes_in_blocks = 0;
            sum_rows_in_blocks = 0;
        }
    }
}

void MergeSortingTransform::serialize()
{
    current_chunk = merge_sorter->read();
    if (!current_chunk)
        merge_sorter.reset();
}

void MergeSortingTransform::generate()
{
    if (!generated_prefix)
    {
        if (runs.empty())
        {
            merge_sorter = std::make_unique<MergeSorter>(std::make_shared<const Block>(header_without_constants), std::move(chunks), description, max_merged_block_size, limit);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::ExternalSortMerge);
            LOG_INFO(log, "There are {} temporary sorted parts to merge", runs.size());

            auto header = std::make_shared<const Block>(header_without_constants);
            SourcePtr tail;
            if (!chunks.empty())
                tail = std::make_shared<MergeSorterSource>(header, std::move(chunks), description, max_merged_block_size, limit);
            auto merge = [header, description = description, block_size = external_merge_block_size, limit = limit](
                             const SharedHeaders & headers) -> ProcessorPtr
            {
                return std::make_shared<MergingSortedTransform>(
                    header,
                    headers.size(),
                    description,
                    block_size,
                    /*max_block_size_bytes=*/0,
                    /*max_dynamic_subcolumns=*/std::nullopt,
                    SortingQueueStrategy::Batch,
                    limit,
                    /*always_read_till_end_=*/false,
                    /*out_row_sources_buf_=*/nullptr,
                    /*filter_column_name_=*/std::nullopt,
                    /*use_average_block_sizes=*/false,
                    /*apply_virtual_row_conversions=*/false);
            };
            std::vector<ExternalMergeSource::Group> groups;
            groups.emplace_back(std::move(runs), merge);
            processors.emplace_back(
                std::make_shared<ExternalMergeSource>(
                    header, std::move(groups), std::move(tail), merge, max_external_merge_fan_in, tmp_data, min_free_disk_space, log));
        }

        generated_prefix = true;
    }

    if (merge_sorter)
    {
        generated_chunk = merge_sorter->read();
        if (!generated_chunk)
            merge_sorter.reset();
        else
            enrichChunkWithConstants(generated_chunk);
    }
}

void MergeSortingTransform::remerge()
{
    LOG_DEBUG(log, "Re-merging intermediate ORDER BY data ({} blocks with {} rows) to save memory consumption", chunks.size(), sum_rows_in_blocks);

    /// NOTE Maybe concat all blocks and partial sort will be faster than merge?
    MergeSorter remerge_sorter(std::make_shared<const Block>(header_without_constants), std::move(chunks), description, max_merged_block_size, limit);

    Chunks new_chunks;
    size_t new_sum_rows_in_blocks = 0;
    size_t new_sum_bytes_in_blocks = 0;

    while (auto chunk = remerge_sorter.read())
    {
        new_sum_rows_in_blocks += chunk.getNumRows();
        new_sum_bytes_in_blocks += chunk.allocatedBytes();
        new_chunks.emplace_back(std::move(chunk));
    }

    LOG_DEBUG(log, "Memory usage is lowered from {} to {}", ReadableSize(sum_bytes_in_blocks), ReadableSize(new_sum_bytes_in_blocks));

    /// If the memory consumption was not lowered enough - we will not perform remerge anymore.
    if (remerge_lowered_memory_bytes_ratio > 0.0 && (static_cast<double>(new_sum_bytes_in_blocks) * remerge_lowered_memory_bytes_ratio > static_cast<double>(sum_bytes_in_blocks)))
    {
        remerge_is_useful = false;
        LOG_DEBUG(log, "Re-merging is not useful (memory usage was not lowered by remerge_sort_lowered_memory_bytes_ratio={})", remerge_lowered_memory_bytes_ratio);
    }

    chunks = std::move(new_chunks);
    sum_rows_in_blocks = new_sum_rows_in_blocks;
    sum_bytes_in_blocks = new_sum_bytes_in_blocks;

    /// Publish the updated TopK value if optimization is ON
    if (threshold_tracker && sum_rows_in_blocks == limit && chunks.size() == 1)
    {
        Field value;
        /// Chunk columns follow `header_without_constants` order; the first sort column
        /// is not necessarily at position 0 (e.g. lazy materialization can place a
        /// WHERE-only column before it). Resolve its actual position by name.
        chassert(!description.empty());
        size_t sort_column_position = header_without_constants.getPositionByName(description.front().column_name);
        chunks[0].getColumns()[sort_column_position]->get(limit - 1, value);
        threshold_tracker->testAndSet(value);
        LOG_DEBUG(log, "TopK threshold tracker is updated");
    }
}

}
