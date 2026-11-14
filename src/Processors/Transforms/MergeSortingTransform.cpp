#include <Processors/Transforms/MergeSortingTransform.h>

#include <algorithm>
#include <iterator>
#include <utility>

#include <Processors/Transforms/BufferingFileTransforms.h>
#include <Processors/Merges/DistinctSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Common/Exception.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>


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
    TopKThresholdTrackerPtr threshold_tracker_,
    MergeSorter::Mode merge_mode_,
    ProfileEvents::Event external_merge_event_,
    SortingQueryResultPreviewsPtr query_result_previews_)
    : SortingTransform(header, description_, max_merged_block_size_, limit_, increase_sort_description_compile_attempts)
    , max_bytes_before_remerge(max_bytes_before_remerge_)
    , remerge_lowered_memory_bytes_ratio(remerge_lowered_memory_bytes_ratio_)
    , max_bytes_in_block_before_external_sort(max_bytes_in_block_before_external_sort_)
    , max_bytes_in_query_before_external_sort(max_bytes_in_query_before_external_sort_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
    , max_block_bytes(max_block_bytes_)
    , threshold_tracker(threshold_tracker_)
    , query_result_previews(std::move(query_result_previews_))
    , merge_mode(merge_mode_)
    , external_merge_event(external_merge_event_)
{
    chassert(merge_mode == MergeSorter::Mode::PreserveRows || limit_ == 0);
    if (query_result_previews)
    {
        preview_participant_index = query_result_previews->participants.size();
        query_result_previews->participants.push_back(this);
    }
}

QueryResultPreviewsControl * MergeSortingTransform::getQueryResultPreviewsControl()
{
    return query_result_previews ? &query_result_previews->control : nullptr;
}

IProcessor::PipelineUpdate MergeSortingTransform::updatePipeline()
{
    if (inputs.size() == 1)
    {
        /// Add external_merging_sorted.
        inputs.emplace_back(header_without_constants, this);
        connect(external_merging_sorted->getOutputs().front(), inputs.back());
        processors.emplace_back(external_merging_sorted);
    }

    auto & source = processors.front();

    external_merging_sorted->addInput(header_without_constants);
    connect(source->getOutputs().back(), external_merging_sorted->getInputs().back());

    if (stage == Stage::Serialize)
    {
        auto & sink = *std::next(processors.begin());
        /// Serialize
        outputs.emplace_back(header_without_constants, this);
        connect(sink->getOutputs().front(), source->getInputs().front());
        connect(getOutputs().back(), sink->getInputs().back());
    }

    for (auto & spilled_source : spilled_sources)
    {
        external_merging_sorted->addInput(header_without_constants);
        connect(spilled_source->getOutputs().front(), external_merging_sorted->getInputs().back());
        processors.emplace_back(std::move(spilled_source));
    }
    spilled_sources.clear();

    if (stage == Stage::Generate)
        external_merging_sorted->setHaveAllInputs();

    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

void MergeSortingTransform::consume(Chunk chunk)
{
    /** Algorithm:
      * - read to memory blocks from source stream;
      * - if too many of them and if external sorting is enabled,
      *   - merge all blocks to sorted stream and write it to temporary file;
      * - at the end, merge all sorted streams from temporary files and also from rest of blocks in memory.
      */

    /// A query result preview is a self-contained chunk (see `QueryResultPreview.h`), already
    /// sorted and cut to the limit by `PartialSortingTransform`. Pass it along without mixing it
    /// into the accumulated state. If the single output slot is occupied, drop the preview:
    /// previews are best effort and a newer one will follow.
    if (isQueryResultPreview(chunk))
    {
        if (!generated_chunk)
            generated_chunk = std::move(chunk);
        return;
    }

    /// If there were only const columns in sort description, then there is no need to sort.
    /// Return the chunk as is.
    if (description.empty())
    {
        generated_chunk = std::move(chunk);
        return;
    }

    const UInt64 num_rows = chunk.getNumRows();
    const UInt64 num_bytes = chunk.bytes();

    /// Pairs with the snapshot walk in tryEmitQueryResultPreview.
    std::unique_lock<std::mutex> preview_lock;
    if (query_result_previews)
        preview_lock = std::unique_lock(query_result_previews->control.participantMutex(preview_participant_index));

    removeConstColumns(chunk);
    compactReplicatedColumns(chunk);

    sum_rows_in_blocks += chunk.getNumRows();
    sum_bytes_in_blocks += chunk.allocatedBytes();
    chunks.push_back(std::move(chunk));

    /** If significant amount of data was accumulated, perform preliminary merging step.
      */
    if ((chunks.size() > 1
        && limit
        && limit * 2 < sum_rows_in_blocks   /// 2 is just a guess.
        && remerge_is_useful
        && max_bytes_before_remerge
        && sum_bytes_in_blocks > max_bytes_before_remerge) || (threshold_tracker && (static_cast<double>(sum_rows_in_blocks) > static_cast<double>(limit) * 1.5)))
    {
        remerge();
    }

    /** If too many of them and if external sorting is enabled,
      *  will merge blocks that we have in memory at this moment and write merged stream to temporary (compressed) file.
      * NOTE. It's possible to check free space in filesystem.
      */
    if (max_bytes_in_block_before_external_sort && sum_bytes_in_blocks > max_bytes_in_block_before_external_sort)
    {
        Int64 query_memory = getCurrentQueryMemoryUsage();
        if (!max_bytes_in_query_before_external_sort || query_memory > static_cast<Int64>(max_bytes_in_query_before_external_sort))
        {
            auto sink = prepareSpill();
            auto source = std::make_shared<BufferingFromFileSource>(sink->getPort().getSharedHeader(), sink->getHolder(), log);
            processors.emplace_back(source);
            processors.emplace_back(sink);
            stage = Stage::Serialize;
        }
    }

    if (preview_lock.owns_lock())
    {
        /// Released before emitting: the emitter takes the participant locks itself, one by one.
        preview_lock.unlock();
        if (stage == Stage::Consume)
            tryEmitQueryResultPreview(num_rows, num_bytes);
    }
}

void MergeSortingTransform::tryEmitQueryResultPreview(UInt64 num_rows, UInt64 num_bytes)
{
    auto & control = query_result_previews->control;
    if (!control.accountAndCheckThresholds(num_rows, num_bytes))
        return;

    /// The preview would go into the single output slot; do not clobber a pending chunk.
    if (generated_chunk)
        return;

    /// Another transform is emitting a preview right now; do not wait for it.
    std::unique_lock emit_lock(control.emit_mutex, std::try_to_lock);
    if (!emit_lock.owns_lock())
        return;

    if (control.isDisabled())
        return;

    Chunks merge_input;
    UInt64 total_bytes = 0;
    for (auto * participant : query_result_previews->participants)
    {
        std::lock_guard participant_lock(control.participantMutex(participant->preview_participant_index));

        /// Once a participant spilled to disk or handed its chunks to the final merge, a consistent
        /// snapshot of the sorting state is not possible anymore.
        if (participant->temporary_files_num > 0 || participant->preview_state_moved)
        {
            control.disable();
            return;
        }

        total_bytes += participant->sum_bytes_in_blocks;
        if (control.settings.max_result_bytes && total_bytes > control.settings.max_result_bytes)
        {
            control.disable();
            return;
        }

        for (const auto & participant_chunk : participant->chunks)
            merge_input.push_back(participant_chunk.clone());
    }

    control.startNextRound();

    if (merge_input.empty())
        return;

    /// Merge the accumulated top-N candidates of all the streams and take the first `limit` rows
    /// as one chunk (previews are gated on a small `limit`, see `SortingStep::mergeSorting`).
    MergeSorter preview_sorter(
        std::make_shared<const Block>(header_without_constants), std::move(merge_input), description, limit, limit);
    Chunk preview = preview_sorter.read();
    if (!preview || !preview.hasRows())
        return;

    /// `MergeSorter::read` returns a single chunk verbatim, without cutting it to the limit.
    if (limit && preview.getNumRows() > limit)
    {
        auto columns = preview.detachColumns();
        for (auto & column : columns)
            column = column->cut(0, limit);
        preview.setColumns(std::move(columns), limit);
    }

    enrichChunkWithConstants(preview);
    markAsQueryResultPreview(preview, std::shared_ptr<QueryResultPreviewsControl>(query_result_previews, &query_result_previews->control));
    generated_chunk = std::move(preview);
}

ProcessorMemoryStats MergeSortingTransform::getMemoryStats() const
{
    /// The resident chunks remain spillable after EOF, until ownership passes to a merger.
    if (chunks.empty() || description.empty() || !tmp_data)
        return {};

    ProcessorMemoryStats res;
    res.spillable_memory_bytes = sum_bytes_in_blocks;
    /// One merged block is alive while it is being written
    if (sum_rows_in_blocks)
        res.need_reserved_memory_bytes = sum_bytes_in_blocks / sum_rows_in_blocks * max_merged_block_size;
    return res;
}

size_t MergeSortingTransform::spill(size_t /*at_least_bytes*/)
{
    size_t bytes = getMemoryStats().spillable_memory_bytes;
    if (!bytes)
        return 0;

    std::shared_ptr<BufferingToFileSink> sink;
    {
        /// A spill requested by the scheduler runs outside of `consume`, while another transform of
        /// this sorting may be snapshotting the accumulated chunks for a query result preview (see
        /// tryEmitQueryResultPreview). Moving the chunks out and counting the temporary file happen
        /// under the participant lock, so the snapshot either sees the chunks or the spill.
        std::unique_lock<std::mutex> preview_lock;
        if (query_result_previews)
        {
            preview_lock = std::unique_lock(query_result_previews->control.participantMutex(preview_participant_index));
            query_result_previews->control.disable();
        }

        sink = prepareSpill();
    }

    while (auto chunk = merge_sorter->read())
    {
        if (isCancelled())
            break;
        /// Duplicate-only reads must not become empty blocks in the spill file.
        if (chunk.hasRows())
            sink->consume(std::move(chunk));
    }
    merge_sorter.reset();
    sink->onFinish();
    spilled_sources.emplace_back(std::make_shared<BufferingFromFileSource>(
        sink->getPort().getSharedHeader(), std::move(sink->getHolder()), log));
    return bytes;
}

std::shared_ptr<BufferingToFileSink> MergeSortingTransform::prepareSpill()
{
    if (!tmp_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryDataOnDisk is not set for MergeSortingTransform");
    temporary_files_num++;

    LOG_TRACE(log, "Will dump sorting block ({}, limit: {}) to disk (query memory: {}, limit: {})",
        formatReadableSizeWithBinarySuffix(sum_bytes_in_blocks),
        formatReadableSizeWithBinarySuffix(max_bytes_in_block_before_external_sort),
        formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
        formatReadableSizeWithBinarySuffix(max_bytes_in_query_before_external_sort));

    /// If there's less free disk space than reserve_size, an exception will be thrown
    size_t reserve_size = sum_bytes_in_blocks + min_free_disk_space;
    SharedHeader shared_header_without_constants = std::make_shared<const Block>(header_without_constants);
    TemporaryBlockStreamHolder tmp_stream(shared_header_without_constants, tmp_data, reserve_size);
    merge_sorter = std::make_unique<MergeSorter>(
        shared_header_without_constants, std::move(chunks), description, max_merged_block_size, limit,
        merge_mode, max_block_bytes);
    auto sink = std::make_shared<BufferingToFileSink>(shared_header_without_constants, std::move(tmp_stream), log);

    if (!external_merging_sorted)
    {
        bool have_all_inputs = false;

        if (merge_mode == MergeSorter::Mode::MergeUniqueChunks)
        {
            /// The merges in memory make each input unique, as `DistinctSortedTransform` requires.
            external_merging_sorted = std::make_shared<DistinctSortedTransform>(
                SharedHeaders{}, shared_header_without_constants, description,
                /*already_emitted_flag_column=*/ std::nullopt, merge_sorter->getMaxMergedBlockSize(), have_all_inputs);
        }
        else
        {
            bool use_average_block_sizes = false;
            bool apply_virtual_row = false;

            auto merging_sorted = std::make_shared<MergingSortedTransform>(
                shared_header_without_constants,
                0,
                description,
                merge_sorter->getMaxMergedBlockSize(),
                /*max_merged_block_size_bytes=*/0,
                /*max_dynamic_subcolumns=*/std::nullopt,
                SortingQueueStrategy::Batch,
                limit,
                /*always_read_till_end_=*/ false,
                /*out_row_sources_buf=*/ nullptr,
                /*filter_column_name=*/ std::nullopt,
                use_average_block_sizes,
                apply_virtual_row,
                /*virtual_row_prefetch_window=*/ 0,
                have_all_inputs);

            /// With external sorting this merge produces the sorted result of the stream, so it
            /// publishes the value at the limit instead of `generate` (see there).
            if (threshold_tracker && limit)
                merging_sorted->setTopKThresholdTracker(threshold_tracker, description.front().column_name, limit);

            external_merging_sorted = std::move(merging_sorted);
        }
    }

    sum_bytes_in_blocks = 0;
    sum_rows_in_blocks = 0;
    return sink;
}

void MergeSortingTransform::serialize()
{
    current_chunk = merge_sorter->read();
    if (!current_chunk)
        merge_sorter.reset();
    /// A read of `MergeSorter::Mode::MergeUniqueChunks` that consumes only duplicates returns no rows. The
    /// run gets no block for it, and the next call reads on.
    else if (!current_chunk.hasRows())
        current_chunk.clear();
}

void MergeSortingTransform::generate()
{
    if (!generated_prefix)
    {
        /// Pairs with the snapshot walk in tryEmitQueryResultPreview: another transform of this
        /// sorting may still be consuming and snapshotting the accumulated chunks.
        std::unique_lock<std::mutex> preview_lock;
        if (query_result_previews)
        {
            preview_lock = std::unique_lock(query_result_previews->control.participantMutex(preview_participant_index));
            preview_state_moved = true;
        }

        if (temporary_files_num == 0)
        {
            merge_sorter = std::make_unique<MergeSorter>(
                std::make_shared<const Block>(header_without_constants),
                std::move(chunks),
                description,
                max_merged_block_size,
                limit,
                merge_mode);
        }
        else
        {
            ProfileEvents::increment(external_merge_event);
            LOG_INFO(log, "There are {} temporary sorted parts to merge", temporary_files_num);

            processors.emplace_back(
                std::make_shared<MergeSorterSource>(
                    std::make_shared<const Block>(header_without_constants),
                    std::move(chunks),
                    description,
                    max_merged_block_size,
                    limit,
                    merge_mode));
        }

        generated_prefix = true;
    }

    if (merge_sorter)
    {
        generated_chunk = merge_sorter->read();
        if (!generated_chunk)
        {
            merge_sorter.reset();
        }
        /// A read that consumes only duplicates returns no rows, which are not output.
        else if (!generated_chunk.hasRows())
        {
            generated_chunk.clear();
        }
        else
        {
            /// The sorted result is complete, so the row at `limit` is the K-th value of this stream:
            /// the exact final threshold with a single stream, and never tighter than it with several.
            /// Publishing it lets a reader judge its data against it once the sorting is done, which
            /// `remerge` alone does not guarantee.
            if (threshold_tracker && limit && !generated_threshold_published)
            {
                const size_t rows = generated_chunk.getNumRows();
                if (rows_generated + rows >= limit)
                {
                    if (auto sort_column_position = header_without_constants.findPositionByName(description.front().column_name))
                    {
                        Field value;
                        generated_chunk.getColumns()[*sort_column_position]->get(limit - rows_generated - 1, value);
                        threshold_tracker->testAndSet(value);
                    }
                    generated_threshold_published = true;
                }
                rows_generated += rows;
            }
            enrichChunkWithConstants(generated_chunk);
        }
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
