#include <Columns/ColumnReplicated.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/ISink.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <Disks/IVolume.h>


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

class BufferingToFileSink : public ISink
{
public:
    BufferingToFileSink(SharedHeader header, TemporaryBlockStreamHolder tmp_stream_, LoggerPtr log_)
        : ISink(std::move(header))
        , tmp_stream(std::move(tmp_stream_))
        , log(log_)
    {
        outputs.emplace_back(Block(), this);
        LOG_INFO(log, "Sorting and writing part of data into temporary file {}", tmp_stream.getHolder()->describeFilePath());
    }

    Status prepare() override
    {
        auto status = ISink::prepare();
        if (status == Status::Finished)
            outputs.front().finish();
        return status;
    }

    String getName() const override { return "BufferingToFileSink"; }

    void consume(Chunk chunk) override
    {
        Block block = getPort().getHeader().cloneWithColumns(chunk.detachColumns());
        tmp_stream->write(block);
    }

    void onFinish() override
    {
        auto stat = tmp_stream.finishWriting();
        LOG_INFO(log, "Done writing part of data into temporary file {}, compressed {}, uncompressed {} ",
            tmp_stream.getHolder()->describeFilePath(),
            ReadableSize(static_cast<double>(stat.compressed_size)), ReadableSize(static_cast<double>(stat.uncompressed_size)));
    }

    TemporaryBlockStreamHolder & getHolder() { return tmp_stream; }

private:
    TemporaryBlockStreamHolder tmp_stream;
    LoggerPtr log;
};

class BufferingFromFileSource : public ISource
{
public:
    BufferingFromFileSource(SharedHeader header, TemporaryBlockStreamHolder & tmp_stream_, LoggerPtr log_)
        : ISource(std::move(header))
        , tmp_stream(tmp_stream_)
        , log(log_)
    {
        inputs.emplace_back(Block(), this);
    }

    Status prepare() override
    {
        if (!inputs.front().isFinished())
        {
            if (inputs.front().hasData())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot read the data from BufferingToFileSource input");

            inputs.front().setNeeded();
            return Status::NeedData;
        }

        return ISource::prepare();
    }

    String getName() const override { return "BufferingFromFileSource"; }

    /// These rows were already counted when they were read from the original source.
    std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

    Chunk generate() override
    {
        if (!tmp_read_stream)
        {
            LOG_INFO(log, "Start reading part of data from temporary file");
            tmp_read_stream = tmp_stream.getReadStream();
        }

        Block block = tmp_read_stream.value()->read();
        if (block.empty())
            return {};

        UInt64 num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

private:
    TemporaryBlockStreamHolder & tmp_stream;
    std::optional<TemporaryBlockStreamReaderHolder> tmp_read_stream;
    LoggerPtr log;
};

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
{
    if (query_result_previews)
    {
        preview_participant_index = query_result_previews->participants.size();
        query_result_previews->participants.push_back(this);
    }
}

void MergeSortingTransform::activateQueryResultPreviews()
{
    if (query_result_previews)
        query_result_previews->control.activate();
}

IProcessor::PipelineUpdate MergeSortingTransform::updatePipeline()
{
    if (processors.size() > 2)
    {
        /// Add external_merging_sorted.
        inputs.emplace_back(header_without_constants, this);
        connect(external_merging_sorted->getOutputs().front(), inputs.back());
    }

    auto & source = processors.front();

    static_cast<MergingSortedTransform &>(*external_merging_sorted).addInput();
    connect(source->getOutputs().back(), external_merging_sorted->getInputs().back());

    if (processors.size() > 1)
    {
        auto & sink = *std::next(processors.begin());
        /// Serialize
        outputs.emplace_back(header_without_constants, this);
        connect(sink->getOutputs().front(), source->getInputs().front());
        connect(getOutputs().back(), sink->getInputs().back());
    }
    else
        /// Generate
        static_cast<MergingSortedTransform &>(*external_merging_sorted).setHaveAllInputs();

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
            if (!tmp_data)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryDataOnDisk is not set for MergeSortingTransform");
            temporary_files_num++;

            LOG_TRACE(log, "Will dump sorting block ({}, limit: {}) to disk (query memory: {}, limit: {})",
                formatReadableSizeWithBinarySuffix(sum_bytes_in_blocks),
                formatReadableSizeWithBinarySuffix(max_bytes_in_block_before_external_sort),
                formatReadableSizeWithBinarySuffix(query_memory),
                formatReadableSizeWithBinarySuffix(max_bytes_in_query_before_external_sort));

            /// If there's less free disk space than reserve_size, an exception will be thrown
            size_t reserve_size = sum_bytes_in_blocks + min_free_disk_space;
            SharedHeader shared_header_without_constants = std::make_shared<const Block>(header_without_constants);
            TemporaryBlockStreamHolder tmp_stream(shared_header_without_constants, tmp_data, reserve_size);
            size_t max_merged_block_size = this->max_merged_block_size;
            if (max_block_bytes > 0 && sum_rows_in_blocks > 0 && sum_bytes_in_blocks > 0)
            {
                auto avg_row_bytes = sum_bytes_in_blocks / sum_rows_in_blocks;
                /// max_merged_block_size >= 128
                max_merged_block_size = std::max(std::min(max_merged_block_size, max_block_bytes / avg_row_bytes), 128UL);
            }
            merge_sorter = std::make_unique<MergeSorter>(shared_header_without_constants, std::move(chunks), description, max_merged_block_size, limit);
            auto sink = std::make_shared<BufferingToFileSink>(shared_header_without_constants, std::move(tmp_stream), log);
            auto source = std::make_shared<BufferingFromFileSource>(shared_header_without_constants, sink->getHolder(), log);

            processors.emplace_back(source);
            processors.emplace_back(sink);

            if (!external_merging_sorted)
            {
                bool have_all_inputs = false;
                bool use_average_block_sizes = false;
                bool apply_virtual_row = false;

                external_merging_sorted = std::make_shared<MergingSortedTransform>(
                        shared_header_without_constants,
                        0,
                        description,
                        max_merged_block_size,
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

                processors.emplace_back(external_merging_sorted);
            }

            stage = Stage::Serialize;
            sum_bytes_in_blocks = 0;
            sum_rows_in_blocks = 0;
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
    markAsQueryResultPreview(preview);
    generated_chunk = std::move(preview);
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
            merge_sorter = std::make_unique<MergeSorter>(std::make_shared<const Block>(header_without_constants), std::move(chunks), description, max_merged_block_size, limit);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::ExternalSortMerge);
            LOG_INFO(log, "There are {} temporary sorted parts to merge", temporary_files_num);

            processors.emplace_back(std::make_shared<MergeSorterSource>(
                    std::make_shared<const Block>(header_without_constants), std::move(chunks), description, max_merged_block_size, limit));
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
