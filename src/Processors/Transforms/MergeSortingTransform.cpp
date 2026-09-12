#include <Columns/ColumnReplicated.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/IAccumulatingTransform.h>
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

/// Reads back a sorted part written by `dumpToTemporaryFile`.
class BufferingFromFileSource : public ISource
{
public:
    BufferingFromFileSource(SharedHeader header, const TemporaryBlockStreamHolder & tmp_stream_, LoggerPtr log_)
        : ISource(std::move(header))
        , tmp_stream(tmp_stream_)
        , log(log_)
    {
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
    const TemporaryBlockStreamHolder & tmp_stream;
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
    TopKThresholdTrackerPtr threshold_tracker_)
    : SortingTransform(header, description_, max_merged_block_size_, limit_, increase_sort_description_compile_attempts)
    , max_bytes_before_remerge(max_bytes_before_remerge_)
    , remerge_lowered_memory_bytes_ratio(remerge_lowered_memory_bytes_ratio_)
    , max_bytes_in_block_before_external_sort(max_bytes_in_block_before_external_sort_)
    , max_bytes_in_query_before_external_sort(max_bytes_in_query_before_external_sort_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
    , max_block_bytes(max_block_bytes_)
    , threshold_tracker(threshold_tracker_)
{
}

IProcessor::PipelineUpdate MergeSortingTransform::updatePipeline()
{
    /// The sources of the sorted parts (on disk and in memory) feed the merge, the merge feeds this transform.
    auto merging_input = external_merging_sorted->getInputs().begin();
    for (const auto & source : processors)
        connect(source->getOutputs().front(), *merging_input++);

    inputs.emplace_back(header_without_constants, this);
    connect(external_merging_sorted->getOutputs().front(), inputs.back());

    processors.emplace_back(std::move(external_merging_sorted));
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
            dumpToTemporaryFile();
    }
}

ProcessorMemoryStats MergeSortingTransform::getMemoryStats() const
{
    /// Only the chunks accumulated while consuming can be written out.
    if (stage != Stage::Consume || chunks.empty() || description.empty() || !tmp_data)
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

    dumpToTemporaryFile();
    return bytes;
}

void MergeSortingTransform::dumpToTemporaryFile()
{
    if (!tmp_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryDataOnDisk is not set for MergeSortingTransform");

    /// If there's less free disk space than reserve_size, an exception will be thrown
    size_t reserve_size = sum_bytes_in_blocks + min_free_disk_space;
    SharedHeader header = std::make_shared<const Block>(header_without_constants);
    auto & tmp_stream = temporary_streams.emplace_back(header, tmp_data, reserve_size);

    LOG_INFO(log, "Sorting and writing {} rows ({}) into temporary file {}",
        sum_rows_in_blocks, ReadableSize(sum_bytes_in_blocks), tmp_stream.getHolder()->describeFilePath());

    size_t max_merged_block_size = this->max_merged_block_size;
    if (max_block_bytes > 0 && sum_rows_in_blocks > 0 && sum_bytes_in_blocks > 0)
    {
        auto avg_row_bytes = sum_bytes_in_blocks / sum_rows_in_blocks;
        /// max_merged_block_size >= 128
        max_merged_block_size = std::max(std::min(max_merged_block_size, max_block_bytes / avg_row_bytes), 128UL);
    }

    MergeSorter sorter(header, std::move(chunks), description, max_merged_block_size, limit);
    while (auto chunk = sorter.read())
    {
        if (isCancelled())
            break;
        tmp_stream->write(header->cloneWithColumns(chunk.detachColumns()));
    }
    auto stat = tmp_stream.finishWriting();

    LOG_INFO(log, "Done writing part of data into temporary file {}, compressed {}, uncompressed {}",
        tmp_stream.getHolder()->describeFilePath(),
        ReadableSize(static_cast<double>(stat.compressed_size)), ReadableSize(static_cast<double>(stat.uncompressed_size)));

    chunks.clear();
    sum_bytes_in_blocks = 0;
    sum_rows_in_blocks = 0;
}

void MergeSortingTransform::generate()
{
    if (!generated_prefix)
    {
        SharedHeader header = std::make_shared<const Block>(header_without_constants);
        if (temporary_streams.empty())
        {
            merge_sorter = std::make_unique<MergeSorter>(header, std::move(chunks), description, max_merged_block_size, limit);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::ExternalSortMerge);
            LOG_INFO(log, "There are {} temporary sorted parts to merge", temporary_streams.size());

            for (const auto & tmp_stream : temporary_streams)
                processors.emplace_back(std::make_shared<BufferingFromFileSource>(header, tmp_stream, log));
            if (!chunks.empty())
                processors.emplace_back(std::make_shared<MergeSorterSource>(header, std::move(chunks), description, max_merged_block_size, limit));

            external_merging_sorted = std::make_shared<MergingSortedTransform>(
                header,
                processors.size(),
                description,
                max_merged_block_size,
                /*max_merged_block_size_bytes=*/0,
                /*max_dynamic_subcolumns=*/std::nullopt,
                SortingQueueStrategy::Batch,
                limit,
                /*always_read_till_end_=*/ false,
                /*out_row_sources_buf=*/ nullptr,
                /*filter_column_name=*/ std::nullopt,
                /*use_average_block_sizes=*/ false,
                /*apply_virtual_row=*/ false,
                /*virtual_row_prefetch_window=*/ 0,
                /*have_all_inputs=*/ true);
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
