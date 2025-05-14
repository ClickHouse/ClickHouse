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
    extern const Event ExternalSortWritePart;
    extern const Event ExternalSortMerge;
    extern const Event ExternalSortCompressedBytes;
    extern const Event ExternalSortUncompressedBytes;
    extern const Event ExternalProcessingCompressedBytesTotal;
    extern const Event ExternalProcessingUncompressedBytesTotal;
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
    BufferingToFileSink(Block header, TemporaryBlockStreamHolder tmp_stream_, LoggerPtr log_)
        : ISink(std::move(header))
        , tmp_stream(std::move(tmp_stream_))
        , log(log_)
    {
        outputs.emplace_back(Block(), this);
        LOG_INFO(log, "Sorting and writing part of data into temporary file {}", tmp_stream.getHolder()->describeFilePath());
        ProfileEvents::increment(ProfileEvents::ExternalSortWritePart);
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

        ProfileEvents::increment(ProfileEvents::ExternalProcessingCompressedBytesTotal, stat.compressed_size);
        ProfileEvents::increment(ProfileEvents::ExternalProcessingUncompressedBytesTotal, stat.uncompressed_size);
        ProfileEvents::increment(ProfileEvents::ExternalSortCompressedBytes, stat.compressed_size);
        ProfileEvents::increment(ProfileEvents::ExternalSortUncompressedBytes, stat.uncompressed_size);

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
    BufferingFromFileSource(Block header, TemporaryBlockStreamHolder & tmp_stream_, LoggerPtr log_)
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

    Chunk generate() override
    {
        if (!tmp_read_stream)
        {
            LOG_INFO(log, "Start reading part of data from temporary file");
            tmp_read_stream = tmp_stream.getReadStream();
        }

        Block block = tmp_read_stream.value()->read();
        if (!block)
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
    const Block & header,
    const SortDescription & description_,
    size_t max_merged_block_size_,
    size_t max_block_bytes_,
    UInt64 limit_,
    bool increase_sort_description_compile_attempts,
    size_t max_bytes_before_remerge_,
    double remerge_lowered_memory_bytes_ratio_,
    size_t min_external_sort_block_bytes_,
    size_t max_bytes_before_external_sort_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t min_free_disk_space_)
    : SortingTransform(header, description_, max_merged_block_size_, limit_, increase_sort_description_compile_attempts)
    , max_bytes_before_remerge(max_bytes_before_remerge_)
    , remerge_lowered_memory_bytes_ratio(remerge_lowered_memory_bytes_ratio_)
    , min_external_sort_block_bytes(min_external_sort_block_bytes_)
    , max_bytes_before_external_sort(max_bytes_before_external_sort_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
    , max_block_bytes(max_block_bytes_)
{
}

Processors MergeSortingTransform::expandPipeline()
{
    if (processors.size() > 2)
    {
        /// Add external_merging_sorted.
        inputs.emplace_back(header_without_constants, this);
        connect(external_merging_sorted->getOutputs().front(), inputs.back());
    }

    auto & source = processors.at(0);

    static_cast<MergingSortedTransform &>(*external_merging_sorted).addInput();
    connect(source->getOutputs().back(), external_merging_sorted->getInputs().back());

    if (processors.size() > 1)
    {
        auto & sink = processors.at(1);
        /// Serialize
        outputs.emplace_back(header_without_constants, this);
        connect(sink->getOutputs().front(), source->getInputs().front());
        connect(getOutputs().back(), sink->getInputs().back());
    }
    else
        /// Generate
        static_cast<MergingSortedTransform &>(*external_merging_sorted).setHaveAllInputs();

    return std::move(processors);
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

    sum_rows_in_blocks += chunk.getNumRows();
    sum_bytes_in_blocks += chunk.allocatedBytes();
    chunks.push_back(std::move(chunk));

    /** If significant amount of data was accumulated, perform preliminary merging step.
      */
    if (chunks.size() > 1
        && limit
        && limit * 2 < sum_rows_in_blocks   /// 2 is just a guess.
        && remerge_is_useful
        && max_bytes_before_remerge
        && sum_bytes_in_blocks > max_bytes_before_remerge)
    {
        remerge();
    }

    /** If too many of them and if external sorting is enabled,
      *  will merge blocks that we have in memory at this moment and write merged stream to temporary (compressed) file.
      * NOTE. It's possible to check free space in filesystem.
      */
    if (sum_bytes_in_blocks > min_external_sort_block_bytes && max_bytes_before_external_sort)
    {
        Int64 query_memory = getCurrentQueryMemoryUsage();
        if (query_memory > static_cast<Int64>(max_bytes_before_external_sort))
        {
            if (!tmp_data)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryDataOnDisk is not set for MergeSortingTransform");
            temporary_files_num++;

            LOG_TRACE(log, "Will dump sorting block to disk ({} > {})", formatReadableSizeWithBinarySuffix(query_memory), formatReadableSizeWithBinarySuffix(max_bytes_before_external_sort));

            /// If there's less free disk space than reserve_size, an exception will be thrown
            size_t reserve_size = sum_bytes_in_blocks + min_free_disk_space;
            TemporaryBlockStreamHolder tmp_stream(header_without_constants, tmp_data.get(), reserve_size);
            size_t max_merged_block_size = this->max_merged_block_size;
            if (max_block_bytes > 0 && sum_rows_in_blocks > 0 && sum_bytes_in_blocks > 0)
            {
                auto avg_row_bytes = sum_bytes_in_blocks / sum_rows_in_blocks;
                /// max_merged_block_size >= 128
                max_merged_block_size = std::max(std::min(max_merged_block_size, max_block_bytes / avg_row_bytes), 128UL);
            }
            merge_sorter = std::make_unique<MergeSorter>(header_without_constants, std::move(chunks), description, max_merged_block_size, limit);
            auto sink = std::make_shared<BufferingToFileSink>(header_without_constants, std::move(tmp_stream), log);
            auto source = std::make_shared<BufferingFromFileSource>(header_without_constants, sink->getHolder(), log);

            processors.emplace_back(source);
            processors.emplace_back(sink);

            if (!external_merging_sorted)
            {
                bool have_all_inputs = false;
                bool use_average_block_sizes = false;
                bool apply_virtual_row = false;

                external_merging_sorted = std::make_shared<MergingSortedTransform>(
                        header_without_constants,
                        0,
                        description,
                        max_merged_block_size,
                        /*max_merged_block_size_bytes*/0,
                        SortingQueueStrategy::Batch,
                        limit,
                        /*always_read_till_end_=*/ false,
                        nullptr,
                        use_average_block_sizes,
                        apply_virtual_row,
                        have_all_inputs);

                processors.emplace_back(external_merging_sorted);
            }

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
        if (temporary_files_num == 0)
        {
            merge_sorter = std::make_unique<MergeSorter>(header_without_constants, std::move(chunks), description, max_merged_block_size, limit);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::ExternalSortMerge);
            LOG_INFO(log, "There are {} temporary sorted parts to merge", temporary_files_num);

            processors.emplace_back(std::make_shared<MergeSorterSource>(
                    header_without_constants, std::move(chunks), description, max_merged_block_size, limit));
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
    MergeSorter remerge_sorter(header_without_constants, std::move(chunks), description, max_merged_block_size, limit);

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
    if (remerge_lowered_memory_bytes_ratio > 0.0 && (new_sum_bytes_in_blocks * remerge_lowered_memory_bytes_ratio > sum_bytes_in_blocks))
    {
        remerge_is_useful = false;
        LOG_DEBUG(log, "Re-merging is not useful (memory usage was not lowered by remerge_sort_lowered_memory_bytes_ratio={})", remerge_lowered_memory_bytes_ratio);
    }

    chunks = std::move(new_chunks);
    sum_rows_in_blocks = new_sum_rows_in_blocks;
    sum_bytes_in_blocks = new_sum_bytes_in_blocks;
}

}
