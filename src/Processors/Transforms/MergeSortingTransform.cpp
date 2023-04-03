#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Common/ProfileEvents.h>
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
}


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_ENOUGH_SPACE;
}


class BufferingToFileTransform : public IAccumulatingTransform
{
public:
    BufferingToFileTransform(const Block & header, Poco::Logger * log_, std::string path_)
        : IAccumulatingTransform(header, header), log(log_)
        , path(std::move(path_)), file_buf_out(path), compressed_buf_out(file_buf_out)
        , out_stream(std::make_unique<NativeWriter>(compressed_buf_out, 0, header))
    {
        LOG_INFO(log, "Sorting and writing part of data into temporary file {}", path);
        ProfileEvents::increment(ProfileEvents::ExternalSortWritePart);
    }

    String getName() const override { return "BufferingToFileTransform"; }

    void consume(Chunk chunk) override
    {
        out_stream->write(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    Chunk generate() override
    {
        if (out_stream)
        {
            compressed_buf_out.next();
            file_buf_out.next();
            LOG_INFO(log, "Done writing part of data into temporary file {}", path);

            out_stream.reset();

            file_in = std::make_unique<ReadBufferFromFile>(path);
            compressed_in = std::make_unique<CompressedReadBuffer>(*file_in);
            block_in = std::make_unique<NativeReader>(*compressed_in, getOutputPort().getHeader(), 0);
        }

        if (!block_in)
            return {};

        auto block = block_in->read();
        if (!block)
        {
            block_in.reset();
            return {};
        }

        UInt64 num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

private:
    Poco::Logger * log;
    std::string path;
    WriteBufferFromFile file_buf_out;
    CompressedWriteBuffer compressed_buf_out;
    std::unique_ptr<NativeWriter> out_stream;

    std::unique_ptr<ReadBufferFromFile> file_in;
    std::unique_ptr<CompressedReadBuffer> compressed_in;
    std::unique_ptr<NativeReader> block_in;
};

MergeSortingTransform::MergeSortingTransform(
    const Block & header,
    const SortDescription & description_,
    size_t max_merged_block_size_,
    UInt64 limit_,
    bool increase_sort_description_compile_attempts,
    size_t max_bytes_before_remerge_,
    double remerge_lowered_memory_bytes_ratio_,
    size_t max_bytes_before_external_sort_,
    VolumePtr tmp_volume_,
    size_t min_free_disk_space_)
    : SortingTransform(header, description_, max_merged_block_size_, limit_, increase_sort_description_compile_attempts)
    , max_bytes_before_remerge(max_bytes_before_remerge_)
    , remerge_lowered_memory_bytes_ratio(remerge_lowered_memory_bytes_ratio_)
    , max_bytes_before_external_sort(max_bytes_before_external_sort_)
    , tmp_volume(tmp_volume_)
    , min_free_disk_space(min_free_disk_space_)
{
}

Processors MergeSortingTransform::expandPipeline()
{
    if (processors.size() > 1)
    {
        /// Add external_merging_sorted.
        inputs.emplace_back(header_without_constants, this);
        connect(external_merging_sorted->getOutputs().front(), inputs.back());
    }

    auto & buffer = processors.front();

    static_cast<MergingSortedTransform &>(*external_merging_sorted).addInput();
    connect(buffer->getOutputs().back(), external_merging_sorted->getInputs().back());

    if (!buffer->getInputs().empty())
    {
        /// Serialize
        outputs.emplace_back(header_without_constants, this);
        connect(getOutputs().back(), buffer->getInputs().back());
        /// Hack. Say buffer that we need data from port (otherwise it will return PortFull).
        external_merging_sorted->getInputs().back().setNeeded();
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
    if (max_bytes_before_external_sort && sum_bytes_in_blocks > max_bytes_before_external_sort)
    {
        size_t size = sum_bytes_in_blocks + min_free_disk_space;
        auto reservation = tmp_volume->reserve(size);
        if (!reservation)
            throw Exception("Not enough space for external sort in temporary storage", ErrorCodes::NOT_ENOUGH_SPACE);

        const std::string tmp_path(reservation->getDisk()->getPath());
        temporary_files.emplace_back(createTemporaryFile(tmp_path));

        const std::string & path = temporary_files.back()->path();
        merge_sorter
            = std::make_unique<MergeSorter>(header_without_constants, std::move(chunks), description, max_merged_block_size, limit);
        auto current_processor = std::make_shared<BufferingToFileTransform>(header_without_constants, log, path);

        processors.emplace_back(current_processor);

        if (!external_merging_sorted)
        {
            bool quiet = false;
            bool have_all_inputs = false;
            bool use_average_block_sizes = false;

            external_merging_sorted = std::make_shared<MergingSortedTransform>(
                    header_without_constants,
                    0,
                    description,
                    max_merged_block_size,
                    SortingQueueStrategy::Batch,
                    limit,
                    nullptr,
                    quiet,
                    use_average_block_sizes,
                    have_all_inputs);

            processors.emplace_back(external_merging_sorted);
        }

        stage = Stage::Serialize;
        sum_bytes_in_blocks = 0;
        sum_rows_in_blocks = 0;
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
        if (temporary_files.empty())
            merge_sorter
                = std::make_unique<MergeSorter>(header_without_constants, std::move(chunks), description, max_merged_block_size, limit);
        else
        {
            ProfileEvents::increment(ProfileEvents::ExternalSortMerge);
            LOG_INFO(log, "There are {} temporary sorted parts to merge.", temporary_files.size());

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
    if (remerge_lowered_memory_bytes_ratio && (new_sum_bytes_in_blocks * remerge_lowered_memory_bytes_ratio > sum_bytes_in_blocks))
    {
        remerge_is_useful = false;
        LOG_DEBUG(log, "Re-merging is not useful (memory usage was not lowered by remerge_sort_lowered_memory_bytes_ratio={})", remerge_lowered_memory_bytes_ratio);
    }

    chunks = std::move(new_chunks);
    sum_rows_in_blocks = new_sum_rows_in_blocks;
    sum_bytes_in_blocks = new_sum_bytes_in_blocks;
}

}
