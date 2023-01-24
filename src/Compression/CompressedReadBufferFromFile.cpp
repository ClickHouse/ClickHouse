#include <cassert>

#include "CompressedReadBufferFromFile.h"

#include <Compression/LZ4_decompress_faster.h>
#include <IO/WriteHelpers.h>
#include <Disks/IO/createReadBufferFromFileBase.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


bool CompressedReadBufferFromFile::nextImpl()
{
    size_t size_decompressed = 0;
    size_t size_compressed_without_checksum;
    size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum, false);
    if (!size_compressed)
        return false;

    auto additional_size_at_the_end_of_buffer = codec->getAdditionalSizeAtTheEndOfBuffer();

    /// This is for clang static analyzer.
    assert(size_decompressed + additional_size_at_the_end_of_buffer > 0);

    memory.resize(size_decompressed + additional_size_at_the_end_of_buffer);
    working_buffer = Buffer(memory.data(), &memory[size_decompressed]);

    decompress(working_buffer, size_decompressed, size_compressed_without_checksum);

    /// nextimpl_working_buffer_offset is set in the seek function (lazy seek). So we have to
    /// check that we are not seeking beyond working buffer.
    if (nextimpl_working_buffer_offset > working_buffer.size())
        throw Exception("Required to move position beyond the decompressed block"
        " (pos: " + toString(nextimpl_working_buffer_offset) + ", block size: " + toString(working_buffer.size()) + ")",
        ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    return true;
}


CompressedReadBufferFromFile::CompressedReadBufferFromFile(std::unique_ptr<ReadBufferFromFileBase> buf, bool allow_different_codecs_)
    : BufferWithOwnMemory<ReadBuffer>(0), p_file_in(std::move(buf)), file_in(*p_file_in)
{
    compressed_in = &file_in;
    allow_different_codecs = allow_different_codecs_;
}


void CompressedReadBufferFromFile::prefetch()
{
    file_in.prefetch();
}


void CompressedReadBufferFromFile::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    /// Nothing to do if we already at required position
    if (!size_compressed && static_cast<size_t>(file_in.getPosition()) == offset_in_compressed_file && /// correct position in compressed file
        ((!buffer().empty() && offset() == offset_in_decompressed_block)     /// correct position in buffer or
         || nextimpl_working_buffer_offset == offset_in_decompressed_block)) /// we will move our position to correct one
        return;

    /// Our seek is within working_buffer, so just move the position
    if (size_compressed &&
        offset_in_compressed_file == file_in.getPosition() - size_compressed &&
        offset_in_decompressed_block <= working_buffer.size())
    {
        pos = working_buffer.begin() + offset_in_decompressed_block;
    }
    else /// Our seek outside working buffer, so perform "lazy seek"
    {
        /// Actually seek compressed file
        file_in.seek(offset_in_compressed_file, SEEK_SET);
        /// We will discard our working_buffer, but have to account rest bytes
        bytes += offset();
        /// No data, everything discarded
        resetWorkingBuffer();
        size_compressed = 0;
        /// Remember required offset in decompressed block which will be set in
        /// the next ReadBuffer::next() call
        nextimpl_working_buffer_offset = offset_in_decompressed_block;
    }
}

size_t CompressedReadBufferFromFile::readBig(char * to, size_t n)
{
    size_t bytes_read = 0;
    /// The codec mode is only relevant for codecs which support hardware offloading.
    ICompressionCodec::CodecMode decompress_mode = ICompressionCodec::CodecMode::Synchronous;
    bool read_tail = false;

    /// If there are unread bytes in the buffer, then we copy needed to `to`.
    if (pos < working_buffer.end())
        bytes_read += read(to, std::min(static_cast<size_t>(working_buffer.end() - pos), n));

    /// If you need to read more - we will, if possible, decompress at once to `to`.
    while (bytes_read < n)
    {
        size_t size_decompressed = 0;
        size_t size_compressed_without_checksum = 0;

        ///Try to read block which is entirely located in a single 'compressed_in->' buffer.
        size_t new_size_compressed = readCompressedDataBlockForAsynchronous(size_decompressed, size_compressed_without_checksum);

        if (new_size_compressed)
        {
            /// Current block is entirely located in a single 'compressed_in->' buffer.
            /// We can set asynchronous decompression mode if supported to boost performance.
            decompress_mode = ICompressionCodec::CodecMode::Asynchronous;
        }
        else
        {
            /// Current block cannot be decompressed asynchronously, means it probably span across two compressed_in buffers.
            /// Meanwhile, asynchronous requests for previous blocks should be flushed if any.
            flushAsynchronousDecompressRequests();
            /// Fallback to generic API
            new_size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum, false);
            decompress_mode = ICompressionCodec::CodecMode::Synchronous;
        }
        size_compressed = 0; /// file_in no longer points to the end of the block in working_buffer.

        if (!new_size_compressed)
            break;

        auto additional_size_at_the_end_of_buffer = codec->getAdditionalSizeAtTheEndOfBuffer();

        /// If the decompressed block fits entirely where it needs to be copied and we don't
        /// need to skip some bytes in decompressed data (seek happened before readBig call).
        if (nextimpl_working_buffer_offset == 0 && size_decompressed + additional_size_at_the_end_of_buffer <= n - bytes_read)
        {
            setDecompressMode(decompress_mode);
            decompressTo(to + bytes_read, size_decompressed, size_compressed_without_checksum);
            bytes_read += size_decompressed;
            bytes += size_decompressed;
        }
        else if (nextimpl_working_buffer_offset > 0)
        {
            /// Need to skip some bytes in decompressed data (seek happened before readBig call).
            size_compressed = new_size_compressed;
            bytes += offset();

            /// This is for clang static analyzer.
            assert(size_decompressed + additional_size_at_the_end_of_buffer > 0);
            memory.resize(size_decompressed + additional_size_at_the_end_of_buffer);
            working_buffer = Buffer(memory.data(), &memory[size_decompressed]);
            /// Synchronous mode must be set since we need read partial data immediately from working buffer to target buffer.
            setDecompressMode(ICompressionCodec::CodecMode::Synchronous);
            decompress(working_buffer, size_decompressed, size_compressed_without_checksum);

            /// Read partial data from first block. Won't run here at second block.
            /// Avoid to call nextImpl and unnecessary memcpy in read when the second block fits entirely to output buffer.
            size_t size_partial = std::min((size_decompressed - nextimpl_working_buffer_offset),(n - bytes_read));
            pos = working_buffer.begin() + nextimpl_working_buffer_offset;
            nextimpl_working_buffer_offset = 0;
            bytes_read += read(to + bytes_read, size_partial);
        }
        else
        {
            size_compressed = new_size_compressed;
            bytes += offset();

            /// This is for clang static analyzer.
            assert(size_decompressed + additional_size_at_the_end_of_buffer > 0);
            memory.resize(size_decompressed + additional_size_at_the_end_of_buffer);
            working_buffer = Buffer(memory.data(), &memory[size_decompressed]);
            // Asynchronous mode can be set here because working_buffer wouldn't be overwritten any more since this is the last block.
            setDecompressMode(ICompressionCodec::CodecMode::Asynchronous);
            decompress(working_buffer, size_decompressed, size_compressed_without_checksum);
            read_tail = true;
            break;
        }
    }

    /// Here we must make sure all asynchronous requests above are completely done.
    flushAsynchronousDecompressRequests();

    if (read_tail)
    {
        /// Manually take nextimpl_working_buffer_offset into account, because we don't use
        /// nextImpl in this method.
        pos = working_buffer.begin();
        bytes_read += read(to + bytes_read, n - bytes_read);
    }

    return bytes_read;
}

}
