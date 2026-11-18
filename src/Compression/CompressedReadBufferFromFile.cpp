
#include <Compression/CompressedReadBufferFromFile.h>

#include <Compression/CompressionInfo.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <IO/AsynchronousReadBufferFromFileDescriptor.h>
#include <IO/MMapReadBufferFromFileDescriptor.h>
#include <IO/MMapReadBufferFromFileWithCache.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>


namespace ProfileEvents
{
    extern const Event CompressedReadBufferBlocks;
    extern const Event CompressedReadBufferBytes;
    extern const Event CompressedReadBufferPartialBlocks;
    extern const Event CompressedReadBufferSkippedBytes;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

bool CompressedReadBufferFromFile::startPartialDecompression(size_t size_decompressed, size_t size_compressed_without_checksum, size_t min_bytes)
{
    /// It pays off only if at least one block of the frame is left undecompressed.
    if (!codec || codec->getMethodByte() != static_cast<uint8_t>(CompressionMethodByte::ZSTD)
        || min_bytes + ZstdIncrementalDecompressor::max_block_size >= size_decompressed)
        return false;

    const UInt8 header_size = ICompressionCodec::getHeaderSize();
    if (size_compressed_without_checksum <= header_size
        || !partial_decompressor.start(
            compressed_buffer + header_size, size_compressed_without_checksum - header_size, memory.data(), size_decompressed))
        return false;

    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBlocks);
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferPartialBlocks);
    continuePartialDecompression(min_bytes);
    return true;
}

void CompressedReadBufferFromFile::continuePartialDecompression(size_t min_bytes)
{
    const size_t decompressed_before = partial_decompressor.decompressedBytes();
    const size_t decompressed = partial_decompressor.decompressAtLeast(min_bytes);
    ProfileEvents::increment(ProfileEvents::CompressedReadBufferBytes, decompressed - decompressed_before);
    working_buffer = WorkingBuffer::fromPaddedStorage(memory, 0, decompressed);
}

void CompressedReadBufferFromFile::resetPartialDecompression()
{
    if (size_t skipped = partial_decompressor.reset())
        ProfileEvents::increment(ProfileEvents::CompressedReadBufferSkippedBytes, skipped);
}

/// Whether the data that `in` has returned stays valid until `in` is read or seeked again, even if `prefetch`,
/// `setReadUntilPosition`, `setReadUntilEnd` or `setRequestMap` is called in between. The local file buffers do not
/// override the latter three, and their `prefetch` reads into another buffer or only advises the OS. The mmap buffers
/// keep their mapping while they exist. Other buffers, for example `PipelineReadBuffer`, may free their data in these calls.
static bool keepsReturnedData(const ReadBufferFromFileBase & in)
{
    return dynamic_cast<const AsynchronousReadBufferFromFileDescriptor *>(&in)
        || dynamic_cast<const ReadBufferFromFileDescriptor *>(&in)
        || dynamic_cast<const MMapReadBufferFromFileWithCache *>(&in)
        || dynamic_cast<const MMapReadBufferFromFileDescriptor *>(&in);
}

void CompressedReadBufferFromFile::detachPartialDecompressionSource()
{
    if (!partial_decompressor.inProgress() || keepsReturnedData(file_in))
        return;

    const char * source = partial_decompressor.source();
    const size_t source_size = partial_decompressor.sourceSize();
    if (source >= own_compressed_buffer.data() && source + source_size <= own_compressed_buffer.data() + own_compressed_buffer.size())
        return;

    own_compressed_buffer.resize(source_size);
    memcpy(own_compressed_buffer.data(), source, source_size);
    partial_decompressor.relocateSource(own_compressed_buffer.data());
    /// The header and the checksum of `own_compressed_buffer` are not valid now.
    own_compressed_buffer_header_init = false;
}

/// Cached: `getLogger` takes a process-global mutex, and these buffers are created per column stream
/// of every read task.
static LoggerPtr getCompressedReadBufferFromFileLogger()
{
    static LoggerPtr log = getLogger("CompressedReadBufferFromFile");
    return log;
}

bool CompressedReadBufferFromFile::nextImpl()
{
    try
    {
        if (partial_decompressor.inProgress())
        {
            /// Decompress the next part of the block after the decompressed one, which ZSTD needs as the history.
            /// `working_buffer` keeps starting at the beginning of the block, so skip the old part and compensate `bytes`,
            /// to which `next` has already added the consumed size.
            const size_t old_size = working_buffer.size();
            continuePartialDecompression(old_size + 1);
            bytes -= old_size;
            nextimpl_working_buffer_offset = old_size;
            return true;
        }

        size_t size_decompressed = 0;
        size_t size_compressed_without_checksum = 0;
        size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum, false);
        if (!size_compressed)
            return false;

        auto additional_size_at_the_end_of_buffer = codec->getAdditionalSizeAtTheEndOfBuffer();

        /// This is for clang static analyzer.
        chassert(size_decompressed + additional_size_at_the_end_of_buffer > 0);

        memory.resize(size_decompressed + additional_size_at_the_end_of_buffer);
        working_buffer = WorkingBuffer::fromPaddedStorage(memory, 0, size_decompressed);

        decompress(working_buffer, size_decompressed, size_compressed_without_checksum);

        /// nextimpl_working_buffer_offset is set in the seek function (lazy seek). So we have to
        /// check that we are not seeking beyond working buffer.
        if (nextimpl_working_buffer_offset > working_buffer.size())
            throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Required to move position beyond the decompressed block (pos: "
            "{}, block size: {})", nextimpl_working_buffer_offset, toString(working_buffer.size()));

        return true;
    }
    catch (Exception & e)
    {
        addDiagnostics(e);
        throw;
    }
}


CompressedReadBufferFromFile::CompressedReadBufferFromFile(std::unique_ptr<ReadBufferFromFileBase> buf, bool allow_different_codecs_)
    : BufferWithOwnMemory<ReadBuffer>(0)
    , p_file_in(std::move(buf))
    , file_in(*p_file_in)
    , log(getCompressedReadBufferFromFileLogger(), /* allowed_count */ 1, /* interval */ 1)
{
    compressed_in = &file_in;
    allow_different_codecs = allow_different_codecs_;
    LOG_TEST(log, "Decompressing {}", file_in.getFileName());
}


void CompressedReadBufferFromFile::prefetch(Priority priority)
{
    detachPartialDecompressionSource();
    file_in.prefetch(priority);
}


void CompressedReadBufferFromFile::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    chassert(size_compressed || working_buffer.empty());

    if (partial_decompressor.inProgress())
    {
        if (size_compressed && offset_in_compressed_file == file_in.getPosition() - size_compressed)
        {
            /// Seek within the current block: decompress it up to the required position.
            if (offset_in_decompressed_block > working_buffer.size())
            {
                const size_t pos_offset = offset();
                continuePartialDecompression(offset_in_decompressed_block);
                pos = working_buffer.begin() + pos_offset;
            }
        }
        else
            resetPartialDecompression();
    }

    /// Nothing to do if we already at required position
    if (!size_compressed && static_cast<size_t>(file_in.getPosition()) == offset_in_compressed_file && /// correct position in compressed file
        nextimpl_working_buffer_offset == offset_in_decompressed_block) /// we will move our position to correct one
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
        LOG_TEST(log, "Seek to position {} in compressed file {}", offset_in_compressed_file, file_in.getFileName());
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

off_t CompressedReadBufferFromFile::getPosition() const
{
    return file_in.getPosition();
}

size_t CompressedReadBufferFromFile::readBig(char * to, size_t n)
{
    try
    {
        size_t bytes_read = 0;

        /// If there are unread bytes in the buffer, then we copy needed to `to`.
        if (pos < working_buffer.end())
            bytes_read += read(to, std::min(static_cast<size_t>(working_buffer.end() - pos), n));

        /// If you need to read more - we will, if possible, decompress at once to `to`.
        while (bytes_read < n)
        {
            if (partial_decompressor.inProgress())
            {
                /// The requested data continues in the not yet decompressed part of the current block.
                if (!next())
                    return bytes_read;
                bytes_read += read(to + bytes_read, std::min(available(), n - bytes_read));
                continue;
            }

            size_t size_decompressed = 0;
            size_t size_compressed_without_checksum = 0;

            bytes += offset();
            resetWorkingBuffer();
            size_compressed = 0;
            size_t new_size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum, false);

            if (!new_size_compressed)
                return bytes_read;

            auto additional_size_at_the_end_of_buffer = codec->getAdditionalSizeAtTheEndOfBuffer();

            /// If the decompressed block fits entirely where it needs to be copied and we don't
            /// need to skip some bytes in decompressed data (seek happened before readBig call).
            if (nextimpl_working_buffer_offset == 0 && size_decompressed + additional_size_at_the_end_of_buffer <= n - bytes_read)
            {
                decompressTo(to + bytes_read, size_decompressed, size_compressed_without_checksum);
                bytes_read += size_decompressed;
                bytes += size_decompressed;
            }
            else if (nextimpl_working_buffer_offset > 0)
            {
                /// Need to skip some bytes in decompressed data (seek happened before readBig call).
                size_compressed = new_size_compressed;

                /// This is for clang static analyzer.
                chassert(size_decompressed + additional_size_at_the_end_of_buffer > 0);
                memory.resize(size_decompressed + additional_size_at_the_end_of_buffer);
                working_buffer = WorkingBuffer::fromPaddedStorage(memory, 0, size_decompressed);
                /// Synchronous mode must be set since we need read partial data immediately from working buffer to target buffer.
                decompress(working_buffer, size_decompressed, size_compressed_without_checksum);

                if (nextimpl_working_buffer_offset > working_buffer.size())
                    throw Exception(
                        ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
                        "Required to move position beyond the decompressed block (pos: {}, block size: {})",
                        nextimpl_working_buffer_offset,
                        toString(working_buffer.size()));

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

                /// This is for clang static analyzer.
                chassert(size_decompressed + additional_size_at_the_end_of_buffer > 0);
                memory.resize(size_decompressed + additional_size_at_the_end_of_buffer);

                /// Only the beginning of this block is needed now: try to decompress just that.
                if (!startPartialDecompression(size_decompressed, size_compressed_without_checksum, n - bytes_read))
                {
                    working_buffer = WorkingBuffer::fromPaddedStorage(memory, 0, size_decompressed);
                    // Asynchronous mode can be set here because working_buffer wouldn't be overwritten any more since this is the last block.
                    decompress(working_buffer, size_decompressed, size_compressed_without_checksum);
                }

                /// Read partial data from last block.
                pos = working_buffer.begin();
                bytes_read += read(to + bytes_read, n - bytes_read);
                break;
            }
        }

        return bytes_read;
    }
    catch (Exception & e)
    {
        addDiagnostics(e);
        throw;
    }
}

}
