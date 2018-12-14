#include <IO/CompressedReadBufferFromFile.h>
#include <IO/createReadBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/CompressedStream.h>
#include <IO/LZ4_decompress_faster.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


bool CompressedReadBufferFromFile::nextImpl()
{
    size_t size_decompressed;
    size_t size_compressed_without_checksum;
    size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum);
    if (!size_compressed)
        return false;

    memory.resize(size_decompressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
    working_buffer = Buffer(memory.data(), &memory[size_decompressed]);

    decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

    return true;
}


CompressedReadBufferFromFile::CompressedReadBufferFromFile(
    const std::string & path, size_t estimated_size, size_t aio_threshold, size_t buf_size)
    : BufferWithOwnMemory<ReadBuffer>(0),
        p_file_in(createReadBufferFromFileBase(path, estimated_size, aio_threshold, buf_size)),
        file_in(*p_file_in)
{
    compressed_in = &file_in;
}


void CompressedReadBufferFromFile::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    if (size_compressed &&
        offset_in_compressed_file == file_in.getPositionInFile() - size_compressed &&
        offset_in_decompressed_block <= working_buffer.size())
    {
        bytes += offset();
        pos = working_buffer.begin() + offset_in_decompressed_block;
        /// `bytes` can overflow and get negative, but in `count()` everything will overflow back and get right.
        bytes -= offset();
    }
    else
    {
        file_in.seek(offset_in_compressed_file);

        bytes += offset();
        nextImpl();

        if (offset_in_decompressed_block > working_buffer.size())
            throw Exception("Seek position is beyond the decompressed block"
            " (pos: " + toString(offset_in_decompressed_block) + ", block size: " + toString(working_buffer.size()) + ")",
            ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

        pos = working_buffer.begin() + offset_in_decompressed_block;
        bytes -= offset();
    }
}


size_t CompressedReadBufferFromFile::readBig(char * to, size_t n)
{
    size_t bytes_read = 0;

    /// If there are unread bytes in the buffer, then we copy needed to `to`.
    if (pos < working_buffer.end())
        bytes_read += read(to, std::min(static_cast<size_t>(working_buffer.end() - pos), n));

    /// If you need to read more - we will, if possible, decompress at once to `to`.
    while (bytes_read < n)
    {
        size_t size_decompressed = 0;
        size_t size_compressed_without_checksum = 0;

        size_t new_size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum);
        size_compressed = 0; /// file_in no longer points to the end of the block in working_buffer.
        if (!new_size_compressed)
            return bytes_read;

        /// If the decompressed block fits entirely where it needs to be copied.
        if (size_decompressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER <= n - bytes_read)
        {
            decompress(to + bytes_read, size_decompressed, size_compressed_without_checksum);
            bytes_read += size_decompressed;
            bytes += size_decompressed;
        }
        else
        {
            size_compressed = new_size_compressed;
            bytes += offset();
            memory.resize(size_decompressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
            working_buffer = Buffer(memory.data(), &memory[size_decompressed]);
            pos = working_buffer.begin();

            decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

            bytes_read += read(to + bytes_read, n - bytes_read);
            break;
        }
    }

    return bytes_read;
}

}
