#include <Processors/Formats/Impl/ArrowIPC/MessageWriter.h>

#if USE_ARROW

#include <IO/WriteBuffer.h>
#include <IO/NetUtils.h>

namespace DB::ArrowIPC
{

namespace
{

void writeInt32LE(WriteBuffer & out, int32_t value)
{
    value = DB::toLittleEndian(value);
    out.write(reinterpret_cast<const char *>(&value), sizeof(value));
}

size_t roundUpToEight(size_t n)
{
    return (n + 7) & ~static_cast<size_t>(7);
}

}

MessageWriter::WrittenMessage MessageWriter::writeMessage(const uint8_t * metadata, size_t metadata_size, const char * body, size_t body_size)
{
    const int64_t start = bytes_written;

    /// Pad the metadata so that (4 continuation + 4 length + metadata) is a multiple of 8 and the body
    /// starts 8-byte aligned. Since the prefix is 8 bytes, the metadata itself must be a multiple of 8.
    const size_t padded_metadata = roundUpToEight(metadata_size);

    writeInt32LE(out, static_cast<int32_t>(-1)); /// continuation token 0xFFFFFFFF
    writeInt32LE(out, static_cast<int32_t>(padded_metadata));
    out.write(reinterpret_cast<const char *>(metadata), metadata_size);
    for (size_t i = metadata_size; i < padded_metadata; ++i)
        out.write('\0');
    if (body_size > 0)
        out.write(body, body_size);

    const int32_t metadata_length = static_cast<int32_t>(2 * sizeof(int32_t) + padded_metadata);
    bytes_written += metadata_length + static_cast<int64_t>(body_size);
    return WrittenMessage{start, metadata_length, static_cast<int64_t>(body_size)};
}

void MessageWriter::writeEOS()
{
    writeInt32LE(out, static_cast<int32_t>(-1));
    writeInt32LE(out, 0);
    bytes_written += 2 * sizeof(int32_t);
}

void MessageWriter::writeRaw(const char * data, size_t size)
{
    out.write(data, size);
    bytes_written += static_cast<int64_t>(size);
}

}

#endif
