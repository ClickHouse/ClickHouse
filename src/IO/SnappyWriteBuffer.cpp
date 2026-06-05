#include "config.h"

#if USE_SNAPPY
#include <cstring>

#include <snappy.h>
#include <crc32c/crc32c.h>

#include <IO/SnappyWriteBuffer.h>

namespace DB
{

namespace
{

/// Snappy framing format constants.
/// See https://github.com/google/snappy/blob/main/framing_format.txt

/// Stream identifier: chunk type 0xff, payload "sNaPpY" (6 bytes).
constexpr uint8_t STREAM_IDENTIFIER[] = {0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59};

constexpr uint8_t CHUNK_TYPE_COMPRESSED = 0x00;
constexpr uint8_t CHUNK_TYPE_UNCOMPRESSED = 0x01;

/// Maximum uncompressed data per chunk in snappy framing format.
constexpr size_t MAX_UNCOMPRESSED_CHUNK_SIZE = 65536;

/// Compute masked CRC-32C as defined by snappy framing format:
///   mask = ((crc >> 15) | (crc << 17)) + 0xa282ead8
uint32_t maskedCrc32c(const char * data, size_t size)
{
    uint32_t crc = crc32c::Crc32c(data, size);
    return ((crc >> 15) | (crc << 17)) + 0xa282ead8;
}

}

void SnappyWriteBuffer::writeStreamIdentifier()
{
    out->write(reinterpret_cast<const char *>(STREAM_IDENTIFIER), sizeof(STREAM_IDENTIFIER));
    header_written = true;
}

void SnappyWriteBuffer::writeCompressedChunk(const char * data, size_t size)
{
    /// Compress the data.
    compress_buffer.resize(snappy::MaxCompressedLength(size));
    size_t compressed_size = 0;
    snappy::RawCompress(data, size, compress_buffer.data(), &compressed_size);

    /// Compute masked CRC-32C of the uncompressed data.
    uint32_t crc = maskedCrc32c(data, size);

    uint8_t chunk_type = 0;
    const char * payload = nullptr;
    size_t payload_size = 0;

    if (compressed_size < size)
    {
        chunk_type = CHUNK_TYPE_COMPRESSED;
        payload = compress_buffer.data();
        payload_size = compressed_size;
    }
    else
    {
        /// Compressed data is not smaller — write uncompressed.
        chunk_type = CHUNK_TYPE_UNCOMPRESSED;
        payload = data;
        payload_size = size;
    }

    /// Chunk header: 1 byte type + 3 bytes little-endian length (checksum + payload).
    size_t chunk_data_size = 4 + payload_size; /// 4 bytes for CRC
    uint8_t header[4];
    header[0] = chunk_type;
    header[1] = static_cast<uint8_t>(chunk_data_size & 0xff);
    header[2] = static_cast<uint8_t>((chunk_data_size >> 8) & 0xff);
    header[3] = static_cast<uint8_t>((chunk_data_size >> 16) & 0xff);

    out->write(reinterpret_cast<const char *>(header), 4);

    /// Masked CRC-32C, little-endian.
    uint8_t crc_bytes[4];
    crc_bytes[0] = static_cast<uint8_t>(crc & 0xff);
    crc_bytes[1] = static_cast<uint8_t>((crc >> 8) & 0xff);
    crc_bytes[2] = static_cast<uint8_t>((crc >> 16) & 0xff);
    crc_bytes[3] = static_cast<uint8_t>((crc >> 24) & 0xff);
    out->write(reinterpret_cast<const char *>(crc_bytes), 4);

    out->write(payload, payload_size);
}

void SnappyWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    if (!header_written)
        writeStreamIdentifier();

    const char * data = reinterpret_cast<const char *>(working_buffer.begin());
    size_t remaining = offset();

    /// Split into chunks of at most MAX_UNCOMPRESSED_CHUNK_SIZE bytes.
    while (remaining > 0)
    {
        size_t chunk_size = std::min(remaining, MAX_UNCOMPRESSED_CHUNK_SIZE);
        writeCompressedChunk(data, chunk_size);
        data += chunk_size;
        remaining -= chunk_size;
    }
}

void SnappyWriteBuffer::finalFlushBefore()
{
    next();
    /// Even for empty payloads, emit the stream identifier so the output is a
    /// valid (zero-data) snappy framed stream rather than an empty byte stream.
    if (!header_written)
        writeStreamIdentifier();
}

}

#endif
