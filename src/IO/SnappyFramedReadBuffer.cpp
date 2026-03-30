#include "config.h"

#if USE_SNAPPY
#include <cstring>

#include <snappy.h>
#include <crc32c/crc32c.h>

#include <IO/SnappyFramedReadBuffer.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SNAPPY_UNCOMPRESS_FAILED;
}

namespace
{

/// Snappy framing format constants.
/// See https://github.com/google/snappy/blob/main/framing_format.txt

constexpr uint8_t STREAM_IDENTIFIER[] = {0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59};

constexpr uint8_t CHUNK_TYPE_COMPRESSED = 0x00;
constexpr uint8_t CHUNK_TYPE_UNCOMPRESSED = 0x01;
constexpr uint8_t CHUNK_TYPE_STREAM_IDENTIFIER = 0xff;

/// Maximum uncompressed data per chunk in snappy framing format.
constexpr size_t MAX_UNCOMPRESSED_CHUNK_SIZE = 65536;

/// Compute masked CRC-32C as defined by snappy framing format.
uint32_t maskedCrc32c(const char * data, size_t size)
{
    uint32_t crc = crc32c::Crc32c(data, size);
    return ((crc >> 15) | (crc << 17)) + 0xa282ead8;
}

}

SnappyFramedReadBuffer::SnappyFramedReadBuffer(
    std::unique_ptr<ReadBuffer> in_, size_t buf_size, char * existing_memory, size_t alignment)
    : CompressedReadBufferWrapper(std::move(in_), buf_size, existing_memory, alignment)
{
}

bool SnappyFramedReadBuffer::readExact(char * dst, size_t size)
{
    size_t bytes_read = 0;
    while (bytes_read < size)
    {
        if (in->eof())
            return false;
        size_t available = in->buffer().end() - in->position();
        size_t to_copy = std::min(available, size - bytes_read);
        memcpy(dst + bytes_read, in->position(), to_copy);
        in->position() += to_copy;
        bytes_read += to_copy;
    }
    return true;
}

bool SnappyFramedReadBuffer::readStreamIdentifier()
{
    /// Check if the stream is truly empty (EOF before any bytes).
    if (in->eof())
        return false;

    uint8_t header[sizeof(STREAM_IDENTIFIER)];
    if (!readExact(reinterpret_cast<char *>(header), sizeof(header)))
        throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Truncated snappy stream: incomplete stream identifier");
    if (memcmp(header, STREAM_IDENTIFIER, sizeof(STREAM_IDENTIFIER)) != 0)
        throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Invalid snappy framing format: bad stream identifier");
    identifier_read = true;
    return true;
}

bool SnappyFramedReadBuffer::nextImpl()
{
    if (!identifier_read)
    {
        if (!readStreamIdentifier())
            return false;
    }

    while (true)
    {
        /// Read chunk header: 1 byte type + 3 bytes little-endian length.
        uint8_t chunk_header[4];
        if (!readExact(reinterpret_cast<char *>(chunk_header), 4))
            return false;

        uint8_t chunk_type = chunk_header[0];
        size_t chunk_data_size = chunk_header[1]
            | (static_cast<size_t>(chunk_header[2]) << 8)
            | (static_cast<size_t>(chunk_header[3]) << 16);

        if (chunk_type == CHUNK_TYPE_STREAM_IDENTIFIER)
        {
            /// Stream identifier can appear again mid-stream; skip its payload.
            if (chunk_data_size != 6)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Invalid snappy stream identifier chunk size");
            uint8_t payload[6];
            if (!readExact(reinterpret_cast<char *>(payload), 6))
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Truncated snappy stream identifier");
            continue;
        }

        if (chunk_type == CHUNK_TYPE_COMPRESSED)
        {
            if (chunk_data_size < 4)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Compressed snappy chunk too small");

            /// Read checksum (4 bytes) + compressed payload.
            size_t payload_size = chunk_data_size - 4;
            String chunk_buf(chunk_data_size, '\0');
            if (!readExact(chunk_buf.data(), chunk_data_size))
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Truncated snappy compressed chunk");

            uint32_t expected_crc = 0;
            memcpy(&expected_crc, chunk_buf.data(), 4);

            const char * compressed = chunk_buf.data() + 4;

            size_t uncompressed_length = 0;
            if (!snappy::GetUncompressedLength(compressed, payload_size, &uncompressed_length))
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Cannot determine snappy uncompressed length");

            /// Snappy framing format limits uncompressed data per chunk to 65536 bytes.
            /// Reject larger values to prevent untrusted input from forcing huge allocations.
            if (uncompressed_length > MAX_UNCOMPRESSED_CHUNK_SIZE)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Snappy chunk claims uncompressed length {} which exceeds the framing format limit of {}",
                    uncompressed_length, MAX_UNCOMPRESSED_CHUNK_SIZE);

            decompress_buffer.resize(uncompressed_length);
            if (!snappy::RawUncompress(compressed, payload_size, decompress_buffer.data()))
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Snappy decompression failed");

            uint32_t actual_crc = maskedCrc32c(decompress_buffer.data(), uncompressed_length);
            if (actual_crc != expected_crc)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Snappy CRC-32C checksum mismatch");

            working_buffer = Buffer(decompress_buffer.data(), decompress_buffer.data() + uncompressed_length);
            return true;
        }

        if (chunk_type == CHUNK_TYPE_UNCOMPRESSED)
        {
            if (chunk_data_size < 4)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Uncompressed snappy chunk too small");

            size_t payload_size = chunk_data_size - 4;
            String chunk_buf(chunk_data_size, '\0');
            if (!readExact(chunk_buf.data(), chunk_data_size))
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Truncated snappy uncompressed chunk");

            uint32_t expected_crc = 0;
            memcpy(&expected_crc, chunk_buf.data(), 4);

            decompress_buffer.assign(chunk_buf.data() + 4, payload_size);

            uint32_t actual_crc = maskedCrc32c(decompress_buffer.data(), payload_size);
            if (actual_crc != expected_crc)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Snappy CRC-32C checksum mismatch");

            working_buffer = Buffer(decompress_buffer.data(), decompress_buffer.data() + payload_size);
            return true;
        }

        if (chunk_type >= 0x80)
        {
            /// Skippable chunks (0x80–0xfe): skip the payload.
            String skip_buf(chunk_data_size, '\0');
            if (!readExact(skip_buf.data(), chunk_data_size))
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Truncated snappy skippable chunk");
            continue;
        }

        /// Unskippable reserved chunk types (0x02–0x7f) — must fail.
        throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
            "Unknown unskippable snappy chunk type: 0x{:02x}", chunk_type);
    }
}

}

#endif
