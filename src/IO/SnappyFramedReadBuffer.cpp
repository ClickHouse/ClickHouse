#include "config.h"

#if USE_SNAPPY
#include <cstring>

#include <snappy.h>
#include <crc32c/crc32c.h>

#include <IO/SnappyFramedReadBuffer.h>
#include <IO/WithFileName.h>
#include <Common/Exception.h>
#include <base/unaligned.h>

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

bool SnappyFramedReadBuffer::readExact(char * dst, size_t size, bool allow_partial_eof)
{
    size_t bytes_read = 0;
    while (bytes_read < size)
    {
        if (in->eof())
        {
            if (bytes_read == 0 && allow_partial_eof)
                return false;
            throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                "Truncated snappy stream: expected {} bytes but got {}{}",
                size, bytes_read, getExceptionEntryWithFileName(*in));
        }
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
    /// readExact with allow_partial_eof=false: any truncation throws.
    readExact(reinterpret_cast<char *>(header), sizeof(header), /*allow_partial_eof=*/false);
    if (memcmp(header, STREAM_IDENTIFIER, sizeof(STREAM_IDENTIFIER)) != 0)
        throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
            "Invalid snappy framing format: bad stream identifier{}",
            getExceptionEntryWithFileName(*in));
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
        /// Clean EOF at a chunk boundary is valid (end of stream).
        uint8_t chunk_header[4];
        if (!readExact(reinterpret_cast<char *>(chunk_header), 4, /*allow_partial_eof=*/true))
            return false;

        uint8_t chunk_type = chunk_header[0];
        size_t chunk_data_size = chunk_header[1]
            | (static_cast<size_t>(chunk_header[2]) << 8)
            | (static_cast<size_t>(chunk_header[3]) << 16);

        if (chunk_type == CHUNK_TYPE_STREAM_IDENTIFIER)
        {
            /// Stream identifier can appear again mid-stream; validate its payload.
            if (chunk_data_size != 6)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Invalid snappy stream identifier chunk size{}",
                    getExceptionEntryWithFileName(*in));
            uint8_t payload[6];
            readExact(reinterpret_cast<char *>(payload), 6, /*allow_partial_eof=*/false);
            /// The payload must be exactly "sNaPpY" (bytes 4..9 of the stream identifier).
            if (memcmp(payload, STREAM_IDENTIFIER + 4, 6) != 0)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Invalid snappy stream identifier payload{}",
                    getExceptionEntryWithFileName(*in));
            continue;
        }

        if (chunk_type == CHUNK_TYPE_COMPRESSED)
        {
            if (chunk_data_size < 4)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Compressed snappy chunk too small{}",
                    getExceptionEntryWithFileName(*in));

            /// Read checksum (4 bytes) + compressed payload.
            size_t payload_size = chunk_data_size - 4;

            /// Reject oversized compressed payloads before allocating, so attacker-controlled
            /// chunk header lengths cannot force large per-request memory spikes prior to the
            /// later uncompressed-length / CRC / decode checks. The maximum valid compressed
            /// payload size for a chunk is bounded by `snappy::MaxCompressedLength` of the
            /// snappy framing format's per-chunk uncompressed limit.
            const size_t max_compressed_payload_size = snappy::MaxCompressedLength(MAX_UNCOMPRESSED_CHUNK_SIZE);
            if (payload_size > max_compressed_payload_size)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Snappy compressed chunk payload size {} exceeds the framing format limit of {}{}",
                    payload_size, max_compressed_payload_size, getExceptionEntryWithFileName(*in));

            String chunk_buf(chunk_data_size, '\0');
            readExact(chunk_buf.data(), chunk_data_size, /*allow_partial_eof=*/false);

            /// Snappy framing format encodes the masked CRC-32C as little-endian.
            uint32_t expected_crc = unalignedLoadLittleEndian<uint32_t>(chunk_buf.data());

            const char * compressed = chunk_buf.data() + 4;

            size_t uncompressed_length = 0;
            if (!snappy::GetUncompressedLength(compressed, payload_size, &uncompressed_length))
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Cannot determine snappy uncompressed length{}",
                    getExceptionEntryWithFileName(*in));

            /// Snappy framing format limits uncompressed data per chunk to 65536 bytes.
            /// Reject larger values to prevent untrusted input from forcing huge allocations.
            if (uncompressed_length > MAX_UNCOMPRESSED_CHUNK_SIZE)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Snappy chunk claims uncompressed length {} which exceeds the framing format limit of {}{}",
                    uncompressed_length, MAX_UNCOMPRESSED_CHUNK_SIZE, getExceptionEntryWithFileName(*in));

            decompress_buffer.resize(uncompressed_length);
            if (!snappy::RawUncompress(compressed, payload_size, decompress_buffer.data()))
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Snappy decompression failed{}",
                    getExceptionEntryWithFileName(*in));

            uint32_t actual_crc = maskedCrc32c(decompress_buffer.data(), uncompressed_length);
            if (actual_crc != expected_crc)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Snappy CRC-32C checksum mismatch{}",
                    getExceptionEntryWithFileName(*in));

            /// `ReadBuffer::next` recursively calls itself when `nextImpl` returns true with an
            /// empty `working_buffer`, so a crafted stream of empty chunks could exhaust the stack.
            /// Skip zero-length chunks at the source.
            if (uncompressed_length == 0)
                continue;

            working_buffer = Buffer(decompress_buffer.data(), decompress_buffer.data() + uncompressed_length);
            return true;
        }

        if (chunk_type == CHUNK_TYPE_UNCOMPRESSED)
        {
            if (chunk_data_size < 4)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Uncompressed snappy chunk too small{}",
                    getExceptionEntryWithFileName(*in));

            size_t payload_size = chunk_data_size - 4;

            /// Enforce the same framing format limit for uncompressed chunks.
            if (payload_size > MAX_UNCOMPRESSED_CHUNK_SIZE)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Uncompressed snappy chunk payload size {} exceeds the framing format limit of {}{}",
                    payload_size, MAX_UNCOMPRESSED_CHUNK_SIZE, getExceptionEntryWithFileName(*in));

            String chunk_buf(chunk_data_size, '\0');
            readExact(chunk_buf.data(), chunk_data_size, /*allow_partial_eof=*/false);

            /// Snappy framing format encodes the masked CRC-32C as little-endian.
            uint32_t expected_crc = unalignedLoadLittleEndian<uint32_t>(chunk_buf.data());

            decompress_buffer.assign(chunk_buf.data() + 4, payload_size);

            uint32_t actual_crc = maskedCrc32c(decompress_buffer.data(), payload_size);
            if (actual_crc != expected_crc)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Snappy CRC-32C checksum mismatch{}",
                    getExceptionEntryWithFileName(*in));

            /// See the compressed-chunk branch above: skip zero-length chunks to avoid driving
            /// `ReadBuffer::next` into unbounded recursion on a crafted stream of empty chunks.
            if (payload_size == 0)
                continue;

            working_buffer = Buffer(decompress_buffer.data(), decompress_buffer.data() + payload_size);
            return true;
        }

        if (chunk_type >= 0x80)
        {
            /// Skippable chunks (0x80–0xfe): skip the payload without materializing
            /// it, so an attacker-controlled 24-bit `chunk_data_size` (up to 16 MB)
            /// in untrusted HTTP input cannot force large per-frame allocations.
            size_t skipped = in->tryIgnore(chunk_data_size);
            if (skipped != chunk_data_size)
                throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
                    "Truncated snappy stream: skippable chunk expected {} bytes but got {}{}",
                    chunk_data_size, skipped, getExceptionEntryWithFileName(*in));
            continue;
        }

        /// Unskippable reserved chunk types (0x02–0x7f) — must fail.
        throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED,
            "Unknown unskippable snappy chunk type: 0x{:02x}{}",
            chunk_type, getExceptionEntryWithFileName(*in));
    }
}

}

#endif
