#pragma once

#include "config.h"

#if USE_LIBDEFLATE

#include <IO/ReadBuffer.h>
#include <IO/CompressedReadBufferWrapper.h>
#include <IO/CompressionMethod.h>
#include <Common/PODArray.h>

#include <cstdint>

struct libdeflate_decompressor;

namespace DB
{

/// Streaming gzip/zlib decompressor built on libdeflate's block-boundary-suspendable decoder
/// (libdeflate_deflate_decompress_stream, a ClickHouse addition). It is faster than the zlib
/// streaming path while keeping memory bounded.
///
/// We parse the gzip/zlib header and trailer ourselves and feed the raw DEFLATE body to libdeflate,
/// carrying the last 32 KiB of output as the back-reference window. Concatenated members are handled
/// seamlessly. The CRC32 (gzip) / Adler32 (zlib) trailer is verified.
class LibdeflateInflatingReadBuffer : public CompressedReadBufferWrapper
{
public:
    LibdeflateInflatingReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        CompressionMethod compression_method,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~LibdeflateInflatingReadBuffer() override;

private:
    bool nextImpl() override;
    /// The actual decompression loop. nextImpl wraps it to annotate decompression failures with the
    /// source file name (matching the other *InflatingReadBuffer codecs), so errors on a corrupted
    /// `.gz`/`url`/`s3` source carry "While reading from: <file>" context.
    bool decompressImpl();

    /// Make at least one more byte of compressed input available in in_buf (read from `in`).
    /// Returns false if the nested stream is at EOF.
    bool fillInput();
    /// Ensure `need` bytes are buffered in in_buf starting at in_pos (for header/trailer parsing).
    /// Returns false if EOF reached before `need` bytes are available.
    bool ensureInput(size_t need);

    /// Parse one member header at in_pos (gzip or zlib). Returns true when fully parsed.
    bool parseHeader();
    /// Read and verify the member trailer. Returns true when done.
    bool parseTrailer();
    /// Called once a member's DEFLATE body completes: verify its trailer and move to the next
    /// member (State::Header) or end of stream (State::Eof). Done before the member's final bytes
    /// are returned so the integrity check is never skipped by an exact-size read.
    void finishMember();

    libdeflate_decompressor * decompressor = nullptr;
    const bool gzip;

    enum class State
    {
        Header,
        Body,
        Eof,
    };
    State state;

    /// Accumulated compressed input: valid bytes are in_buf[in_pos .. in_end).
    PODArray<char> in_buf;
    size_t in_pos = 0;
    size_t in_end = 0;
    bool input_eof = false;

    /// Output buffer layout: memory = [32 KiB window][output region]. window_nbytes <= 32 KiB.
    /// produced_end is the offset in `memory` just past the output of the previous nextImpl call
    /// (window + last output, contiguous), used to slide the window on the next call.
    size_t window_nbytes = 0;
    size_t produced_end = 0;
    size_t out_capacity;

    /// Per-member checksum/length accumulators for trailer verification.
    uint32_t checksum = 0;
    uint64_t member_out = 0;
};

}

#endif
