#include <IO/LibdeflateInflatingReadBuffer.h>

#if USE_LIBDEFLATE

#include <Common/Exception.h>
#include <IO/WithFileName.h>

#include <libdeflate.h>

#include <algorithm>
#include <cstring>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_DECOMPRESS;
}

namespace
{
    constexpr size_t DEFLATE_WINDOW = 32768;
    /// Max compressed bytes pulled into in_buf per refill (bounds memory regardless of nested buffer size).
    constexpr size_t INPUT_CHUNK = 1u << 20;

    /// gzip header flag bits (RFC 1952).
    constexpr uint8_t GZIP_FHCRC = 1 << 1;
    constexpr uint8_t GZIP_FEXTRA = 1 << 2;
    constexpr uint8_t GZIP_FNAME = 1 << 3;
    constexpr uint8_t GZIP_FCOMMENT = 1 << 4;
    /// Bits 5..7 of FLG are reserved and must be zero (RFC 1952, section 2.3.1).
    constexpr uint8_t GZIP_FRESERVED = 0xE0;

    uint32_t readLE32(const char * p)
    {
        return static_cast<uint8_t>(p[0]) | (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8)
            | (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16) | (static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24);
    }
    uint32_t readBE32(const char * p)
    {
        return (static_cast<uint32_t>(static_cast<uint8_t>(p[0])) << 24) | (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 16)
            | (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 8) | static_cast<uint8_t>(p[3]);
    }
}

LibdeflateInflatingReadBuffer::LibdeflateInflatingReadBuffer(
    std::unique_ptr<ReadBuffer> in_,
    CompressionMethod compression_method,
    size_t buf_size,
    char * /*existing_memory*/,
    size_t alignment)
    /// memory holds [32 KiB window][output region]; we manage existing_memory ourselves, so don't pass it.
    : CompressedReadBufferWrapper(std::move(in_), DEFLATE_WINDOW + buf_size, nullptr, alignment)
    , gzip(compression_method == CompressionMethod::Gzip)
    , state(State::Header)
    , out_capacity(buf_size)
{
    if (compression_method != CompressionMethod::Gzip && compression_method != CompressionMethod::Zlib)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "LibdeflateInflatingReadBuffer supports only gzip and zlib formats");

    /// Own the decompressor through a guard until the throwing tracked allocation below succeeds:
    /// if `in_buf.resize` throws (for example on `MEMORY_LIMIT_EXCEEDED`) the constructor unwinds
    /// without running `~LibdeflateInflatingReadBuffer`, which would otherwise leak the handle.
    std::unique_ptr<libdeflate_decompressor, decltype(&libdeflate_free_decompressor)> decompressor_holder(
        libdeflate_alloc_decompressor(), &libdeflate_free_decompressor);
    if (!decompressor_holder)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Failed to allocate libdeflate decompressor");

    in_buf.resize(std::max<size_t>(buf_size, 16384));

    decompressor = decompressor_holder.release();
}

LibdeflateInflatingReadBuffer::~LibdeflateInflatingReadBuffer()
{
    libdeflate_free_decompressor(decompressor);
}

bool LibdeflateInflatingReadBuffer::fillInput()
{
    /// Compact consumed bytes to the front.
    if (in_pos > 0)
    {
        memmove(in_buf.data(), in_buf.data() + in_pos, in_end - in_pos);
        in_end -= in_pos;
        in_pos = 0;
    }

    in->nextIfAtEnd();
    /// Copy a bounded amount per call so in_buf stays small even when the nested buffer exposes a
    /// lot at once (e.g. a memory-mapped file): the rest stays in the nested buffer for next time.
    /// libdeflate consumes whole DEFLATE blocks, so the leftover we must keep is at most one block.
    const size_t avail = std::min<size_t>(in->buffer().end() - in->position(), INPUT_CHUNK);
    if (avail == 0)
    {
        input_eof = true;
        return false;
    }

    if (in_end + avail > in_buf.size())
        in_buf.resize(in_end + avail);
    memcpy(in_buf.data() + in_end, in->position(), avail);
    in->position() += avail;
    in_end += avail;
    return true;
}

bool LibdeflateInflatingReadBuffer::ensureInput(size_t need)
{
    while (in_end - in_pos < need)
        if (!fillInput())
            return false;
    return true;
}

bool LibdeflateInflatingReadBuffer::parseHeader()
{
    if (gzip)
    {
        if (!ensureInput(10))
        {
            if (in_end == in_pos)
                return false; /* clean end: no more members */
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated gzip header");
        }
        const char * p = in_buf.data() + in_pos;
        if (static_cast<uint8_t>(p[0]) != 0x1f || static_cast<uint8_t>(p[1]) != 0x8b)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Not a gzip stream");
        if (static_cast<uint8_t>(p[2]) != 8)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Unsupported gzip compression method");
        const uint8_t flg = static_cast<uint8_t>(p[3]);
        if (flg & GZIP_FRESERVED)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Reserved gzip header flag bits are set");

        /// CRC-32 of all header bytes up to (but not including) the optional FHCRC field, used to verify
        /// FHCRC when present (RFC 1952). It is accumulated incrementally as bytes are consumed, because
        /// fillInput() may discard already-read bytes from in_buf when it compacts, so we cannot defer the
        /// CRC to a single pass over the whole header at the end.
        uint32_t header_crc = libdeflate_crc32(0, in_buf.data() + in_pos, 10);
        in_pos += 10;

        if (flg & GZIP_FEXTRA)
        {
            if (!ensureInput(2))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated gzip header (FEXTRA)");
            header_crc = libdeflate_crc32(header_crc, in_buf.data() + in_pos, 2);
            const size_t xlen = static_cast<uint8_t>(in_buf[in_pos]) | (static_cast<size_t>(static_cast<uint8_t>(in_buf[in_pos + 1])) << 8);
            in_pos += 2;
            if (!ensureInput(xlen))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated gzip header (extra)");
            header_crc = libdeflate_crc32(header_crc, in_buf.data() + in_pos, xlen);
            in_pos += xlen;
        }
        if (flg & GZIP_FNAME)
        {
            uint8_t c = 0;
            do
            {
                if (!ensureInput(1))
                    throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated gzip header (name)");
                c = static_cast<uint8_t>(in_buf[in_pos]);
                header_crc = libdeflate_crc32(header_crc, in_buf.data() + in_pos, 1);
                ++in_pos;
            } while (c != 0);
        }
        if (flg & GZIP_FCOMMENT)
        {
            uint8_t c = 0;
            do
            {
                if (!ensureInput(1))
                    throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated gzip header (comment)");
                c = static_cast<uint8_t>(in_buf[in_pos]);
                header_crc = libdeflate_crc32(header_crc, in_buf.data() + in_pos, 1);
                ++in_pos;
            } while (c != 0);
        }
        if (flg & GZIP_FHCRC)
        {
            if (!ensureInput(2))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated gzip header (hcrc)");
            const uint16_t stored_crc = static_cast<uint8_t>(in_buf[in_pos])
                | static_cast<uint16_t>(static_cast<uint8_t>(in_buf[in_pos + 1]) << 8);
            if (stored_crc != static_cast<uint16_t>(header_crc & 0xFFFF))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "gzip header CRC16 mismatch");
            in_pos += 2;
        }
        checksum = 0; /* CRC32 initial value */
    }
    else
    {
        if (!ensureInput(2))
        {
            if (in_end == in_pos)
                return false;
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated zlib header");
        }
        const uint8_t cmf = static_cast<uint8_t>(in_buf[in_pos]);
        const uint8_t flg = static_cast<uint8_t>(in_buf[in_pos + 1]);
        if ((cmf & 0x0f) != 8)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Unsupported zlib compression method");
        if ((cmf >> 4) > 7)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Invalid zlib window size (CINFO > 7)");
        if (((static_cast<unsigned>(cmf) << 8) | flg) % 31 != 0)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Bad zlib header check bits");
        if (flg & 0x20)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "zlib preset dictionary is not supported");
        in_pos += 2;
        checksum = 1; /* Adler32 initial value */
    }
    return true;
}

bool LibdeflateInflatingReadBuffer::parseTrailer()
{
    if (gzip)
    {
        if (!ensureInput(8))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated gzip trailer");
        const uint32_t crc = readLE32(in_buf.data() + in_pos);
        const uint32_t isize = readLE32(in_buf.data() + in_pos + 4);
        in_pos += 8;
        if (crc != checksum)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "gzip CRC32 mismatch");
        if (isize != static_cast<uint32_t>(member_out))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "gzip ISIZE mismatch");
    }
    else
    {
        if (!ensureInput(4))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated zlib trailer");
        const uint32_t adler = readBE32(in_buf.data() + in_pos);
        in_pos += 4;
        if (adler != checksum)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "zlib Adler32 mismatch");
    }
    return true;
}

bool LibdeflateInflatingReadBuffer::nextImpl()
{
    try
    {
        return decompressImpl();
    }
    catch (Exception & e)
    {
        /// Annotate decompression (and nested read) failures with the source file name, so a corrupted
        /// `.gz`/`url`/`s3` stream reports "While reading from: <file>" like every other codec does.
        const auto file_name = getFileNameFromReadBuffer(*in);
        if (!file_name.empty())
            e.addMessage("While reading from: {}", file_name);
        throw;
    }
}

bool LibdeflateInflatingReadBuffer::decompressImpl()
{
    while (true)
    {
        switch (state)
        {
            case State::Eof:
                return false;

            case State::Header:
            {
                if (!parseHeader())
                {
                    state = State::Eof;
                    return false; /* clean end of stream */
                }
                libdeflate_deflate_decompress_stream_reset(decompressor);
                window_nbytes = 0;
                member_out = 0;
                produced_end = 0;
                state = State::Body;
                [[fallthrough]];
            }

            case State::Body:
            {
                /* Slide the last <=32 KiB of (window + previously produced output) to the front. */
                const size_t recent = produced_end;
                const size_t new_win = std::min(recent, DEFLATE_WINDOW);
                if (new_win)
                    memmove(memory.data(), memory.data() + recent - new_win, new_win);
                window_nbytes = new_win;

                /// Decompress into the output region after the window, accumulating across as many
                /// libdeflate calls as needed. Output produced at a non-final block boundary
                /// (LIBDEFLATE_STREAM_NEED_INPUT) is deliberately NOT exposed yet: the stream may end at
                /// the very next block, and its trailer must be validated before the final bytes reach the
                /// caller. We expose the accumulated output only when the buffer fills at a block boundary
                /// (the stream is then provably incomplete, so more output follows) or once the final block
                /// is decoded and its trailer verified. This mirrors ZlibInflatingReadBuffer, which has
                /// validated the trailer by the time it hands back the final bytes, so a reader that
                /// consumes an exact byte count with readStrict (and never calls nextImpl again) cannot
                /// skip the integrity check.
                size_t produced = 0;
                while (true)
                {
                    if (in_pos == in_end && !input_eof)
                        fillInput();

                    char * out = memory.data() + window_nbytes + produced;
                    const size_t out_avail = memory.size() - window_nbytes - produced;
                    /// The valid history immediately before `out` is the window plus everything produced so
                    /// far in this call (contiguous in `memory`), capped at the 32 KiB back-reference reach.
                    const size_t cur_window = std::min(window_nbytes + produced, DEFLATE_WINDOW);
                    size_t in_used = 0;
                    size_t out_used = 0;
                    const libdeflate_result r = libdeflate_deflate_decompress_stream(
                        decompressor, input_eof ? 1 : 0,
                        in_buf.data() + in_pos, in_end - in_pos,
                        out, out_avail, cur_window, &in_used, &out_used);
                    in_pos += in_used;

                    if (out_used)
                        checksum = gzip ? libdeflate_crc32(checksum, out, out_used)
                                        : libdeflate_adler32(checksum, out, out_used);
                    member_out += out_used;
                    produced += out_used;

                    if (r == LIBDEFLATE_BAD_DATA)
                        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Malformed {} stream", gzip ? "gzip" : "zlib");

                    if (r == LIBDEFLATE_SUCCESS)
                    {
                        /// Final block decoded: verify the gzip/zlib trailer (CRC32/Adler32/ISIZE) and
                        /// advance to the next member (or EOF) before exposing the bytes below.
                        /// finishMember() also updates produced_end for the next member.
                        finishMember();
                        break;
                    }

                    if (r == LIBDEFLATE_STREAM_NEED_INPUT)
                    {
                        /// At a non-final block boundary with the input exhausted. With end_of_input=true
                        /// the decoder never returns NEED_INPUT, so this means more input must exist; if the
                        /// nested stream is already at EOF the data is truncated. Otherwise pull more input
                        /// (or mark EOF, so the retry passes end_of_input=true and the final block finishes)
                        /// and keep accumulating into the same buffer.
                        if (input_eof)
                            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Unexpected end of {} stream", gzip ? "gzip" : "zlib");
                        fillInput();
                        continue;
                    }

                    if (r == LIBDEFLATE_STREAM_NEED_OUTPUT)
                    {
                        if (produced == 0)
                        {
                            /* A single DEFLATE block's uncompressed size exceeds the whole output buffer.
                             * libdeflate's streaming decoder only suspends at block boundaries, so the whole
                             * block must be buffered before any of its output is exposed: grow the buffer (the
                             * 32 KiB window at the front is preserved) and retry. We grow geometrically and
                             * don't impose an artificial ceiling; the buffer is allocated through ClickHouse's
                             * tracked allocator, so a crafted single-block decompression bomb runs into the
                             * query/server memory limit and throws MEMORY_LIMIT_EXCEEDED, exactly like any
                             * other oversized allocation. Every mainstream gzip/zlib/deflate encoder bounds its
                             * blocks to well under a megabyte of uncompressed data (zlib flushes roughly every
                             * lit_bufsize symbols, libdeflate's SOFT_MAX_BLOCK_LENGTH is 300000 bytes, etc.),
                             * so a real-world stream never reaches this grow path; only a hand-crafted one does. */
                            memory.resize(std::max(memory.size() + out_capacity, memory.size() * 2));
                            continue;
                        }
                        /// Output buffer full at a block boundary: the stream is provably incomplete (the
                        /// final block was not reached), so more output follows and it is safe to expose
                        /// what we have without a trailer check. The window carries to the next call.
                        produced_end = window_nbytes + produced;
                        break;
                    }

                    throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Unexpected libdeflate result {}", static_cast<int>(r));
                }

                if (produced)
                {
                    working_buffer = Buffer(memory.data() + window_nbytes, memory.data() + window_nbytes + produced);
                    return true;
                }
                /* No output this call (e.g. SUCCESS of an empty member): advance to the next state. */
                continue;
            }
        }
    }
}

void LibdeflateInflatingReadBuffer::finishMember()
{
    parseTrailer();
    /// Detect whether another concatenated member follows or the stream ends here.
    if (in_pos == in_end && !input_eof)
        fillInput();
    if (in_pos == in_end && input_eof)
    {
        state = State::Eof;
    }
    else
    {
        state = State::Header; /* another concatenated member */
        produced_end = 0;
    }
}

}

#endif
