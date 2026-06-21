#include <IO/LibdeflateInflatingReadBuffer.h>

#if USE_LIBDEFLATE

#include <Common/Exception.h>

#include <libdeflate.h>

#include <algorithm>
#include <cstring>

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

    decompressor = libdeflate_alloc_decompressor();
    if (!decompressor)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Failed to allocate libdeflate decompressor");

    in_buf.resize(std::max<size_t>(buf_size, 16384));
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
        in_pos += 10;

        if (flg & GZIP_FEXTRA)
        {
            if (!ensureInput(2))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated gzip header (FEXTRA)");
            const size_t xlen = static_cast<uint8_t>(in_buf[in_pos]) | (static_cast<size_t>(static_cast<uint8_t>(in_buf[in_pos + 1])) << 8);
            in_pos += 2;
            if (!ensureInput(xlen))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated gzip header (extra)");
            in_pos += xlen;
        }
        if (flg & GZIP_FNAME)
            do { if (!ensureInput(1)) throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated gzip header (name)"); } while (in_buf[in_pos++] != 0);
        if (flg & GZIP_FCOMMENT)
            do { if (!ensureInput(1)) throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated gzip header (comment)"); } while (in_buf[in_pos++] != 0);
        if (flg & GZIP_FHCRC)
        {
            if (!ensureInput(2))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Truncated gzip header (hcrc)");
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

                if (in_pos == in_end && !input_eof)
                    fillInput();

                char * out = memory.data() + window_nbytes;
                const size_t out_avail = memory.size() - window_nbytes;
                size_t in_used = 0;
                size_t out_used = 0;
                const libdeflate_result r = libdeflate_deflate_decompress_stream(
                    decompressor, input_eof ? 1 : 0,
                    in_buf.data() + in_pos, in_end - in_pos,
                    out, out_avail, window_nbytes, &in_used, &out_used);
                in_pos += in_used;

                if (out_used)
                    checksum = gzip ? libdeflate_crc32(checksum, out, out_used)
                                    : libdeflate_adler32(checksum, out, out_used);
                member_out += out_used;
                produced_end = window_nbytes + out_used;

                switch (r)
                {
                    case LIBDEFLATE_BAD_DATA:
                        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Malformed {} stream", gzip ? "gzip" : "zlib");
                    case LIBDEFLATE_SUCCESS:
                        state = State::Trailer;
                        break;
                    case LIBDEFLATE_STREAM_NEED_INPUT:
                        if (out_used == 0)
                        {
                            if (input_eof)
                                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Unexpected end of {} stream", gzip ? "gzip" : "zlib");
                            /// Make forward progress: append more compressed input, or detect nested
                            /// EOF (sets input_eof so the retry passes end_of_input=true and the
                            /// decoder can finish the final block). Needed even when in_buf still has
                            /// unconsumed bytes (a partial final block that can't complete without
                            /// knowing the stream ended).
                            fillInput();
                        }
                        break;
                    case LIBDEFLATE_STREAM_NEED_OUTPUT:
                        if (out_used == 0)
                        {
                            /* A single block exceeds the output buffer: grow it (preserving the window) and retry. */
                            memory.resize(memory.size() + out_capacity);
                            continue;
                        }
                        break;
                    default:
                        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Unexpected libdeflate result {}", static_cast<int>(r));
                }

                if (out_used)
                {
                    working_buffer = Buffer(out, out + out_used);
                    return true;
                }
                /* No output yet (SUCCESS of an empty member, or NEED_INPUT with more to read): keep going. */
                continue;
            }

            case State::Trailer:
            {
                parseTrailer();
                if (in_pos == in_end && !input_eof)
                    fillInput();
                if (in_pos == in_end && input_eof)
                {
                    state = State::Eof;
                    return false;
                }
                state = State::Header; /* another concatenated member */
                produced_end = 0;
                continue;
            }
        }
    }
}

}

#endif
