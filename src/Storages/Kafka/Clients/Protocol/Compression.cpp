#include <Storages/Kafka/Clients/Protocol/Compression.h>

#include "config.h"

#include <Storages/Kafka/Clients/Error.h>

#include <base/scope_guard.h>

#include <lz4frame.h>
#include <zlib.h>
#include <zstd.h>

#if USE_SNAPPY
#    include <snappy.h>
#endif

#include <algorithm>
#include <cstring>

namespace Kafka::Protocol
{

namespace
{

/// Malformed batches must not OOM the server: a few compressed bytes can claim
/// gigabytes of output, so decompression refuses to produce more than this.
constexpr size_t max_decompressed_size = 1 << 30;

/// Grow the output buffer of a streaming decompression loop, enforcing the cap.
void growDecompressionBuffer(Bytes & out, CompressionType type)
{
    if (out.size() >= max_decompressed_size)
        throw Exception(
            ErrorKind::Protocol,
            "Decompressed size of a {} record batch exceeds the limit of {} bytes",
            compressionTypeName(type),
            max_decompressed_size);
    constexpr size_t initial_size = 64 * 1024;
    out.resize(std::min(std::max(out.size() * 2, initial_size), max_decompressed_size));
}

/// GZIP (zlib-ng in zlib-compat mode).

/// 15 = maximum window; +16 makes deflate emit the gzip wrapper instead of zlib.
constexpr int gzip_window_bits = 15 + 16;
/// +32 makes inflate auto-detect the gzip or zlib wrapper.
constexpr int gzip_auto_window_bits = 15 + 32;
constexpr int gzip_mem_level = 8;

/// zlib counts avail_in/avail_out in 32-bit uInt; feed large buffers in slices.
constexpr size_t max_zlib_slice = 1 << 30;

[[noreturn]] void throwZlibError(const char * action, int code, const z_stream & stream)
{
    throw Exception(
        ErrorKind::Protocol, "Gzip {} failed: {} (code {})", action, stream.msg != nullptr ? stream.msg : zError(code), code);
}

/// Plain wrappers: the zlib names are self-referential macros (zlib-ng name
/// mangling), and expanding them inside another macro (SCOPE_EXIT) trips
/// -Wdisabled-macro-expansion.
void endDeflate(z_stream & stream)
{
    deflateEnd(&stream);
}

void endInflate(z_stream & stream)
{
    inflateEnd(&stream);
}

Bytes gzipCompress(std::string_view data, int level)
{
    z_stream stream{};
    const int init_rc = deflateInit2(
        &stream, level < 0 ? Z_DEFAULT_COMPRESSION : level, Z_DEFLATED, gzip_window_bits, gzip_mem_level, Z_DEFAULT_STRATEGY);
    if (init_rc != Z_OK)
        throwZlibError("compression init", init_rc, stream);
    SCOPE_EXIT({ endDeflate(stream); });

    Bytes out;
    out.resize(deflateBound(&stream, data.size()));

    size_t in_pos = 0;
    size_t out_pos = 0;
    int rc = Z_OK;
    do
    {
        if (stream.avail_in == 0 && in_pos < data.size())
        {
            const size_t in_slice = std::min(data.size() - in_pos, max_zlib_slice);
            stream.next_in = reinterpret_cast<unsigned char *>(const_cast<char *>(data.data() + in_pos));
            stream.avail_in = static_cast<uInt>(in_slice);
            in_pos += in_slice;
        }

        /// deflateBound covers the single-pass case; slicing can need a bit more.
        if (out_pos == out.size())
            out.resize(out.size() + out.size() / 2 + 64);
        const size_t out_slice = std::min(out.size() - out_pos, max_zlib_slice);
        stream.next_out = reinterpret_cast<unsigned char *>(out.data() + out_pos);
        stream.avail_out = static_cast<uInt>(out_slice);

        rc = deflate(&stream, in_pos == data.size() ? Z_FINISH : Z_NO_FLUSH);
        if (rc != Z_OK && rc != Z_BUF_ERROR && rc != Z_STREAM_END)
            throwZlibError("compression", rc, stream);
        out_pos += out_slice - stream.avail_out;
    } while (rc != Z_STREAM_END);

    out.resize(out_pos);
    return out;
}

Bytes gzipDecompress(std::string_view data)
{
    z_stream stream{};
    const int init_rc = inflateInit2(&stream, gzip_auto_window_bits);
    if (init_rc != Z_OK)
        throwZlibError("decompression init", init_rc, stream);
    SCOPE_EXIT({ endInflate(stream); });

    Bytes out;
    size_t in_pos = 0;
    size_t out_pos = 0;
    int rc = Z_OK;
    do
    {
        if (stream.avail_in == 0 && in_pos < data.size())
        {
            const size_t in_slice = std::min(data.size() - in_pos, max_zlib_slice);
            stream.next_in = reinterpret_cast<unsigned char *>(const_cast<char *>(data.data() + in_pos));
            stream.avail_in = static_cast<uInt>(in_slice);
            in_pos += in_slice;
        }

        if (out_pos == out.size())
            growDecompressionBuffer(out, CompressionType::Gzip);
        const size_t out_slice = std::min(out.size() - out_pos, max_zlib_slice);
        stream.next_out = reinterpret_cast<unsigned char *>(out.data() + out_pos);
        stream.avail_out = static_cast<uInt>(out_slice);

        rc = inflate(&stream, Z_NO_FLUSH);
        /// Z_BUF_ERROR means no progress was possible; with output space always
        /// available that leaves one cause — the compressed stream ended early.
        if (rc == Z_BUF_ERROR && stream.avail_in == 0 && in_pos == data.size())
            throw Exception(ErrorKind::Protocol, "Gzip decompression failed: compressed data is truncated");
        if (rc != Z_OK && rc != Z_BUF_ERROR && rc != Z_STREAM_END)
            throwZlibError("decompression", rc, stream);
        out_pos += out_slice - stream.avail_out;
    } while (rc != Z_STREAM_END);

    out.resize(out_pos);
    return out;
}

/// LZ4 (the LZ4 Frame format; Kafka uses it for message format v2).

Bytes lz4Compress(std::string_view data, int level)
{
    LZ4F_preferences_t preferences{};
    preferences.compressionLevel = level < 0 ? 0 : level;

    Bytes out;
    out.resize(LZ4F_compressFrameBound(data.size(), &preferences));

    const size_t written = LZ4F_compressFrame(out.data(), out.size(), data.data(), data.size(), &preferences);
    if (LZ4F_isError(written))
        throw Exception(ErrorKind::Protocol, "Lz4 compression failed: {}", LZ4F_getErrorName(written));

    out.resize(written);
    return out;
}

Bytes lz4Decompress(std::string_view data)
{
    LZ4F_dctx * dctx = nullptr;
    const LZ4F_errorCode_t create_rc = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
    if (LZ4F_isError(create_rc))
        throw Exception(ErrorKind::Protocol, "Lz4 decompression failed: {}", LZ4F_getErrorName(create_rc));
    SCOPE_EXIT({ LZ4F_freeDecompressionContext(dctx); });

    Bytes out;
    size_t in_pos = 0;
    size_t out_pos = 0;
    /// Non-zero while the decoder is inside a frame and expects more bytes.
    size_t expected = 1;
    while (in_pos < data.size() || expected != 0)
    {
        if (out_pos == out.size())
            growDecompressionBuffer(out, CompressionType::Lz4);
        size_t dst_size = out.size() - out_pos;
        size_t src_size = data.size() - in_pos;

        expected = LZ4F_decompress(dctx, out.data() + out_pos, &dst_size, data.data() + in_pos, &src_size, nullptr);
        if (LZ4F_isError(expected))
            throw Exception(ErrorKind::Protocol, "Lz4 decompression failed: {}", LZ4F_getErrorName(expected));

        in_pos += src_size;
        out_pos += dst_size;
        if (expected != 0 && in_pos == data.size() && dst_size == 0)
            throw Exception(ErrorKind::Protocol, "Lz4 decompression failed: compressed data is truncated");
    }

    out.resize(out_pos);
    return out;
}

/// ZSTD.

Bytes zstdCompress(std::string_view data, int level)
{
    const size_t bound = ZSTD_compressBound(data.size());
    if (ZSTD_isError(bound))
        throw Exception(ErrorKind::Protocol, "Zstd compression failed: {}", ZSTD_getErrorName(bound));

    Bytes out;
    out.resize(bound);

    const size_t written
        = ZSTD_compress(out.data(), out.size(), data.data(), data.size(), level < 0 ? ZSTD_defaultCLevel() : level);
    if (ZSTD_isError(written))
        throw Exception(ErrorKind::Protocol, "Zstd compression failed: {}", ZSTD_getErrorName(written));

    out.resize(written);
    return out;
}

/// Streaming decompression: the frame content size can be unknown (streaming
/// producers), so a one-shot ZSTD_decompress is not an option.
Bytes zstdDecompress(std::string_view data)
{
    ZSTD_DStream * stream = ZSTD_createDStream();
    if (stream == nullptr)
        throw Exception(ErrorKind::Protocol, "Zstd decompression failed: cannot create a decompression stream");
    SCOPE_EXIT({ ZSTD_freeDStream(stream); });

    Bytes out;
    size_t out_pos = 0;
    ZSTD_inBuffer input{data.data(), data.size(), 0};
    /// Non-zero while the decoder is inside a frame and expects more bytes.
    size_t expected = 1;
    while (input.pos < input.size || expected != 0)
    {
        if (out_pos == out.size())
            growDecompressionBuffer(out, CompressionType::Zstd);
        ZSTD_outBuffer output{out.data(), out.size(), out_pos};

        expected = ZSTD_decompressStream(stream, &output, &input);
        if (ZSTD_isError(expected))
            throw Exception(ErrorKind::Protocol, "Zstd decompression failed: {}", ZSTD_getErrorName(expected));

        if (expected != 0 && input.pos == input.size && output.pos == out_pos)
            throw Exception(ErrorKind::Protocol, "Zstd decompression failed: compressed data is truncated");
        out_pos = output.pos;
    }

    out.resize(out_pos);
    return out;
}

/// SNAPPY (optional at build time).

#if USE_SNAPPY

/// Kafka's Java client wraps snappy in the stream framing of Java's
/// SnappyOutputStream (the "xerial" format): an 8-byte magic, two big-endian
/// Int32 version fields, then chunks of [big-endian Int32 compressed length]
/// [raw snappy block]. librdkafka instead sends one bare snappy block, so the
/// decoder accepts both forms.
constexpr char xerial_magic[8] = {'\x82', 'S', 'N', 'A', 'P', 'P', 'Y', '\0'};
constexpr size_t xerial_header_size = sizeof(xerial_magic) + 8;
constexpr UInt32 xerial_version = 1;
constexpr UInt32 xerial_compat_version = 1;

void appendBigEndian(Bytes & out, UInt32 value)
{
    out.push_back(static_cast<char>(value >> 24));
    out.push_back(static_cast<char>((value >> 16) & 0xFF));
    out.push_back(static_cast<char>((value >> 8) & 0xFF));
    out.push_back(static_cast<char>(value & 0xFF));
}

UInt32 readBigEndian(const char * data)
{
    return (static_cast<UInt32>(static_cast<UInt8>(data[0])) << 24) | (static_cast<UInt32>(static_cast<UInt8>(data[1])) << 16)
        | (static_cast<UInt32>(static_cast<UInt8>(data[2])) << 8) | static_cast<UInt32>(static_cast<UInt8>(data[3]));
}

/// Decode one raw snappy block onto the end of `out`, enforcing the cap.
void snappyUncompressBlock(std::string_view block, Bytes & out)
{
    size_t uncompressed_size = 0;
    if (!snappy::GetUncompressedLength(block.data(), block.size(), &uncompressed_size))
        throw Exception(ErrorKind::Protocol, "Snappy decompression failed: corrupt block header");
    if (uncompressed_size > max_decompressed_size - out.size())
        throw Exception(
            ErrorKind::Protocol,
            "Decompressed size of a snappy record batch exceeds the limit of {} bytes",
            max_decompressed_size);

    const size_t old_size = out.size();
    out.resize(old_size + uncompressed_size);
    if (!snappy::RawUncompress(block.data(), block.size(), out.data() + old_size))
        throw Exception(ErrorKind::Protocol, "Snappy decompression failed: corrupt block");
}

Bytes snappyCompress(std::string_view data)
{
    /// snappy::Compress rejects inputs of 2^32 bytes or more (and batches never
    /// get anywhere near that big — batchLength is an Int32).
    if (data.size() >= (1ULL << 32))
        throw Exception(ErrorKind::Protocol, "Snappy compression failed: input of {} bytes is too large", data.size());

    String block;
    snappy::Compress(data.data(), data.size(), &block);
    if (block.size() > static_cast<size_t>(INT32_MAX))
        throw Exception(ErrorKind::Protocol, "Snappy compression failed: compressed block of {} bytes is too large", block.size());

    Bytes out;
    out.reserve(xerial_header_size + 4 + block.size());
    out.append(xerial_magic, sizeof(xerial_magic));
    appendBigEndian(out, xerial_version);
    appendBigEndian(out, xerial_compat_version);
    appendBigEndian(out, static_cast<UInt32>(block.size()));
    out += block;
    return out;
}

Bytes snappyDecompress(std::string_view data)
{
    Bytes out;

    if (data.size() >= sizeof(xerial_magic) && memcmp(data.data(), xerial_magic, sizeof(xerial_magic)) == 0)
    {
        if (data.size() < xerial_header_size)
            throw Exception(ErrorKind::Protocol, "Snappy decompression failed: truncated xerial header");

        size_t pos = xerial_header_size;
        while (pos < data.size())
        {
            if (data.size() - pos < 4)
                throw Exception(ErrorKind::Protocol, "Snappy decompression failed: truncated xerial chunk length");
            const UInt32 chunk_size = readBigEndian(data.data() + pos);
            pos += 4;
            if (chunk_size > data.size() - pos)
                throw Exception(
                    ErrorKind::Protocol,
                    "Snappy decompression failed: xerial chunk of {} bytes overruns the payload",
                    chunk_size);
            snappyUncompressBlock(data.substr(pos, chunk_size), out);
            pos += chunk_size;
        }
        return out;
    }

    /// A bare snappy block (the form librdkafka produces).
    snappyUncompressBlock(data, out);
    return out;
}

#endif

}

bool compressionSupported(CompressionType type)
{
    switch (type)
    {
        case CompressionType::None:
        case CompressionType::Gzip:
        case CompressionType::Lz4:
        case CompressionType::Zstd:
            return true;
        case CompressionType::Snappy:
#if USE_SNAPPY
            return true;
#else
            return false;
#endif
    }
    return false;
}

Bytes compress(CompressionType type, std::string_view data, int level)
{
    switch (type)
    {
        case CompressionType::None:
            return Bytes(data);
        case CompressionType::Gzip:
            return gzipCompress(data, level);
        case CompressionType::Snappy:
#if USE_SNAPPY
            /// Snappy has no compression levels.
            return snappyCompress(data);
#else
            throw Exception(
                ErrorKind::NotSupported, "Cannot compress a record batch with snappy: ClickHouse was built without snappy support");
#endif
        case CompressionType::Lz4:
            return lz4Compress(data, level);
        case CompressionType::Zstd:
            return zstdCompress(data, level);
    }
    throw Exception(ErrorKind::Protocol, "Unknown compression type {}", static_cast<Int32>(type));
}

Bytes decompress(CompressionType type, std::string_view data)
{
    switch (type)
    {
        case CompressionType::None:
            return Bytes(data);
        case CompressionType::Gzip:
            return gzipDecompress(data);
        case CompressionType::Snappy:
#if USE_SNAPPY
            return snappyDecompress(data);
#else
            throw Exception(
                ErrorKind::NotSupported, "Cannot decompress a snappy record batch: ClickHouse was built without snappy support");
#endif
        case CompressionType::Lz4:
            return lz4Decompress(data);
        case CompressionType::Zstd:
            return zstdDecompress(data);
    }
    throw Exception(ErrorKind::Protocol, "Unknown compression type {}", static_cast<Int32>(type));
}

}
