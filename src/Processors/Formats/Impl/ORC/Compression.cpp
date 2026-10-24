#include <Processors/Formats/Impl/ORC/Compression.h>

#include <Common/Exception.h>

#include <lz4.h>
#include <snappy.h>
#include <zlib.h>
#include <zstd.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_COMPRESS;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::ORC
{

namespace
{

[[noreturn]] void throwMalformedLzo(size_t offset)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed LZO-compressed data in ORC file at offset {}", offset);
}

/// LZO1X decompressor, as written by the Hadoop codecs (and the ORC Java writer). A chunk can be a
/// concatenation of several LZO blocks, each terminated with the end-of-stream marker.
size_t decompressLzo(const char * src, size_t src_size, char * dst, size_t dst_capacity)
{
    const UInt8 * input = reinterpret_cast<const UInt8 *>(src);
    const UInt8 * const input_begin = input;
    const UInt8 * const input_end = input + src_size;
    char * output = dst;
    char * const output_end = dst + dst_capacity;

    auto offset = [&] { return static_cast<size_t>(input - input_begin); };

    auto read_byte = [&]() -> UInt32
    {
        if (input >= input_end)
            throwMalformedLzo(offset());
        return *input++;
    };

    /// A run of zero bytes extends a length by 255 each, then the next byte is added.
    auto read_variable_length = [&](UInt32 base) -> size_t
    {
        size_t length = base;
        UInt32 next = 0;
        while ((next = read_byte()) == 0)
        {
            length += 0xFF;
            if (length > dst_capacity)
                throwMalformedLzo(offset());
        }
        return length + next;
    };

    while (input < input_end)
    {
        bool first_command = true;
        size_t last_literal_length = 0;

        while (true)
        {
            UInt32 command = read_byte();
            if (command == 0x11)
                break;

            size_t match_length = 0;
            size_t match_offset = 0;
            size_t literal_length = 0;

            if ((command & 0xF0) == 0)
            {
                if (last_literal_length == 0)
                {
                    /// A long run of literals.
                    literal_length = command & 0xF;
                    if (literal_length == 0)
                        literal_length = read_variable_length(0xF);
                    literal_length += 3;
                }
                else if (last_literal_length <= 3)
                {
                    match_length = 3;
                    match_offset = (command & 0xC) >> 2;
                    match_offset |= read_byte() << 2;
                    match_offset |= 0x800;
                    literal_length = command & 0x3;
                }
                else
                {
                    match_length = 2;
                    match_offset = (command & 0xC) >> 2;
                    match_offset |= read_byte() << 2;
                    literal_length = command & 0x3;
                }
            }
            else if (first_command)
            {
                literal_length = command - 17;
            }
            else if ((command & 0xF0) == 0x10)
            {
                match_length = command & 0x7;
                if (match_length == 0)
                    match_length = read_variable_length(0x7);
                match_length += 2;
                UInt32 trailer = read_byte();
                trailer |= read_byte() << 8;
                match_offset = trailer >> 2;
                match_offset |= (command & 0x8) ? 0x8000 : 0x4000;
                --match_offset;
                literal_length = trailer & 0x3;
            }
            else if ((command & 0xE0) == 0x20)
            {
                match_length = command & 0x1F;
                if (match_length == 0)
                    match_length = read_variable_length(0x1F);
                match_length += 2;
                UInt32 trailer = read_byte();
                trailer |= read_byte() << 8;
                match_offset = trailer >> 2;
                literal_length = trailer & 0x3;
            }
            else if ((command & 0xC0) != 0)
            {
                match_length = ((command & 0xE0) >> 5) + 1;
                match_offset = (command & 0x1C) >> 2;
                match_offset |= read_byte() << 3;
                literal_length = command & 0x3;
            }
            else
                throwMalformedLzo(offset());

            first_command = false;

            if (match_length != 0)
            {
                /// LZO encodes the match offset minus one.
                ++match_offset;
                if (match_offset > static_cast<size_t>(output - dst) || match_length > static_cast<size_t>(output_end - output))
                    throwMalformedLzo(offset());
                const char * match = output - match_offset;
                /// The source can overlap the destination, so copy byte by byte.
                for (size_t i = 0; i < match_length; ++i)
                    output[i] = match[i];
                output += match_length;
            }

            if (literal_length > static_cast<size_t>(output_end - output) || literal_length > static_cast<size_t>(input_end - input))
                throwMalformedLzo(offset());
            memcpy(output, input, literal_length);
            output += literal_length;
            input += literal_length;
            last_literal_length = literal_length;
        }

        /// The end-of-stream marker is 0x11 0x00 0x00.
        if (input_end - input < 2 || input[0] != 0 || input[1] != 0)
            throwMalformedLzo(offset());
        input += 2;
    }

    return output - dst;
}

size_t decompressZlib(const char * src, size_t src_size, char * dst, size_t dst_capacity)
{
    z_stream stream{};
    if (inflateInit2(&stream, -15) != Z_OK)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot initialize zlib decompression for ORC file");

    stream.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(src));
    stream.avail_in = static_cast<uInt>(src_size);
    stream.next_out = reinterpret_cast<Bytef *>(dst);
    stream.avail_out = static_cast<uInt>(dst_capacity);

    int ret = inflate(&stream, Z_FINISH);
    size_t result = dst_capacity - stream.avail_out;
    inflateEnd(&stream);

    if (ret != Z_STREAM_END)
    {
        if (ret == Z_BUF_ERROR && stream.avail_out == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "A zlib-compressed chunk of ORC file decompresses to more than the compression block size {}", dst_capacity);
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed zlib-compressed data in ORC file (zlib error {})", ret);
    }
    return result;
}

size_t decompressSnappy(const char * src, size_t src_size, char * dst, size_t dst_capacity)
{
    size_t uncompressed_size = 0;
    if (!snappy::GetUncompressedLength(src, src_size, &uncompressed_size))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed snappy-compressed data in ORC file");
    if (uncompressed_size > dst_capacity)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "A snappy-compressed chunk of ORC file decompresses to {} bytes, more than the compression block size {}",
            uncompressed_size, dst_capacity);
    if (!snappy::RawUncompress(src, src_size, dst))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed snappy-compressed data in ORC file");
    return uncompressed_size;
}

size_t decompressLz4(const char * src, size_t src_size, char * dst, size_t dst_capacity)
{
    if (src_size > static_cast<size_t>(std::numeric_limits<int>::max()) || dst_capacity > static_cast<size_t>(std::numeric_limits<int>::max()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "LZ4-compressed chunk of ORC file is too large");
    int res = LZ4_decompress_safe(src, dst, static_cast<int>(src_size), static_cast<int>(dst_capacity));
    if (res < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Malformed LZ4-compressed data in ORC file, or it decompresses to more than the compression block size {}", dst_capacity);
    return static_cast<size_t>(res);
}

size_t decompressZstd(const char * src, size_t src_size, char * dst, size_t dst_capacity)
{
    size_t res = ZSTD_decompress(dst, dst_capacity, src, src_size);
    if (ZSTD_isError(res))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Malformed zstd-compressed data in ORC file, or it decompresses to more than the compression block size {}: {}",
            dst_capacity, ZSTD_getErrorName(res));
    return res;
}


class ZlibCompressor final : public Compressor
{
public:
    ZlibCompressor()
    {
        /// The level and the parameters are the same as in the C++ ORC library with its default
        /// compression strategy, "speed".
        if (deflateInit2(&stream, Z_BEST_SPEED + 1, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY) != Z_OK)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot initialize zlib compression for ORC");
    }

    ~ZlibCompressor() override { deflateEnd(&stream); }

    size_t compress(const char * src, size_t src_size, std::string & out) override
    {
        if (deflateReset(&stream) != Z_OK)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot reset zlib compression for ORC");

        size_t old_size = out.size();
        /// Allow only the output smaller than the input.
        out.resize(old_size + src_size);
        stream.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(src));
        stream.avail_in = static_cast<uInt>(src_size);
        stream.next_out = reinterpret_cast<Bytef *>(out.data() + old_size);
        stream.avail_out = static_cast<uInt>(src_size);

        int ret = deflate(&stream, Z_FINISH);
        if (ret == Z_STREAM_END && stream.avail_out > 0)
        {
            size_t compressed = src_size - stream.avail_out;
            out.resize(old_size + compressed);
            return compressed;
        }
        out.resize(old_size);
        if (ret != Z_STREAM_END && ret != Z_OK && ret != Z_BUF_ERROR)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "zlib compression failed with error {}", ret);
        return 0;
    }

private:
    z_stream stream{};
};

class SnappyCompressor final : public Compressor
{
public:
    size_t compress(const char * src, size_t src_size, std::string & out) override
    {
        size_t old_size = out.size();
        out.resize(old_size + snappy::MaxCompressedLength(src_size));
        size_t compressed = 0;
        snappy::RawCompress(src, src_size, out.data() + old_size, &compressed);
        return finish(out, old_size, compressed, src_size);
    }

    static size_t finish(std::string & out, size_t old_size, size_t compressed, size_t src_size)
    {
        if (compressed >= src_size)
        {
            out.resize(old_size);
            return 0;
        }
        out.resize(old_size + compressed);
        return compressed;
    }
};

class Lz4Compressor final : public Compressor
{
public:
    size_t compress(const char * src, size_t src_size, std::string & out) override
    {
        size_t old_size = out.size();
        int bound = LZ4_compressBound(static_cast<int>(src_size));
        out.resize(old_size + bound);
        int compressed = LZ4_compress_default(src, out.data() + old_size, static_cast<int>(src_size), bound);
        if (compressed <= 0)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "LZ4 compression failed");
        return SnappyCompressor::finish(out, old_size, compressed, src_size);
    }
};

class ZstdCompressor final : public Compressor
{
public:
    ZstdCompressor() : context(ZSTD_createCCtx())
    {
        if (!context)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot create zstd compression context");
    }

    ~ZstdCompressor() override { ZSTD_freeCCtx(context); }

    size_t compress(const char * src, size_t src_size, std::string & out) override
    {
        size_t old_size = out.size();
        size_t bound = ZSTD_compressBound(src_size);
        out.resize(old_size + bound);
        /// Level 1, as in the C++ ORC library with its default compression strategy, "speed".
        size_t compressed = ZSTD_compressCCtx(context, out.data() + old_size, bound, src, src_size, 1);
        if (ZSTD_isError(compressed))
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "zstd compression failed: {}", ZSTD_getErrorName(compressed));
        return SnappyCompressor::finish(out, old_size, compressed, src_size);
    }

private:
    ZSTD_CCtx * context;
};

}


size_t decompressChunk(CompressionKind kind, const char * src, size_t src_size, char * dst, size_t dst_capacity)
{
    switch (kind)
    {
        case CompressionKind::ZLIB: return decompressZlib(src, src_size, dst, dst_capacity);
        case CompressionKind::SNAPPY: return decompressSnappy(src, src_size, dst, dst_capacity);
        case CompressionKind::LZO: return decompressLzo(src, src_size, dst, dst_capacity);
        case CompressionKind::LZ4: return decompressLz4(src, src_size, dst, dst_capacity);
        case CompressionKind::ZSTD: return decompressZstd(src, src_size, dst, dst_capacity);
        case CompressionKind::NONE:
        case CompressionKind::BROTLI:
            break;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ORC compression {} is not supported", compressionKindToString(kind));
}

std::unique_ptr<Compressor> createCompressor(CompressionKind kind)
{
    switch (kind)
    {
        case CompressionKind::NONE: return nullptr;
        case CompressionKind::ZLIB: return std::make_unique<ZlibCompressor>();
        case CompressionKind::SNAPPY: return std::make_unique<SnappyCompressor>();
        case CompressionKind::LZ4: return std::make_unique<Lz4Compressor>();
        case CompressionKind::ZSTD: return std::make_unique<ZstdCompressor>();
        case CompressionKind::LZO:
        case CompressionKind::BROTLI:
            break;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Writing ORC with compression {} is not supported", compressionKindToString(kind));
}

String compressionKindToString(CompressionKind kind)
{
    switch (kind)
    {
        case CompressionKind::NONE: return "NONE";
        case CompressionKind::ZLIB: return "ZLIB";
        case CompressionKind::SNAPPY: return "SNAPPY";
        case CompressionKind::LZO: return "LZO";
        case CompressionKind::LZ4: return "LZ4";
        case CompressionKind::ZSTD: return "ZSTD";
        case CompressionKind::BROTLI: return "BROTLI";
    }
    return "UNKNOWN(" + std::to_string(static_cast<int>(kind)) + ")";
}

}
