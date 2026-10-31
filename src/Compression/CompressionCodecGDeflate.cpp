#include <Compression/CompressionCodecGDeflate.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/registerCompressionCodecs.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <base/unaligned.h>

#include <gdeflate.h>

#include <algorithm>
#include <cstring>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

namespace
{

constexpr size_t chunk_bytes = CompressionCodecGDeflate::chunk_bytes;
constexpr size_t chunk_alignment = 4;

size_t alignUp(size_t bytes)
{
    return (bytes + chunk_alignment - 1) / chunk_alignment * chunk_alignment;
}

size_t numChunks(size_t uncompressed_bytes)
{
    return (uncompressed_bytes + chunk_bytes - 1) / chunk_bytes;
}

/// The most a GDeflate page of `chunk_bytes` compresses to.
size_t maxPageBytes()
{
    return libdeflate_gdeflate_compress_bound(nullptr, chunk_bytes, nullptr);
}

struct CompressorDeleter
{
    void operator()(libdeflate_gdeflate_compressor * compressor) const { libdeflate_free_gdeflate_compressor(compressor); }
};

struct DecompressorDeleter
{
    void operator()(libdeflate_gdeflate_decompressor * decompressor) const
    {
        libdeflate_free_gdeflate_decompressor(decompressor);
    }
};

class Chunk
{
public:
    static constexpr size_t descriptor_bytes = sizeof(UInt32);

    static size_t maxCompressedBytes() { return alignUp(maxPageBytes()); }

    [[nodiscard]] size_t compressedBytes() const { return alignUp(compressed_size); }

    /// Compresses `size` bytes of `source` into `out`, of `maxCompressedBytes`.
    void compress(libdeflate_gdeflate_compressor * compressor, const char * source, size_t size, char * out)
    {
        libdeflate_gdeflate_out_page page{.data = out, .nbytes = maxPageBytes()};
        if (libdeflate_gdeflate_compress(compressor, source, size, &page, 1) == 0)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress a chunk of {} bytes with GDeflate", size);
        compressed_size = static_cast<UInt32>(page.nbytes);
        memset(out + compressed_size, 0, compressedBytes() - compressed_size);
    }

    void decompress(const char * in, size_t size, char * out) const
    {
        thread_local std::unique_ptr<libdeflate_gdeflate_decompressor, DecompressorDeleter> decompressor;
        if (!decompressor)
        {
            decompressor.reset(libdeflate_alloc_gdeflate_decompressor());
            if (!decompressor)
                throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot allocate a GDeflate decompressor");
        }

        libdeflate_gdeflate_in_page page{.data = in, .nbytes = compressed_size};
        size_t decompressed_size = 0;
        const auto result = libdeflate_gdeflate_decompress(decompressor.get(), &page, 1, out, size, &decompressed_size);
        if (result != LIBDEFLATE_SUCCESS)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress a GDeflate chunk: error code {}", static_cast<int>(result));
        if (decompressed_size != size)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS, "A GDeflate chunk expands to {} bytes, where {} were expected", decompressed_size, size);
    }

    void serialize(char * out) const { unalignedStoreLittleEndian<UInt32>(out, compressed_size); }
    void deserialize(const char * in) { compressed_size = unalignedLoadLittleEndian<UInt32>(in); }

    UInt32 compressed_size = 0;
};

class Header
{
public:
    Header() = default;
    explicit Header(size_t num_chunks_) : num_chunks(num_chunks_) {}

    static size_t maxCompressedBytes(size_t num_chunks) { return descriptorOffset(num_chunks); }

    [[nodiscard]] size_t compressedBytes() const { return descriptorOffset(num_chunks); }
    static size_t descriptorOffset(size_t chunk) { return sizeof(UInt32) + chunk * Chunk::descriptor_bytes; }

    void serialize(char * out) const { unalignedStoreLittleEndian<UInt32>(out, static_cast<UInt32>(num_chunks)); }
    void deserialize(const char * in) { num_chunks = unalignedLoadLittleEndian<UInt32>(in); }

    size_t num_chunks = 0;
};

}

CompressionCodecGDeflate::CompressionCodecGDeflate(int level_)
    : level(level_)
{
}

uint8_t CompressionCodecGDeflate::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::GDeflate);
}

ASTPtr CompressionCodecGDeflate::getCodecDescription() const
{
    return makeCodecDescription("GDeflate", {make_intrusive<ASTLiteral>(static_cast<UInt64>(level))});
}

void CompressionCodecGDeflate::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecGDeflate::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    const size_t num_chunks = numChunks(uncompressed_size);
    return static_cast<UInt32>(Header::maxCompressedBytes(num_chunks) + num_chunks * Chunk::maxCompressedBytes());
}

UInt32 CompressionCodecGDeflate::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    const Header header(numChunks(source_size));
    header.serialize(dest);

    std::unique_ptr<libdeflate_gdeflate_compressor, CompressorDeleter> compressor(
        libdeflate_alloc_gdeflate_compressor(level));
    if (!compressor)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot allocate a GDeflate compressor of level {}", level);

    size_t at = header.compressedBytes();
    for (size_t i = 0; i < header.num_chunks; ++i)
    {
        Chunk chunk;
        chunk.compress(compressor.get(), source + i * chunk_bytes, std::min(chunk_bytes, source_size - i * chunk_bytes), dest + at);
        chunk.serialize(dest + Header::descriptorOffset(i));
        at += chunk.compressedBytes();
    }
    return static_cast<UInt32>(at);
}

UInt32 CompressionCodecGDeflate::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < sizeof(UInt32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "A GDeflate block of {} bytes", source_size);

    Header header;
    header.deserialize(source);
    if (header.num_chunks != numChunks(uncompressed_size) || header.compressedBytes() > source_size)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "A GDeflate block of {} bytes holds {} chunks, where {} expanded bytes make {}",
            source_size,
            header.num_chunks,
            uncompressed_size,
            numChunks(uncompressed_size));

    size_t at = header.compressedBytes();
    for (size_t i = 0; i < header.num_chunks; ++i)
    {
        Chunk chunk;
        chunk.deserialize(source + Header::descriptorOffset(i));
        if (at + chunk.compressed_size > source_size)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "Chunk {} of a GDeflate block of {} bytes ends at {}",
                i,
                source_size,
                at + chunk.compressed_size);

        chunk.decompress(source + at, std::min(chunk_bytes, uncompressed_size - i * chunk_bytes), dest + i * chunk_bytes);
        at += chunk.compressedBytes();
    }
    return uncompressed_size;
}

void registerCodecGDeflate(CompressionCodecFactory & factory)
{
    const UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::GDeflate);
    factory.registerCompressionCodec("GDeflate", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        int level = CompressionCodecGDeflate::DEFAULT_LEVEL;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() != 1)
                throw Exception(
                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "GDeflate codec must have 1 parameter, given {}", arguments->children.size());

            const auto * literal = arguments->children[0]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::UInt64)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "GDeflate codec argument must be a positive integer");

            const UInt64 value = literal->value.safeGet<UInt64>();
            if (value < static_cast<UInt64>(CompressionCodecGDeflate::MIN_LEVEL) || value > static_cast<UInt64>(CompressionCodecGDeflate::MAX_LEVEL))
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "GDeflate codec level must be between {} and {}, given {}",
                    CompressionCodecGDeflate::MIN_LEVEL,
                    CompressionCodecGDeflate::MAX_LEVEL,
                    value);
            level = static_cast<int>(value);
        }

        return std::make_shared<CompressionCodecGDeflate>(level);
    });
}

}
