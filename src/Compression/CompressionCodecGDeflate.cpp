#include <Compression/CompressionCodecGDeflate.h>

#if USE_GPU

#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/registerCompressionCodecs.h>
#include <GPU/GPUGDeflate.h>
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

/// The block format is that of `GPU::forEachGDeflateChunk`, which the host expands here with the GDeflate pages of
/// NVIDIA's fork of libdeflate: a device is no faster for a block at a time, and is not there for every read.
constexpr size_t chunk_bytes = CompressionCodecGDeflate::chunk_bytes;
static_assert(chunk_bytes == GPU::gdeflate_chunk_bytes);
constexpr size_t chunk_alignment = 4;

size_t alignUp(size_t bytes)
{
    return (bytes + chunk_alignment - 1) / chunk_alignment * chunk_alignment;
}

size_t numChunks(size_t uncompressed_bytes)
{
    return (uncompressed_bytes + chunk_bytes - 1) / chunk_bytes;
}

size_t headerBytes(size_t num_chunks)
{
    return sizeof(UInt32) + num_chunks * sizeof(UInt32);
}

/// The most a GDeflate page of `chunk_bytes` compresses to, the room each chunk has in a block.
size_t maxCompressedChunkBytes()
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

void decompressChunk(const char * compressed, size_t compressed_size, char * out, size_t out_size)
{
    thread_local std::unique_ptr<libdeflate_gdeflate_decompressor, DecompressorDeleter> decompressor;
    if (!decompressor)
    {
        decompressor.reset(libdeflate_alloc_gdeflate_decompressor());
        if (!decompressor)
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot allocate a GDeflate decompressor");
    }

    libdeflate_gdeflate_in_page page{.data = compressed, .nbytes = compressed_size};
    size_t decompressed_size = 0;
    const auto result = libdeflate_gdeflate_decompress(decompressor.get(), &page, 1, out, out_size, &decompressed_size);
    if (result != LIBDEFLATE_SUCCESS)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress a GDeflate chunk: error code {}", static_cast<int>(result));
    if (decompressed_size != out_size)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS, "A GDeflate chunk expands to {} bytes, where {} were expected", decompressed_size, out_size);
}

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
    return static_cast<UInt32>(headerBytes(num_chunks) + num_chunks * alignUp(maxCompressedChunkBytes()));
}

UInt32 CompressionCodecGDeflate::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    const size_t num_chunks = numChunks(source_size);
    unalignedStoreLittleEndian<UInt32>(dest, static_cast<UInt32>(num_chunks));

    std::unique_ptr<libdeflate_gdeflate_compressor, CompressorDeleter> compressor(libdeflate_alloc_gdeflate_compressor(level));
    if (!compressor)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot allocate a GDeflate compressor of level {}", level);

    size_t at = headerBytes(num_chunks);
    for (size_t i = 0; i < num_chunks; ++i)
    {
        const size_t chunk_size = std::min(chunk_bytes, source_size - i * chunk_bytes);
        libdeflate_gdeflate_out_page page{.data = dest + at, .nbytes = maxCompressedChunkBytes()};
        if (libdeflate_gdeflate_compress(compressor.get(), source + i * chunk_bytes, chunk_size, &page, 1) == 0)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress a chunk of {} bytes with GDeflate", chunk_size);

        unalignedStoreLittleEndian<UInt32>(dest + sizeof(UInt32) * (1 + i), static_cast<UInt32>(page.nbytes));
        const size_t next = alignUp(at + page.nbytes);
        memset(dest + at + page.nbytes, 0, next - at - page.nbytes);
        at = next;
    }
    return static_cast<UInt32>(at);
}

UInt32 CompressionCodecGDeflate::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    GPU::forEachGDeflateChunk({source, source_size}, uncompressed_size, [&](const GPU::GDeflateChunk & chunk)
    {
        decompressChunk(source + chunk.offset, chunk.compressed_bytes, dest + chunk.decompressed_offset, chunk.decompressed_bytes);
    });
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

#endif
