#include <Compression/CompressionCodecGDeflate.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/registerCompressionCodecs.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Common/PODArray.h>
#include <base/unaligned.h>

#include <gdeflate.h>

#include <algorithm>
#include <bit>
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

/// Set in the number of chunks of a block whose chunks have descriptors of `packed_descriptor_bytes`.
constexpr UInt32 packed_flag = UInt32{1} << 31;
constexpr size_t plain_descriptor_bytes = sizeof(UInt32);
constexpr size_t packed_descriptor_bytes = 16;

/// The most bits an integer is packed in, which a read of 8 bytes holds at any bit offset.
constexpr UInt32 max_packed_bits = 56;

/// What `unpack` may read past the packed bits.
constexpr size_t packed_padding = sizeof(UInt64);

size_t alignUp(size_t bytes)
{
    return (bytes + chunk_alignment - 1) / chunk_alignment * chunk_alignment;
}

size_t numChunks(size_t uncompressed_bytes)
{
    return (uncompressed_bytes + chunk_bytes - 1) / chunk_bytes;
}

size_t headerBytes(size_t num_chunks, bool packed)
{
    return sizeof(UInt32) + num_chunks * (packed ? packed_descriptor_bytes : plain_descriptor_bytes);
}

/// The most a GDeflate page of `chunk_bytes` compresses to, the room each chunk has in a block.
size_t maxCompressedChunkBytes()
{
    return libdeflate_gdeflate_compress_bound(nullptr, chunk_bytes, nullptr);
}

bool isPackedElementSize(size_t size)
{
    return size == 1 || size == 2 || size == 4 || size == 8;
}

size_t packedBytes(size_t count, UInt32 bits)
{
    return (count * bits + 7) / 8;
}

struct PackedChunk
{
    UInt32 bits = 0;
    UInt64 reference = 0;
};

template <typename U>
PackedChunk packIntegers(const char * chunk, size_t chunk_size, bool is_signed, char * out)
{
    const U sign_bit = is_signed ? static_cast<U>(U{1} << (8 * sizeof(U) - 1)) : U{0};
    const size_t count = chunk_size / sizeof(U);
    U least = unalignedLoadLittleEndian<U>(chunk) ^ sign_bit;
    U most = least;
    for (size_t i = 1; i < count; ++i)
    {
        const U value = unalignedLoadLittleEndian<U>(chunk + i * sizeof(U)) ^ sign_bit;
        least = std::min(least, value);
        most = std::max(most, value);
    }

    const auto bits = std::max<UInt32>(1, static_cast<UInt32>(std::bit_width(static_cast<UInt64>(static_cast<U>(most - least)))));
    if (bits > max_packed_bits || bits >= 8 * sizeof(U))
        return {};

    /// Each integer is ORed into the bytes from the lowest bit of the byte its first bit is in, 8 bytes at a time, which
    /// hold any 56 bits from any bit offset.
    memset(out, 0, packedBytes(count, bits) + packed_padding);
    for (size_t i = 0; i < count; ++i)
    {
        const U value = unalignedLoadLittleEndian<U>(chunk + i * sizeof(U)) ^ sign_bit;
        const UInt64 delta = static_cast<U>(value - least);
        const size_t first_bit = i * bits;
        char * const at = out + first_bit / 8;
        unalignedStoreLittleEndian<UInt64>(at, unalignedLoadLittleEndian<UInt64>(at) | (delta << (first_bit % 8)));
    }
    return {.bits = bits, .reference = static_cast<U>(least ^ sign_bit)};
}

/// Packs the chunk into `out`, of `packedBytes` and `packed_padding` more, if its integers take fewer bits than they are
/// wide; otherwise returns a chunk of 0 bits.
PackedChunk pack(const char * chunk, size_t chunk_size, UInt8 element_size, bool is_signed, char * out)
{
    switch (element_size)
    {
        case 1: return packIntegers<UInt8>(chunk, chunk_size, is_signed, out);
        case 2: return packIntegers<UInt16>(chunk, chunk_size, is_signed, out);
        case 4: return packIntegers<UInt32>(chunk, chunk_size, is_signed, out);
        default: return packIntegers<UInt64>(chunk, chunk_size, is_signed, out);
    }
}

template <typename U>
void unpackIntegers(const char * packed, size_t count, UInt32 bits, UInt64 reference, char * out)
{
    const UInt64 mask = (UInt64{1} << bits) - 1;
    for (size_t i = 0; i < count; ++i)
    {
        const size_t first_bit = i * bits;
        const UInt64 delta = (unalignedLoadLittleEndian<UInt64>(packed + first_bit / 8) >> (first_bit % 8)) & mask;
        unalignedStoreLittleEndian<U>(out + i * sizeof(U), static_cast<U>(reference + delta));
    }
}

/// Writes the `count` integers of `element_size` bytes packed in `bits` bits at `packed`, readable for `packed_padding`
/// bytes past them.
void unpack(const char * packed, size_t count, UInt32 bits, UInt32 element_size, UInt64 reference, char * out)
{
    switch (element_size)
    {
        case 1: unpackIntegers<UInt8>(packed, count, bits, reference, out); break;
        case 2: unpackIntegers<UInt16>(packed, count, bits, reference, out); break;
        case 4: unpackIntegers<UInt32>(packed, count, bits, reference, out); break;
        default: unpackIntegers<UInt64>(packed, count, bits, reference, out); break;
    }
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

/// Compresses one chunk into `out`, of `maxCompressedChunkBytes`, and returns its compressed size.
size_t compressChunk(libdeflate_gdeflate_compressor * compressor, const char * chunk, size_t chunk_size, char * out)
{
    libdeflate_gdeflate_out_page page{.data = out, .nbytes = maxCompressedChunkBytes()};
    if (libdeflate_gdeflate_compress(compressor, chunk, chunk_size, &page, 1) == 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress a chunk of {} bytes with GDeflate", chunk_size);
    return page.nbytes;
}

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

CompressionCodecGDeflate::CompressionCodecGDeflate(int level_, UInt8 element_size_, bool is_signed_)
    : level(level_)
    , element_size(element_size_)
    , is_signed(is_signed_)
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
    return static_cast<UInt32>(headerBytes(num_chunks, true) + num_chunks * alignUp(maxCompressedChunkBytes()));
}

UInt32 CompressionCodecGDeflate::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    const size_t num_chunks = numChunks(source_size);
    /// A block that ends in the middle of an integer is not packed: its chunks do not hold whole integers.
    const bool packing = isPackedElementSize(element_size) && source_size % element_size == 0;
    unalignedStoreLittleEndian<UInt32>(dest, static_cast<UInt32>(num_chunks) | (packing ? packed_flag : 0));

    std::unique_ptr<libdeflate_gdeflate_compressor, CompressorDeleter> compressor(
        libdeflate_alloc_gdeflate_compressor(level));
    if (!compressor)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot allocate a GDeflate compressor of level {}", level);

    /// The packed form of a chunk and what it compresses to, kept where it is smaller than the plain one.
    PODArray<char> packed;
    PODArray<char> compressed_packed;
    if (packing)
    {
        packed.resize(chunk_bytes + packed_padding);
        compressed_packed.resize(maxCompressedChunkBytes());
    }

    size_t at = headerBytes(num_chunks, packing);
    for (size_t i = 0; i < num_chunks; ++i)
    {
        const char * const chunk = source + i * chunk_bytes;
        const size_t chunk_size = std::min(chunk_bytes, source_size - i * chunk_bytes);
        size_t compressed_size = compressChunk(compressor.get(), chunk, chunk_size, dest + at);

        PackedChunk packed_chunk;
        if (packing)
        {
            packed_chunk = pack(chunk, chunk_size, element_size, is_signed, packed.data());
            if (packed_chunk.bits)
            {
                const size_t packed_size = compressChunk(
                    compressor.get(), packed.data(), packedBytes(chunk_size / element_size, packed_chunk.bits), compressed_packed.data());
                if (packed_size < compressed_size)
                {
                    memcpy(dest + at, compressed_packed.data(), packed_size);
                    compressed_size = packed_size;
                }
                else
                    packed_chunk = {};
            }
        }

        char * const descriptor = dest + headerBytes(i, packing);
        unalignedStoreLittleEndian<UInt32>(descriptor, static_cast<UInt32>(compressed_size));
        if (packing)
        {
            unalignedStoreLittleEndian<UInt8>(descriptor + 4, static_cast<UInt8>(packed_chunk.bits));
            unalignedStoreLittleEndian<UInt8>(descriptor + 5, element_size);
            unalignedStoreLittleEndian<UInt16>(descriptor + 6, 0);
            unalignedStoreLittleEndian<UInt64>(descriptor + 8, packed_chunk.reference);
        }

        const size_t next = alignUp(at + compressed_size);
        memset(dest + at + compressed_size, 0, next - at - compressed_size);
        at = next;
    }
    return static_cast<UInt32>(at);
}

UInt32 CompressionCodecGDeflate::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < sizeof(UInt32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "A GDeflate block of {} bytes", source_size);

    const UInt32 header = unalignedLoadLittleEndian<UInt32>(source);
    const bool packed = header & packed_flag;
    const size_t num_chunks = header & ~packed_flag;
    if (num_chunks != numChunks(uncompressed_size) || headerBytes(num_chunks, packed) > source_size)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "A GDeflate block of {} bytes holds {} chunks, where {} expanded bytes make {}",
            source_size,
            num_chunks,
            uncompressed_size,
            numChunks(uncompressed_size));

    PODArray<char> unpacked;
    size_t at = headerBytes(num_chunks, packed);
    for (size_t i = 0; i < num_chunks; ++i)
    {
        const char * const descriptor = source + headerBytes(i, packed);
        const size_t compressed_size = unalignedLoadLittleEndian<UInt32>(descriptor);
        const size_t chunk_size = std::min(chunk_bytes, uncompressed_size - i * chunk_bytes);
        if (at + compressed_size > source_size)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS, "Chunk {} of a GDeflate block of {} bytes ends at {}", i, source_size, at + compressed_size);

        const UInt32 bits = packed ? unalignedLoadLittleEndian<UInt8>(descriptor + 4) : 0;
        char * const out = dest + i * chunk_bytes;
        if (bits == 0)
        {
            decompressChunk(source + at, compressed_size, out, chunk_size);
        }
        else
        {
            const UInt32 chunk_element_size = unalignedLoadLittleEndian<UInt8>(descriptor + 5);
            if (bits > max_packed_bits || !isPackedElementSize(chunk_element_size) || chunk_size % chunk_element_size != 0)
                throw Exception(
                    ErrorCodes::CANNOT_DECOMPRESS,
                    "Chunk {} of a GDeflate block packs integers of {} bytes in {} bits",
                    i,
                    chunk_element_size,
                    bits);

            const size_t count = chunk_size / chunk_element_size;
            const size_t packed_size = packedBytes(count, bits);
            unpacked.resize(packed_size + packed_padding);
            memset(unpacked.data() + packed_size, 0, packed_padding);
            decompressChunk(source + at, compressed_size, unpacked.data(), packed_size);
            unpack(unpacked.data(), count, bits, chunk_element_size, unalignedLoadLittleEndian<UInt64>(descriptor + 8), out);
        }
        at = alignUp(at + compressed_size);
    }
    return uncompressed_size;
}

namespace
{

/// The integers whose chunks the codec packs: their size and whether they are signed, or 0 for a column of anything else.
std::pair<UInt8, bool> getPackedTypeInfo(const IDataType * column_type)
{
    if (!column_type)
        return {0, false};

    WhichDataType which(column_type);
    if (!(which.isInt() || which.isUInt() || which.isDecimal() || which.isDateOrDate32() || which.isDateTime() || which.isDateTime64()))
        return {0, false};

    const size_t size = column_type->getSizeOfValueInMemory();
    if (isPackedElementSize(size))
        return {static_cast<UInt8>(size), !column_type->isValueRepresentedByUnsignedInteger()};
    return {0, false};
}

}

void registerCodecGDeflate(CompressionCodecFactory & factory)
{
    const UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::GDeflate);
    factory.registerCompressionCodecWithType("GDeflate", method_code, [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
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

        const auto [element_size, is_signed] = getPackedTypeInfo(column_type);
        return std::make_shared<CompressionCodecGDeflate>(level, element_size, is_signed);
    });
}

}
