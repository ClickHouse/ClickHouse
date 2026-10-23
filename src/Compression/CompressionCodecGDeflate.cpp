#include <Compression/CompressionCodecGDeflate.h>

#if USE_GPU

#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/registerCompressionCodecs.h>
#include <DataTypes/IDataType.h>
#include <GPU/GPUGDeflate.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Common/PODArray.h>
#include <base/unaligned.h>

#include <gdeflate.h>

#include <algorithm>
#include <cstring>
#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_DECOMPRESS;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

namespace
{

/// The block format is that of `GPU::forEachGDeflateChunk`, which the host expands here with the GDeflate pages of
/// NVIDIA's fork of libdeflate: a device is no faster for a block at a time, and is not there for every read.
constexpr size_t chunk_bytes = GPU::gdeflate_chunk_bytes;
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

bool isPackedElementSize(size_t size)
{
    return size == 1 || size == 2 || size == 4 || size == 8;
}

void storeInteger(char * at, UInt64 value, UInt32 element_size)
{
    switch (element_size)
    {
        case 1: unalignedStoreLittleEndian<UInt8>(at, static_cast<UInt8>(value)); break;
        case 2: unalignedStoreLittleEndian<UInt16>(at, static_cast<UInt16>(value)); break;
        case 4: unalignedStoreLittleEndian<UInt32>(at, static_cast<UInt32>(value)); break;
        default: unalignedStoreLittleEndian<UInt64>(at, value); break;
    }
}

size_t packedBytes(size_t count, UInt32 bits)
{
    return (count * bits + 7) / 8;
}

/// Writes the `count` integers of `element_size` bytes packed in `bits` bits at `packed`, readable for `packed_padding`
/// bytes past them.
void unpack(const char * packed, size_t count, UInt32 bits, UInt32 element_size, UInt64 reference, char * out)
{
    const UInt64 mask = (UInt64{1} << bits) - 1;
    for (size_t i = 0; i < count; ++i)
    {
        const size_t first_bit = i * bits;
        const UInt64 delta = (unalignedLoadLittleEndian<UInt64>(packed + first_bit / 8) >> (first_bit % 8)) & mask;
        storeInteger(out + i * element_size, reference + delta, element_size);
    }
}

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

CompressionCodecGDeflate::CompressionCodecGDeflate(int algorithm_, UInt8 element_size_, bool is_signed_)
    : algorithm(algorithm_)
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
    return makeCodecDescription("GDeflate", {make_intrusive<ASTLiteral>(static_cast<UInt64>(algorithm))});
}

void CompressionCodecGDeflate::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecGDeflate::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return static_cast<UInt32>(GPU::gdeflateMaxCompressedBytes(uncompressed_size, algorithm));
}

UInt32 CompressionCodecGDeflate::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    return static_cast<UInt32>(GPU::gdeflateCompress(source, source_size, dest, algorithm, element_size, is_signed));
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
    if (size == 1 || size == 2 || size == 4 || size == 8)
        return {static_cast<UInt8>(size), !column_type->isValueRepresentedByUnsignedInteger()};
    return {0, false};
}

}

void registerCodecGDeflate(CompressionCodecFactory & factory)
{
    const UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::GDeflate);
    factory.registerCompressionCodecWithType("GDeflate", method_code, [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        int algorithm = CompressionCodecGDeflate::DEFAULT_ALGORITHM;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() != 1)
                throw Exception(
                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "GDeflate codec must have 1 parameter, given {}", arguments->children.size());

            const auto * literal = arguments->children[0]->as<ASTLiteral>();
            if (!literal)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "GDeflate codec argument must be integer");

            const UInt64 value = literal->value.safeGet<UInt64>();
            if (value > CompressionCodecGDeflate::MAX_ALGORITHM)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "GDeflate codec algorithm must be between 0 and {}, given {}",
                    CompressionCodecGDeflate::MAX_ALGORITHM,
                    value);
            algorithm = static_cast<int>(value);
        }
        const auto [element_size, is_signed] = getPackedTypeInfo(column_type);
        return std::make_shared<CompressionCodecGDeflate>(algorithm, element_size, is_signed);
    });
}

}

#endif
