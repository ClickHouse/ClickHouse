#include <Compression/CompressionCodecBitPacking.h>
#include <Compression/BitPackingFormat.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/registerCompressionCodecs.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <base/unaligned.h>

#include <algorithm>
#include <bit>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
}

namespace
{

using namespace BitPackingFormat;

bool isElementSize(size_t size)
{
    return size == 1 || size == 2 || size == 4 || size == 8;
}

/// An integer of `element_size` bytes, widened to 64 bits as what it is, signed or not, so that the integers of a chunk
/// order as their type does.
UInt64 integerAt(const char * at, UInt8 element_size, bool is_signed)
{
    switch (element_size)
    {
        case 1: return is_signed ? static_cast<UInt64>(static_cast<Int64>(unalignedLoadLittleEndian<Int8>(at))) : unalignedLoadLittleEndian<UInt8>(at);
        case 2: return is_signed ? static_cast<UInt64>(static_cast<Int64>(unalignedLoadLittleEndian<Int16>(at))) : unalignedLoadLittleEndian<UInt16>(at);
        case 4: return is_signed ? static_cast<UInt64>(static_cast<Int64>(unalignedLoadLittleEndian<Int32>(at))) : unalignedLoadLittleEndian<UInt32>(at);
        default: return unalignedLoadLittleEndian<UInt64>(at);
    }
}

/// The width of the differences of a chunk from its least integer, and that integer.
struct ChunkWidth
{
    UInt32 bits = 0;
    UInt64 reference = 0;
};

ChunkWidth widthOf(const char * chunk, size_t count, UInt8 element_size, bool is_signed)
{
    UInt64 least = integerAt(chunk, element_size, is_signed);
    UInt64 most = least;
    for (size_t i = 1; i < count; ++i)
    {
        const UInt64 value = integerAt(chunk + i * element_size, element_size, is_signed);
        if (is_signed ? static_cast<Int64>(value) < static_cast<Int64>(least) : value < least)
            least = value;
        if (is_signed ? static_cast<Int64>(value) > static_cast<Int64>(most) : value > most)
            most = value;
    }

    const auto bits = static_cast<UInt32>(std::bit_width(most - least));
    if (bits > max_packed_bits)
        return {.bits = raw_bits, .reference = 0};
    return {.bits = bits, .reference = least};
}

/// Packs the chunk into `out`, zeroed for `paddedBytes(packedBytes(count, bits))` and 8 more: each difference is ORed in
/// as 8 bytes from the byte its first bit is in, of which the bytes past its last bit are zeros.
void pack(const char * chunk, size_t count, UInt8 element_size, bool is_signed, ChunkWidth width, char * out)
{
    if (width.bits == raw_bits)
    {
        memcpy(out, chunk, count * element_size);
        return;
    }
    if (width.bits == 0)
        return;

    for (size_t i = 0; i < count; ++i)
    {
        const UInt64 delta = integerAt(chunk + i * element_size, element_size, is_signed) - width.reference;
        const size_t first_bit = i * width.bits;
        char * const at = out + first_bit / 8;
        unalignedStoreLittleEndian<UInt64>(at, unalignedLoadLittleEndian<UInt64>(at) | (delta << (first_bit % 8)));
    }
}

/// Writes the `count` integers of `T` packed in `bits` bits at `packed`, readable for 8 bytes from the byte of any bit.
template <typename T>
void unpackAs(const char * packed, size_t count, UInt32 bits, UInt64 reference, char * out)
{
    if (bits == 0)
    {
        for (size_t i = 0; i < count; ++i)
            unalignedStoreLittleEndian<T>(out + i * sizeof(T), static_cast<T>(reference));
        return;
    }

    const UInt64 mask = (UInt64{1} << bits) - 1;
    for (size_t i = 0; i < count; ++i)
    {
        const size_t first_bit = i * bits;
        const UInt64 delta = (unalignedLoadLittleEndian<UInt64>(packed + first_bit / 8) >> (first_bit % 8)) & mask;
        unalignedStoreLittleEndian<T>(out + i * sizeof(T), static_cast<T>(reference + delta));
    }
}

void unpack(const char * packed, size_t count, UInt32 bits, UInt32 element_size, UInt64 reference, char * out)
{
    switch (element_size)
    {
        case 1: unpackAs<UInt8>(packed, count, bits, reference, out); break;
        case 2: unpackAs<UInt16>(packed, count, bits, reference, out); break;
        case 4: unpackAs<UInt32>(packed, count, bits, reference, out); break;
        default: unpackAs<UInt64>(packed, count, bits, reference, out); break;
    }
}

/// The integers the codec packs: their size and whether they are signed.
std::pair<UInt8, bool> integerTypeInfo(const IDataType & type)
{
    WhichDataType which(type);
    const bool integer = which.isInt() || which.isUInt() || which.isDecimal() || which.isDateOrDate32() || which.isDateTime()
        || which.isDateTime64() || which.isEnum() || which.isIPv4();
    const size_t size = integer ? type.getSizeOfValueInMemory() : 0;
    if (!integer || !isElementSize(size))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Codec BitPacking cannot be applied to column of type {}: it packs integers of 1, 2, 4 or 8 bytes, Int*, UInt*, Decimal32, "
            "Decimal64, Date*, DateTime*, Enum* and IPv4",
            type.getName());
    return {static_cast<UInt8>(size), !type.isValueRepresentedByUnsignedInteger()};
}

}

CompressionCodecBitPacking::CompressionCodecBitPacking(UInt8 element_size_, bool is_signed_)
    : element_size(element_size_)
    , is_signed(is_signed_)
{
}

uint8_t CompressionCodecBitPacking::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::BitPacking);
}

ASTPtr CompressionCodecBitPacking::getCodecDescription() const
{
    return makeCodecDescription("BitPacking");
}

void CompressionCodecBitPacking::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecBitPacking::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    /// Every chunk stored as it is, rounded up to 8 bytes, and the tail.
    const size_t num_chunks = numChunks(uncompressed_size);
    return static_cast<UInt32>(dataOffset(num_chunks) + paddedBytes(uncompressed_size) + num_chunks * 8 + 8 + padding_bytes);
}

UInt32 CompressionCodecBitPacking::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if (element_size == 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Codec BitPacking cannot compress without the type of its column");
    const size_t tail_bytes = source_size % element_size;
    const size_t packed_bytes = source_size - tail_bytes;
    const size_t num_chunks = numChunks(packed_bytes);
    dest[0] = static_cast<char>(version);
    dest[1] = static_cast<char>(element_size);
    dest[2] = static_cast<char>(tail_bytes);
    dest[3] = 0;
    unalignedStoreLittleEndian<UInt32>(dest + 4, static_cast<UInt32>(num_chunks));

    /// The packed bytes are ORed into zeros, and the bytes past them, that an 8-byte store spills into, are zeros as well.
    size_t at = dataOffset(num_chunks);
    memset(dest + at, 0, getMaxCompressedDataSize(source_size) - at);

    for (size_t i = 0; i < num_chunks; ++i)
    {
        const char * const chunk = source + i * chunk_bytes;
        const size_t count = std::min(chunk_bytes, packed_bytes - i * chunk_bytes) / element_size;
        const ChunkWidth width = widthOf(chunk, count, element_size, is_signed);

        char * const descriptor = dest + descriptorOffset(i);
        descriptor[0] = static_cast<char>(width.bits);
        memset(descriptor + 1, 0, 7);
        unalignedStoreLittleEndian<UInt64>(descriptor + 8, width.reference);

        pack(chunk, count, element_size, is_signed, width, dest + at);
        at += paddedBytes(packedBytes(count, width.bits));
    }

    memcpy(dest + at, source + packed_bytes, tail_bytes);
    return static_cast<UInt32>(at + tail_bytes + padding_bytes);
}

UInt32 CompressionCodecBitPacking::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < header_bytes)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "A BitPacking block of {} bytes", source_size);

    const auto block_version = static_cast<UInt8>(source[0]);
    const auto block_element_size = static_cast<UInt8>(source[1]);
    const size_t tail_bytes = static_cast<UInt8>(source[2]);
    const size_t num_chunks = unalignedLoadLittleEndian<UInt32>(source + 4);
    const size_t packed_bytes = uncompressed_size - std::min(tail_bytes, static_cast<size_t>(uncompressed_size));
    if (block_version != version || !isElementSize(block_element_size) || tail_bytes >= block_element_size || tail_bytes > uncompressed_size
        || packed_bytes % block_element_size != 0 || num_chunks != numChunks(packed_bytes)
        || dataOffset(num_chunks) + tail_bytes + padding_bytes > source_size)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "A BitPacking block of {} bytes, version {}, of integers of {} bytes in {} chunks and a tail of {} bytes, where {} bytes unpacked make {}",
            source_size,
            static_cast<size_t>(block_version),
            static_cast<size_t>(block_element_size),
            num_chunks,
            tail_bytes,
            uncompressed_size,
            numChunks(packed_bytes));

    size_t at = dataOffset(num_chunks);
    for (size_t i = 0; i < num_chunks; ++i)
    {
        const char * const descriptor = source + descriptorOffset(i);
        const auto bits = static_cast<UInt32>(static_cast<UInt8>(descriptor[0]));
        const UInt64 reference = unalignedLoadLittleEndian<UInt64>(descriptor + 8);
        const size_t count = std::min(chunk_bytes, packed_bytes - i * chunk_bytes) / block_element_size;

        if (!(bits <= max_packed_bits || (bits == raw_bits && block_element_size == 8)) || bits > 8 * block_element_size)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Chunk {} of a BitPacking block packs integers of {} bytes in {} bits", i, static_cast<size_t>(block_element_size), bits);

        const size_t packed = packedBytes(count, bits);
        if (at + paddedBytes(packed) + tail_bytes + padding_bytes > source_size)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Chunk {} of a BitPacking block of {} bytes ends at {}", i, source_size, at + packed);

        char * const out = dest + i * chunk_bytes;
        if (bits == raw_bits)
            memcpy(out, source + at, count * block_element_size);
        else
            unpack(source + at, count, bits, block_element_size, reference, out);
        at += paddedBytes(packed);
    }

    if (at + tail_bytes + padding_bytes != source_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "A BitPacking block of {} bytes holds {} bytes of chunks", source_size, at);
    memcpy(dest + packed_bytes, source + at, tail_bytes);
    return uncompressed_size;
}

void registerCodecBitPacking(CompressionCodecFactory & factory)
{
    const UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::BitPacking);
    factory.registerCompressionCodecWithType("BitPacking", method_code, [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        if (arguments && !arguments->children.empty())
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "BitPacking codec must have 0 parameters, given {}", arguments->children.size());

        /// Without a type, as a codec is made for reading, it unpacks what the header of a block says.
        if (!column_type)
            return std::make_shared<CompressionCodecBitPacking>(0, false);

        const auto [element_size, is_signed] = integerTypeInfo(*column_type);
        return std::make_shared<CompressionCodecBitPacking>(element_size, is_signed);
    });
}

}
