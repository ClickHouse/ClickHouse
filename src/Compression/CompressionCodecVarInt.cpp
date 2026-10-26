#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <base/defines.h>
#include <base/unaligned.h>
#include <Parsers/IAST.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>
#include <type_traits>


namespace DB
{

/** `VarInt` codec - a byte-aligned variable-length encoding of integers.
  *
  * The codec is a data preparation codec (it does not compress on its own) and is meant to be
  * combined with an entropy compressor, for example `CODEC(Delta, VarInt, ZSTD(1))`.
  *
  * Every block is encoded as follows:
  *
  *   1. Values are treated as integers of `data_bytes_size` bytes. Their sign bit is flipped,
  *      which turns signed order into unsigned order.
  *   2. A frame of reference is chosen and stored once: either the smallest value of the block or
  *      zero, whichever gives the smaller encoding. Zero is preferred when the block contains a few
  *      far-off outliers, because subtracting the minimum would then shift all other values.
  *   3. The difference between each value and the frame of reference is stored using groups of
  *      four values. Each group starts with a 16-bit control word that holds the byte width
  *      (0 to 8) of every value in a 4-bit nibble, followed by the values' low bytes in
  *      little-endian order. A zero difference takes no bytes at all.
  *
  * Subtracting a per-block frame of reference before the variable-length encoding is the
  * frame-of-reference idea used by Lemire et al. in FastPFor; grouping values and storing their
  * widths together is the group-varint idea from the same line of work (StreamVByte). Both keep
  * the output byte-aligned, so a subsequent `ZSTD` can deduplicate the repeated control words and
  * the zero runs of regular time series. For a constant scrape interval, every difference is zero,
  * and the whole block consists of repeated control words that `ZSTD` reduces to almost nothing.
  *
  * The sign flip makes the codec work for signed and unsigned columns alike. In particular, after
  * `Delta, Delta` the double deltas of an unsigned timestamp column are negative values represented
  * modulo 2^64; the flip makes them sort as negative, so they stay narrow instead of looking like
  * huge unsigned numbers.
  *
  * The first byte of the payload is `data_bytes_size`. The payload of a codec chain is not
  * necessarily a multiple of the value size (the previous codec prepends its own block header),
  * so the leading `uncompressed_size % data_bytes_size` bytes are stored verbatim.
  */
class CompressionCodecVarInt : public ICompressionCodec
{
public:
    explicit CompressionCodecVarInt(UInt8 data_bytes_size_);

    uint8_t getMethodByte() const override;

    ASTPtr getCodecDescription() const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }

    String getDescription() const override
    {
        return "Preprocessor. Variable-length integer encoding of values relative to a per-block frame of reference, "
               "grouped four at a time with a control word holding each value's byte width; "
               "helps ZSTD where the deltas are small and varied, such as counters and irregular time series.";
    }

private:
    const UInt8 data_bytes_size;
};


namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int BAD_ARGUMENTS;
}

namespace
{

constexpr UInt32 group_size = 4;

/// The sign bit of an `N`-bit two's complement number. Flipping it maps signed order to unsigned order.
template <typename T>
constexpr T getSignMask()
{
    return static_cast<T>(static_cast<T>(1) << (sizeof(T) * 8 - 1));
}

/// Number of bytes needed to store `value`, 0 for zero.
inline UInt8 getByteWidth(UInt64 value)
{
    return value == 0 ? 0 : static_cast<UInt8>((64 - std::countl_zero(value) + 7) / 8);
}

inline void writeLittleEndianBytes(UInt64 value, UInt8 num_bytes, char *& out)
{
    for (UInt8 i = 0; i < num_bytes; ++i)
        *out++ = static_cast<char>(value >> (8 * i));
}

inline UInt64 readLittleEndianBytes(const char *& in, const char * in_end, UInt8 num_bytes)
{
    if (static_cast<size_t>(in_end - in) < num_bytes)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data: unexpected end of data");

    UInt64 value = 0;
    for (UInt8 i = 0; i < num_bytes; ++i)
        value |= static_cast<UInt64>(static_cast<UInt8>(in[i])) << (8 * i);
    in += num_bytes;
    return value;
}

template <typename T>
UInt32 compressDataForType(const char * source, UInt32 source_size, char * dest)
{
    static_assert(std::is_unsigned_v<T>, "T must be unsigned; signed values are handled by flipping the sign bit");
    chassert(source_size % sizeof(T) == 0);

    const UInt32 count = source_size / sizeof(T);
    if (count == 0)
        return 0;

    const T sign_mask = getSignMask<T>();

    auto keyAt = [&](UInt32 i) { return static_cast<T>(unalignedLoadLittleEndian<T>(source + i * sizeof(T)) ^ sign_mask); };

    T min_key = keyAt(0);
    for (UInt32 i = 1; i < count; ++i)
        min_key = std::min(min_key, keyAt(i));

    /// Choose the frame of reference that stores fewer bytes. The minimum is usually best, but a
    /// single far-off small outlier (for example the leading bytes of the previous codec in a chain)
    /// makes every other value look huge; zero then stores only the outlier wide.
    const T zero_key = sign_mask;
    UInt64 min_key_size = 0;
    UInt64 zero_key_size = 0;
    for (UInt32 i = 0; i < count; ++i)
    {
        const T key = keyAt(i);
        min_key_size += getByteWidth(static_cast<T>(key - min_key));
        zero_key_size += getByteWidth(static_cast<T>(key - zero_key));
    }
    const T reference = min_key_size <= zero_key_size ? min_key : zero_key;

    char * out = dest;
    unalignedStoreLittleEndian<T>(out, reference);
    out += sizeof(T);

    for (UInt32 i = 0; i < count; i += group_size)
    {
        char * control_pos = out;
        out += sizeof(UInt16);
        UInt16 control = 0;

        for (UInt32 j = 0; j < group_size && i + j < count; ++j)
        {
            const UInt64 delta = static_cast<UInt64>(static_cast<T>(keyAt(i + j) - reference));
            const UInt8 width = getByteWidth(delta);

            control |= static_cast<UInt16>(width) << (4 * j);
            writeLittleEndianBytes(delta, width, out);
        }

        unalignedStoreLittleEndian<UInt16>(control_pos, control);
    }

    return static_cast<UInt32>(out - dest);
}

template <typename T>
void decompressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 count)
{
    static_assert(std::is_unsigned_v<T>, "T must be unsigned; signed values are handled by flipping the sign bit");

    const char * in = source;
    const char * const in_end = source + source_size;

    /// A block that reaches here holds at least one value: `doDecompressData` returns early for an
    /// empty block and for a block smaller than one value.
    if (count == 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data: the block is too small to hold a value");

    if (static_cast<size_t>(in_end - in) < sizeof(T))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data: unexpected end of data");

    const T reference = unalignedLoadLittleEndian<T>(in);
    in += sizeof(T);

    const T sign_mask = getSignMask<T>();

    for (UInt32 i = 0; i < count; i += group_size)
    {
        if (static_cast<size_t>(in_end - in) < sizeof(UInt16))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data: unexpected end of data");

        const UInt16 control = unalignedLoadLittleEndian<UInt16>(in);
        in += sizeof(UInt16);

        for (UInt32 j = 0; j < group_size && i + j < count; ++j)
        {
            const UInt8 width = (control >> (4 * j)) & 0x0F;
            if (width > sizeof(T))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data: invalid value width {}", static_cast<UInt32>(width));

            const UInt64 delta = readLittleEndianBytes(in, in_end, width);
            const T key = static_cast<T>(reference + static_cast<T>(delta));
            unalignedStoreLittleEndian<T>(dest + (i + j) * sizeof(T), static_cast<T>(key ^ sign_mask));
        }
    }

    if (in != in_end)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data: unexpected trailing data");
}

UInt8 getDataBytesSize(const IDataType * column_type)
{
    if (!column_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec VarInt is not applicable for {} because the data type is not of fixed size",
            column_type->getName());

    const size_t data_bytes_size = column_type->getSizeOfValueInMemory();
    if (data_bytes_size != 1 && data_bytes_size != 2 && data_bytes_size != 4 && data_bytes_size != 8)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Codec VarInt is only applicable for data types of size 1, 2, 4, 8 bytes. Given type {}",
            column_type->getName());

    return static_cast<UInt8>(data_bytes_size);
}

}

CompressionCodecVarInt::CompressionCodecVarInt(UInt8 data_bytes_size_)
    : data_bytes_size(data_bytes_size_)
{
}

ASTPtr CompressionCodecVarInt::getCodecDescription() const
{
    return makeCodecDescription("VarInt");
}

uint8_t CompressionCodecVarInt::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::VarInt);
}

void CompressionCodecVarInt::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
    hash.update(data_bytes_size);
}

UInt32 CompressionCodecVarInt::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    const UInt32 bytes_to_skip = uncompressed_size % data_bytes_size;
    const UInt32 count = (uncompressed_size - bytes_to_skip) / data_bytes_size;
    const UInt32 groups = (count + group_size - 1) / group_size;

    /// Header (`data_bytes_size`), skipped bytes, frame of reference,
    /// one control word per group and up to `data_bytes_size` bytes per value.
    const UInt64 result = sizeof(UInt8) + bytes_to_skip + data_bytes_size + static_cast<UInt64>(groups) * sizeof(UInt16) + static_cast<UInt64>(count) * data_bytes_size;
    /// `getCompressedReserveSize` adds the block header to this value in `UInt32`.
    if (result > std::numeric_limits<UInt32>::max() - getHeaderSize())
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with VarInt codec: the reserved size for {} bytes overflows 4 GiB", uncompressed_size);

    return static_cast<UInt32>(result);
}

UInt32 CompressionCodecVarInt::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    const UInt8 bytes_to_skip = source_size % data_bytes_size;

    dest[0] = static_cast<char>(data_bytes_size);
    memcpy(&dest[1], source, bytes_to_skip);

    const size_t payload_pos = 1 + bytes_to_skip;
    UInt32 payload_size = 0;

    switch (data_bytes_size)
    {
        case 1: payload_size = compressDataForType<UInt8>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[payload_pos]); break;
        case 2: payload_size = compressDataForType<UInt16>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[payload_pos]); break;
        case 4: payload_size = compressDataForType<UInt32>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[payload_pos]); break;
        case 8: payload_size = compressDataForType<UInt64>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[payload_pos]); break;
        default:
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with VarInt codec: unsupported data size {}", static_cast<UInt32>(data_bytes_size));
    }

    return static_cast<UInt32>(payload_pos + payload_size);
}

UInt32 CompressionCodecVarInt::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < 1)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data. File has wrong header");

    if (uncompressed_size == 0)
        return 0;

    const UInt8 bytes_size = static_cast<UInt8>(source[0]);
    if (bytes_size != 1 && bytes_size != 2 && bytes_size != 4 && bytes_size != 8)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data. File has wrong header");

    /// The block was compressed as a whole, so the number of bytes that do not form a whole value
    /// follows from the uncompressed size.
    const UInt8 bytes_to_skip = uncompressed_size % bytes_size;

    if (static_cast<UInt32>(1 + bytes_to_skip) > source_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data. File has wrong header");

    memcpy(dest, &source[1], bytes_to_skip);

    const UInt32 output_size = uncompressed_size - bytes_to_skip;
    const UInt32 count = output_size / bytes_size;
    const UInt32 payload_size = source_size - 1 - bytes_to_skip;

    /// The block is smaller than one value, so everything in it is the leading partial value.
    if (count == 0)
    {
        if (payload_size != 0)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data: unexpected trailing data");

        return uncompressed_size;
    }

    switch (bytes_size)
    {
        case 1: decompressDataForType<UInt8>(&source[1 + bytes_to_skip], payload_size, &dest[bytes_to_skip], count); break;
        case 2: decompressDataForType<UInt16>(&source[1 + bytes_to_skip], payload_size, &dest[bytes_to_skip], count); break;
        case 4: decompressDataForType<UInt32>(&source[1 + bytes_to_skip], payload_size, &dest[bytes_to_skip], count); break;
        case 8: decompressDataForType<UInt64>(&source[1 + bytes_to_skip], payload_size, &dest[bytes_to_skip], count); break;
        default:
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data: unsupported data size {}", static_cast<UInt32>(bytes_size));
    }

    return uncompressed_size;
}

void registerCodecVarInt(CompressionCodecFactory & factory)
{
    const UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::VarInt);

    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        if (arguments && !arguments->children.empty())
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "VarInt codec must have 0 parameters, given {}", arguments->children.size());

        /// Default: 1-byte values. This keeps the codec usable where the column type is unknown;
        /// a known type always wins so that the byte widths match the actual values.
        UInt8 data_bytes_size = 1;

        if (column_type)
            data_bytes_size = getDataBytesSize(column_type);

        return std::make_shared<CompressionCodecVarInt>(data_bytes_size);
    };

    factory.registerCompressionCodecWithType("VarInt", method_code, codec_builder);
}

}
