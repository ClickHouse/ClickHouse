#include <Compression/CompressionCodecDoubleDelta.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <common/unaligned.h>
#include <Parsers/IAST_fwd.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/BitHelpers.h>
#include <IO/WriteHelpers.h>

#include <string.h>
#include <algorithm>
#include <cstdlib>
#include <type_traits>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
}

namespace
{

Int64 getMaxValueForByteSize(UInt8 byte_size)
{
    switch (byte_size)
    {
        case sizeof(UInt8):
            return std::numeric_limits<Int8>::max();
        case sizeof(UInt16):
            return std::numeric_limits<Int16>::max();
        case sizeof(UInt32):
            return std::numeric_limits<Int32>::max();
        case sizeof(UInt64):
            return std::numeric_limits<Int64>::max();
        default:
            assert(false && "only 1, 2, 4 and 8 data sizes are supported");
    }
    __builtin_unreachable();
}

struct WriteSpec
{
    const UInt8 prefix_bits;
    const UInt8 prefix;
    const UInt8 data_bits;
};

const std::array<UInt8, 5> DELTA_SIZES{7, 9, 12, 32, 64};

template <typename T>
WriteSpec getDeltaWriteSpec(const T & value)
{
    if (value > -63 && value < 64)
    {
        return WriteSpec{2, 0b10, 7};
    }
    else if (value > -255 && value < 256)
    {
        return WriteSpec{3, 0b110, 9};
    }
    else if (value > -2047 && value < 2048)
    {
        return WriteSpec{4, 0b1110, 12};
    }
    else if (value > std::numeric_limits<Int32>::min() && value < std::numeric_limits<Int32>::max())
    {
        return WriteSpec{5, 0b11110, 32};
    }
    else
    {
        return WriteSpec{5, 0b11111, 64};
    }
}

WriteSpec getDeltaMaxWriteSpecByteSize(UInt8 data_bytes_size)
{
    return getDeltaWriteSpec(getMaxValueForByteSize(data_bytes_size));
}

UInt32 getCompressedHeaderSize(UInt8 data_bytes_size)
{
    const UInt8 items_count_size = 4;
    const UInt8 first_delta_bytes_size = data_bytes_size;

    return items_count_size + data_bytes_size + first_delta_bytes_size;
}

UInt32 getCompressedDataSize(UInt8 data_bytes_size, UInt32 uncompressed_size)
{
    const UInt32 items_count = uncompressed_size / data_bytes_size;
    const auto double_delta_write_spec = getDeltaMaxWriteSpecByteSize(data_bytes_size);

    const UInt32 max_item_size_bits = double_delta_write_spec.prefix_bits + double_delta_write_spec.data_bits;

    // + 8 is to round up to next byte.
    auto result = (items_count * max_item_size_bits + 7) / 8;

    return result;
}

template <typename ValueType>
UInt32 compressDataForType(const char * source, UInt32 source_size, char * dest)
{
    // Since only unsinged int has granted 2-compliment overflow handling, we are doing math here on unsigned types.
    // To simplify and booletproof code, we operate enforce ValueType to be unsigned too.
    static_assert(std::is_unsigned_v<ValueType>, "ValueType must be unsigned.");
    using UnsignedDeltaType = ValueType;

    // We use signed delta type to turn huge unsigned values into smaller signed:
    // ffffffff => -1
    using SignedDeltaType = typename std::make_signed<UnsignedDeltaType>::type;

    if (source_size % sizeof(ValueType) != 0)
        throw Exception("Cannot compress, data size " + toString(source_size)
                        + " is not aligned to " + toString(sizeof(ValueType)), ErrorCodes::CANNOT_COMPRESS);
    const char * source_end = source + source_size;

    const UInt32 items_count = source_size / sizeof(ValueType);
    unalignedStore<UInt32>(dest, items_count);
    dest += sizeof(items_count);

    ValueType prev_value{};
    UnsignedDeltaType prev_delta{};

    if (source < source_end)
    {
        prev_value = unalignedLoad<ValueType>(source);
        unalignedStore<ValueType>(dest, prev_value);

        source += sizeof(prev_value);
        dest += sizeof(prev_value);
    }

    if (source < source_end)
    {
        const ValueType curr_value = unalignedLoad<ValueType>(source);

        prev_delta = curr_value - prev_value;
        unalignedStore<UnsignedDeltaType>(dest, prev_delta);

        source += sizeof(curr_value);
        dest += sizeof(prev_delta);
        prev_value = curr_value;
    }

    WriteBuffer buffer(dest, getCompressedDataSize(sizeof(ValueType), source_size - sizeof(ValueType)*2));
    BitWriter writer(buffer);

    int item = 2;
    for (; source < source_end; source += sizeof(ValueType), ++item)
    {
        const ValueType curr_value = unalignedLoad<ValueType>(source);

        const UnsignedDeltaType delta = curr_value - prev_value;
        const UnsignedDeltaType double_delta = delta - prev_delta;

        prev_delta = delta;
        prev_value = curr_value;

        if (double_delta == 0)
        {
            writer.writeBits(1, 0);
        }
        else
        {
            const SignedDeltaType signed_dd = static_cast<SignedDeltaType>(double_delta);
            const auto sign = std::signbit(signed_dd);
            // -1 shirnks dd down to fit into number of bits, and there can't be 0, so it is OK.
            const auto abs_value = static_cast<UnsignedDeltaType>(std::abs(signed_dd) - 1);
            const auto write_spec = getDeltaWriteSpec(signed_dd);

            writer.writeBits(write_spec.prefix_bits, write_spec.prefix);
            writer.writeBits(1, sign);
            writer.writeBits(write_spec.data_bits - 1, abs_value);
        }
    }

    writer.flush();

    return sizeof(items_count) + sizeof(prev_value) + sizeof(prev_delta) + buffer.count();
}

template <typename ValueType>
void decompressDataForType(const char * source, UInt32 source_size, char * dest)
{
    static_assert(std::is_unsigned_v<ValueType>, "ValueType must be unsigned.");
    using UnsignedDeltaType = ValueType;
    using SignedDeltaType = typename std::make_signed<UnsignedDeltaType>::type;

    const char * source_end = source + source_size;

    const UInt32 items_count = unalignedLoad<UInt32>(source);
    source += sizeof(items_count);

    ValueType prev_value{};
    UnsignedDeltaType prev_delta{};

    if (source < source_end)
    {
        prev_value = unalignedLoad<ValueType>(source);
        unalignedStore<ValueType>(dest, prev_value);

        source += sizeof(prev_value);
        dest += sizeof(prev_value);
    }

    if (source < source_end)
    {
        prev_delta = unalignedLoad<UnsignedDeltaType>(source);
        prev_value = prev_value + static_cast<ValueType>(prev_delta);
        unalignedStore<ValueType>(dest, prev_value);

        source += sizeof(prev_delta);
        dest += sizeof(prev_value);
    }

    ReadBufferFromMemory buffer(source, source_size - sizeof(prev_value) - sizeof(prev_delta) - sizeof(items_count));
    BitReader reader(buffer);

    // since data is tightly packed, up to 1 bit per value, and last byte is padded with zeroes,
    // we have to keep track of items to avoid reading more that there is.
    for (UInt32 items_read = 2; items_read < items_count && !reader.eof(); ++items_read)
    {
        UnsignedDeltaType double_delta = 0;
        if (reader.readBit() == 1)
        {
            UInt8 i = 0;
            for (; i < sizeof(DELTA_SIZES) - 1; ++i)
            {
                const auto next_bit = reader.readBit();
                if (next_bit == 0)
                {
                    break;
                }
            }

            const UInt8 sign = reader.readBit();
            SignedDeltaType signed_dd = static_cast<SignedDeltaType>(reader.readBits(DELTA_SIZES[i] - 1) + 1);
            if (sign)
            {
                signed_dd *= -1;
            }
            double_delta = static_cast<UnsignedDeltaType>(signed_dd);
        }
        // else if first bit is zero, no need to read more data.

        const UnsignedDeltaType delta = double_delta + prev_delta;
        const ValueType curr_value = prev_value + delta;
        unalignedStore<ValueType>(dest, curr_value);
        dest += sizeof(curr_value);

        prev_delta = curr_value - prev_value;
        prev_value = curr_value;
    }
}

UInt8 getDataBytesSize(DataTypePtr column_type)
{
    UInt8 data_bytes_size = 1;
    if (column_type && column_type->haveMaximumSizeOfValue())
    {
        size_t max_size = column_type->getSizeOfValueInMemory();
        if (max_size == 1 || max_size == 2 || max_size == 4 || max_size == 8)
            data_bytes_size = static_cast<UInt8>(max_size);
    }
    return data_bytes_size;
}

}


CompressionCodecDoubleDelta::CompressionCodecDoubleDelta(UInt8 data_bytes_size_)
    : data_bytes_size(data_bytes_size_)
{
}

UInt8 CompressionCodecDoubleDelta::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::DoubleDelta);
}

String CompressionCodecDoubleDelta::getCodecDesc() const
{
    return "DoubleDelta";
}

UInt32 CompressionCodecDoubleDelta::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    const auto result = 2 // common header
            + data_bytes_size // max bytes skipped if source is not properly aligned.
            + getCompressedHeaderSize(data_bytes_size) // data-specific header
            + getCompressedDataSize(data_bytes_size, uncompressed_size);

    return result;
}

UInt32 CompressionCodecDoubleDelta::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % data_bytes_size;
    dest[0] = data_bytes_size;
    dest[1] = bytes_to_skip; /// unused (backward compatibility)
    memcpy(&dest[2], source, bytes_to_skip);
    size_t start_pos = 2 + bytes_to_skip;
    UInt32 compressed_size = 0;

    switch (data_bytes_size)
    {
    case 1:
        compressed_size = compressDataForType<UInt8>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 2:
        compressed_size = compressDataForType<UInt16>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 4:
        compressed_size = compressDataForType<UInt32>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 8:
        compressed_size = compressDataForType<UInt64>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    }

    return 1 + 1 + compressed_size;
}

void CompressionCodecDoubleDelta::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < 2)
        throw Exception("Cannot decompress. File has wrong header", ErrorCodes::CANNOT_DECOMPRESS);

    UInt8 bytes_size = source[0];
    UInt8 bytes_to_skip = uncompressed_size % bytes_size;

    if (UInt32(2 + bytes_to_skip) > source_size)
        throw Exception("Cannot decompress. File has wrong header", ErrorCodes::CANNOT_DECOMPRESS);

    memcpy(dest, &source[2], bytes_to_skip);
    UInt32 source_size_no_header = source_size - bytes_to_skip - 2;
    switch (bytes_size)
    {
    case 1:
        decompressDataForType<UInt8>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip]);
        break;
    case 2:
        decompressDataForType<UInt16>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip]);
        break;
    case 4:
        decompressDataForType<UInt32>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip]);
        break;
    case 8:
        decompressDataForType<UInt64>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip]);
        break;
    }
}

void CompressionCodecDoubleDelta::useInfoAboutType(DataTypePtr data_type)
{
    data_bytes_size = getDataBytesSize(data_type);
}

void registerCodecDoubleDelta(CompressionCodecFactory & factory)
{
    UInt8 method_code = UInt8(CompressionMethodByte::DoubleDelta);
    factory.registerCompressionCodecWithType("DoubleDelta", method_code, [&](const ASTPtr &, DataTypePtr column_type) -> CompressionCodecPtr
    {
        UInt8 delta_bytes_size = getDataBytesSize(column_type);
        return std::make_shared<CompressionCodecDoubleDelta>(delta_bytes_size);
    });
}
}
