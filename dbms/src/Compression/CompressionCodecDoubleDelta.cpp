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

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
}

namespace
{
UInt32 getDeltaTypeByteSize(UInt8 data_bytes_size)
{
    // both delta and double delta can be twice the size of data item, but not less than 32 bits and not more that 64.
    return std::min(64/8, std::max(32/8, data_bytes_size * 2));
}

UInt32 getCompressedHeaderSize(UInt8 data_bytes_size)
{
    const UInt8 items_count_size = 4;

    return items_count_size + data_bytes_size + getDeltaTypeByteSize(data_bytes_size);
}

UInt32 getCompressedDataSize(UInt8 data_bytes_size, UInt32 uncompressed_size)
{
    const UInt32 items_count = uncompressed_size / data_bytes_size;

    // 11111 + max 64 bits of double delta.
    const UInt32 max_item_size_bits = 5 + getDeltaTypeByteSize(data_bytes_size) * 8;

    // + 8 is to round up to next byte.
    return (items_count * max_item_size_bits + 8) / 8;
}

struct WriteSpec
{
    const UInt8 prefix_bits;
    const UInt8 prefix;
    const UInt8 data_bits;
};

template <typename T>
WriteSpec getWriteSpec(const T & value)
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

template <typename T, typename DeltaType>
UInt32 compressDataForType(const char * source, UInt32 source_size, char * dest)
{
    using UnsignedDeltaType = typename std::make_unsigned<DeltaType>::type;

    if (source_size % sizeof(T) != 0)
        throw Exception("Cannot compress, data size " + toString(source_size) + " is not aligned to " + toString(sizeof(T)), ErrorCodes::CANNOT_COMPRESS);
    const char * source_end = source + source_size;

    const UInt32 items_count = source_size / sizeof(T);
    unalignedStore(dest, items_count);
    dest += sizeof(items_count);

    T prev_value{};
    DeltaType prev_delta{};

    if (source < source_end)
    {
        prev_value = unalignedLoad<T>(source);
        unalignedStore(dest, prev_value);

        source += sizeof(prev_value);
        dest += sizeof(prev_value);
    }

    if (source < source_end)
    {
        const T curr_value = unalignedLoad<T>(source);
        prev_delta = static_cast<DeltaType>(curr_value - prev_value);
        unalignedStore(dest, prev_delta);

        source += sizeof(curr_value);
        dest += sizeof(prev_delta);
        prev_value = curr_value;
    }

    WriteBuffer buffer(dest, getCompressedDataSize(sizeof(T), source_size - sizeof(T)*2));
    BitWriter writer(buffer);

    for (; source < source_end; source += sizeof(T))
    {
        const T curr_value = unalignedLoad<T>(source);

        const auto delta = curr_value - prev_value;
        const DeltaType double_delta = static_cast<DeltaType>(delta - static_cast<T>(prev_delta));

        prev_delta = delta;
        prev_value = curr_value;

        if (double_delta == 0)
        {
            writer.writeBits(1, 0);
        }
        else
        {
            const auto sign = std::signbit(double_delta);
            const auto abs_value = static_cast<UnsignedDeltaType>(std::abs(double_delta));
            const auto write_spec = getWriteSpec(double_delta);

            writer.writeBits(write_spec.prefix_bits, write_spec.prefix);
            writer.writeBits(1, sign);
            writer.writeBits(write_spec.data_bits - 1, abs_value);
        }
    }

    writer.flush();

    return sizeof(items_count) + sizeof(prev_value) + sizeof(prev_delta) + buffer.count();
}

template <typename T, typename DeltaType>
void decompressDataForType(const char * source, UInt32 source_size, char * dest)
{
    const char * source_end = source + source_size;

    const UInt32 items_count = unalignedLoad<UInt32>(source);
    source += sizeof(items_count);

    T prev_value{};
    DeltaType prev_delta{};

    if (source < source_end)
    {
        prev_value = unalignedLoad<T>(source);
        unalignedStore(dest, prev_value);

        source += sizeof(prev_value);
        dest += sizeof(prev_value);
    }

    if (source < source_end)
    {
        prev_delta = unalignedLoad<DeltaType>(source);
        prev_value = static_cast<DeltaType>(prev_value) + prev_delta;
        unalignedStore(dest, prev_value);

        source += sizeof(prev_delta);
        dest += sizeof(prev_value);
    }

    ReadBufferFromMemory buffer(source, source_size - sizeof(prev_value) - sizeof(prev_delta) - sizeof(items_count));
    BitReader reader(buffer);

    // since data is tightly packed, up to 1 bit per value, and last byte is padded with zeroes,
    // we have to keep track of items to avoid reading more that there is.
    for (UInt32 items_read = 2; items_read < items_count && !reader.eof(); ++items_read)
    {
        DeltaType double_delta = 0;
        if (reader.readBit() == 1)
        {
            const UInt8 data_sizes[] = {6, 8, 11, 31, 63};
            UInt8 i = 0;
            for (; i < sizeof(data_sizes) - 1; ++i)
            {
                const auto next_bit = reader.readBit();
                if (next_bit == 0)
                    break;
            }

            const UInt8 sign = reader.readBit();
            double_delta = static_cast<DeltaType>(reader.readBits(data_sizes[i]));
            if (sign)
            {
                double_delta *= -1;
            }
        }
        // else if first bit is zero, no need to read more data.

        const T curr_value = static_cast<T>(prev_value + prev_delta + double_delta);
        unalignedStore(dest, curr_value);
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

} // namespace


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
    dest[1] = bytes_to_skip;
    memcpy(&dest[2], source, bytes_to_skip);
    size_t start_pos = 2 + bytes_to_skip;
    UInt32 compressed_size = 0;
    switch (data_bytes_size)
    {
    case 1:
        compressed_size = compressDataForType<UInt8, Int16>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 2:
        compressed_size = compressDataForType<UInt16, Int32>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 4:
        compressed_size = compressDataForType<UInt32, Int64>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 8:
        compressed_size = compressDataForType<UInt64, Int64>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    }

    return 1 + 1 + compressed_size;
}

void CompressionCodecDoubleDelta::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 /* uncompressed_size */) const
{
    UInt8 bytes_size = source[0];
    UInt8 bytes_to_skip = source[1];

    memcpy(dest, &source[2], bytes_to_skip);
    UInt32 source_size_no_header = source_size - bytes_to_skip - 2;
    switch (bytes_size)
    {
    case 1:
        decompressDataForType<UInt8, Int16>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip]);
        break;
    case 2:
        decompressDataForType<UInt16, Int32>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip]);
        break;
    case 4:
        decompressDataForType<UInt32, Int64>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip]);
        break;
    case 8:
        decompressDataForType<UInt64, Int64>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip]);
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
