#include <Compression/CompressionCodecGorilla.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <common/unaligned.h>
#include <Parsers/IAST_fwd.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/BitHelpers.h>

#include <string.h>
#include <algorithm>
#include <type_traits>

#include <bitset>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int BAD_ARGUMENTS;
}

namespace
{

constexpr inline UInt8 getBitLengthOfLength(UInt8 data_bytes_size)
{
    // 1-byte value is 8 bits, and we need 4 bits to represent 8 : 1000,
    // 2-byte         16 bits        =>    5
    // 4-byte         32 bits        =>    6
    // 8-byte         64 bits        =>    7
    const UInt8 bit_lengths[] = {0, 4, 5, 0, 6, 0, 0, 0, 7};
    assert(data_bytes_size >= 1 && data_bytes_size < sizeof(bit_lengths) && bit_lengths[data_bytes_size] != 0);

    return bit_lengths[data_bytes_size];
}


UInt32 getCompressedHeaderSize(UInt8 data_bytes_size)
{
    const UInt8 items_count_size = 4;

    return items_count_size + data_bytes_size;
}

UInt32 getCompressedDataSize(UInt8 data_bytes_size, UInt32 uncompressed_size)
{
    const UInt32 items_count = uncompressed_size / data_bytes_size;
    static const auto DATA_BIT_LENGTH = getBitLengthOfLength(data_bytes_size);
    // -1 since there must be at least 1 non-zero bit.
    static const auto LEADING_ZEROES_BIT_LENGTH = DATA_BIT_LENGTH - 1;

    // worst case (for 32-bit value):
    // 11 + 5 bits of leading zeroes bit-size + 5 bits of data bit-size + non-zero data bits.
    const UInt32 max_item_size_bits = 2 + LEADING_ZEROES_BIT_LENGTH + DATA_BIT_LENGTH + data_bytes_size * 8;

    // + 8 is to round up to next byte.
    return (items_count * max_item_size_bits + 8) / 8;
}

struct BinaryValueInfo
{
    UInt8 leading_zero_bits;
    UInt8 data_bits;
    UInt8 trailing_zero_bits;
};

template <typename T>
BinaryValueInfo getLeadingAndTrailingBits(const T & value)
{
    constexpr UInt8 bit_size = sizeof(T) * 8;

    const UInt8 lz = getLeadingZeroBits(value);
    const UInt8 tz = getTrailingZeroBits(value);
    const UInt8 data_size = value == 0 ? 0 : static_cast<UInt8>(bit_size - lz - tz);

    return BinaryValueInfo{lz, data_size, tz};
}

template <typename T>
UInt32 compressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 dest_size)
{
    static const auto DATA_BIT_LENGTH = getBitLengthOfLength(sizeof(T));
    // -1 since there must be at least 1 non-zero bit.
    static const auto LEADING_ZEROES_BIT_LENGTH = DATA_BIT_LENGTH - 1;

    if (source_size % sizeof(T) != 0)
        throw Exception("Cannot compress, data size " + toString(source_size) + " is not aligned to " + toString(sizeof(T)), ErrorCodes::CANNOT_COMPRESS);
    const char * source_end = source + source_size;
    const char * dest_start = dest;
    const char * dest_end = dest + dest_size;

    const UInt32 items_count = source_size / sizeof(T);

    unalignedStore<UInt32>(dest, items_count);
    dest += sizeof(items_count);

    T prev_value{};
    // That would cause first XORed value to be written in-full.
    BinaryValueInfo prev_xored_info{0, 0, 0};

    if (source < source_end)
    {
        prev_value = unalignedLoad<T>(source);
        unalignedStore<T>(dest, prev_value);

        source += sizeof(prev_value);
        dest += sizeof(prev_value);
    }

    BitWriter writer(dest, dest_end - dest);

    while (source < source_end)
    {
        const T curr_value = unalignedLoad<T>(source);
        source += sizeof(curr_value);

        const auto xored_data = curr_value ^ prev_value;
        const BinaryValueInfo curr_xored_info = getLeadingAndTrailingBits(xored_data);

        if (xored_data == 0)
        {
            writer.writeBits(1, 0);
        }
        else if (prev_xored_info.data_bits != 0
                && prev_xored_info.leading_zero_bits <= curr_xored_info.leading_zero_bits
                && prev_xored_info.trailing_zero_bits <= curr_xored_info.trailing_zero_bits)
        {
            writer.writeBits(2, 0b10);
            writer.writeBits(prev_xored_info.data_bits, xored_data >> prev_xored_info.trailing_zero_bits);
        }
        else
        {
            writer.writeBits(2, 0b11);
            writer.writeBits(LEADING_ZEROES_BIT_LENGTH, curr_xored_info.leading_zero_bits);
            writer.writeBits(DATA_BIT_LENGTH, curr_xored_info.data_bits);
            writer.writeBits(curr_xored_info.data_bits, xored_data >> curr_xored_info.trailing_zero_bits);
            prev_xored_info = curr_xored_info;
        }

        prev_value = curr_value;
    }

    writer.flush();

    return (dest - dest_start) + (writer.count() + 7) / 8;
}

template <typename T>
void decompressDataForType(const char * source, UInt32 source_size, char * dest)
{
    static const auto DATA_BIT_LENGTH = getBitLengthOfLength(sizeof(T));
    // -1 since there must be at least 1 non-zero bit.
    static const auto LEADING_ZEROES_BIT_LENGTH = DATA_BIT_LENGTH - 1;

    const char * source_end = source + source_size;

    if (source + sizeof(UInt32) > source_end)
        return;

    const UInt32 items_count = unalignedLoad<UInt32>(source);
    source += sizeof(items_count);

    T prev_value{};

    // decoding first item
    if (source + sizeof(T) > source_end || items_count < 1)
        return;

    prev_value = unalignedLoad<T>(source);
    unalignedStore<T>(dest, prev_value);

    source += sizeof(prev_value);
    dest += sizeof(prev_value);

    BitReader reader(source, source_size - sizeof(items_count) - sizeof(prev_value));

    BinaryValueInfo prev_xored_info{0, 0, 0};

    // since data is tightly packed, up to 1 bit per value, and last byte is padded with zeroes,
    // we have to keep track of items to avoid reading more that there is.
    for (UInt32 items_read = 1; items_read < items_count && !reader.eof(); ++items_read)
    {
        T curr_value = prev_value;
        BinaryValueInfo curr_xored_info = prev_xored_info;
        T xored_data{};

        if (reader.readBit() == 1)
        {
            if (reader.readBit() == 1)
            {
                // 0b11 prefix
                curr_xored_info.leading_zero_bits = reader.readBits(LEADING_ZEROES_BIT_LENGTH);
                curr_xored_info.data_bits = reader.readBits(DATA_BIT_LENGTH);
                curr_xored_info.trailing_zero_bits = sizeof(T) * 8 - curr_xored_info.leading_zero_bits - curr_xored_info.data_bits;
            }
            // else: 0b10 prefix - use prev_xored_info

            if (curr_xored_info.leading_zero_bits == 0
                && curr_xored_info.data_bits == 0
                && curr_xored_info.trailing_zero_bits == 0)
            {
                throw Exception("Cannot decompress gorilla-encoded data: corrupted input data.",
                        ErrorCodes::CANNOT_DECOMPRESS);
            }

            xored_data = reader.readBits(curr_xored_info.data_bits);
            xored_data <<= curr_xored_info.trailing_zero_bits;
            curr_value = prev_value ^ xored_data;
        }
        // else: 0b0 prefix - use prev_value

        unalignedStore<T>(dest, curr_value);
        dest += sizeof(curr_value);

        prev_xored_info = curr_xored_info;
        prev_value = curr_value;
    }
}

UInt8 getDataBytesSize(DataTypePtr column_type)
{
    if (!column_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec Gorilla is not applicable for {} because the data type is not of fixed size",
            column_type->getName());

    size_t max_size = column_type->getSizeOfValueInMemory();
    if (max_size == 1 || max_size == 2 || max_size == 4 || max_size == 8)
        return static_cast<UInt8>(max_size);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec Delta is only applicable for data types of size 1, 2, 4, 8 bytes. Given type {}",
            column_type->getName());
}

}


CompressionCodecGorilla::CompressionCodecGorilla(UInt8 data_bytes_size_)
    : data_bytes_size(data_bytes_size_)
{
}

uint8_t CompressionCodecGorilla::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Gorilla);
}

String CompressionCodecGorilla::getCodecDesc() const
{
    return "Gorilla";
}

UInt32 CompressionCodecGorilla::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    const auto result = 2 // common header
            + data_bytes_size // max bytes skipped if source is not properly aligned.
            + getCompressedHeaderSize(data_bytes_size) // data-specific header
            + getCompressedDataSize(data_bytes_size, uncompressed_size);

    return result;
}

UInt32 CompressionCodecGorilla::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % data_bytes_size;
    dest[0] = data_bytes_size;
    dest[1] = bytes_to_skip; /// unused (backward compatibility)
    memcpy(&dest[2], source, bytes_to_skip);
    size_t start_pos = 2 + bytes_to_skip;
    UInt32 result_size = 0;

    const UInt32 compressed_size = getMaxCompressedDataSize(source_size);
    switch (data_bytes_size)
    {
    case 1:
        result_size = compressDataForType<UInt8>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], compressed_size);
        break;
    case 2:
        result_size = compressDataForType<UInt16>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], compressed_size);
        break;
    case 4:
        result_size = compressDataForType<UInt32>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], compressed_size);
        break;
    case 8:
        result_size = compressDataForType<UInt64>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], compressed_size);
        break;
    }

    return 1 + 1 + result_size;
}

void CompressionCodecGorilla::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
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

void CompressionCodecGorilla::useInfoAboutType(const DataTypePtr & data_type)
{
    data_bytes_size = getDataBytesSize(data_type);
}

void registerCodecGorilla(CompressionCodecFactory & factory)
{
    UInt8 method_code = UInt8(CompressionMethodByte::Gorilla);
    factory.registerCompressionCodecWithType("Gorilla", method_code,
        [&](const ASTPtr & arguments, DataTypePtr column_type) -> CompressionCodecPtr
    {
        if (arguments)
            throw Exception("Codec Gorilla does not accept any arguments", ErrorCodes::BAD_ARGUMENTS);

        UInt8 data_bytes_size = column_type ? getDataBytesSize(column_type) : 0;   /// Maybe postponed to the call to "useInfoAboutType"
        return std::make_shared<CompressionCodecGorilla>(data_bytes_size);
    });
}
}
