#pragma clang diagnostic ignored "-Wreserved-identifier"

#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <base/unaligned.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/BitHelpers.h>

#include <bitset>
#include <cstring>
#include <algorithm>
#include <type_traits>


namespace DB
{

/** Chimp column codec implementation.
 *
 * Implementation of Chimp128 algorithm proposed in: Panagiotis Liakos, Katia Papakonstantinopoulou, Yannis Kotidis:
 * Chimp: Efficient Lossless Floating Point Compression for Time Series Databases. Proc. VLDB Endow. 15(11): 3058-3070 (2022)
 * Available in: https://dl.acm.org/doi/abs/10.14778/3551793.3551852
 *
 */
class CompressionCodecChimp : public ICompressionCodec
{
public:
    explicit CompressionCodecChimp(UInt8 data_bytes_size_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:

    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    bool isFloatingPointTimeSeriesCodec() const override { return true; }

private:
    const UInt8 data_bytes_size;
};

namespace LeadingZero
{
    static const auto BIT_LENGTH = 3;
    static const short round[65] =
    {
        0, 0, 0, 0, 0, 0, 0, 0,
        8, 8, 8, 8, 12, 12, 12, 12,
        16, 16, 18, 18, 20, 20, 22, 22,
        24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24, 24
    };
    static const short binaryRepresentation[65] =
    {
        0, 0, 0, 0, 0, 0, 0, 0,
        1, 1, 1, 1, 2, 2, 2, 2,
        3, 3, 4, 4, 5, 5, 6, 6,
        7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7, 7
    };
    static const short reverseBinaryRepresentation[8] = {0, 8, 12, 16, 18, 20, 22, 24};
}

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

namespace
{

constexpr UInt8 getBitLengthOfLength(UInt8 data_bytes_size)
{
    // 4-byte value is 32 bits, and we need 5 bits to represent 32 values
    // 8-byte          64 bits        =>    6
    const UInt8 bit_lengths[] = {0, 0, 0, 0, 5, 0, 0, 0, 6};
    assert(data_bytes_size >= 1 && data_bytes_size < sizeof(bit_lengths) && bit_lengths[data_bytes_size] != 0);
    return bit_lengths[data_bytes_size];
}

UInt32 getCompressedHeaderSize(UInt8 data_bytes_size)
{
    constexpr UInt8 items_count_size = 4;
    return items_count_size + data_bytes_size;
}

UInt32 getCompressedDataSize(UInt8 data_bytes_size, UInt32 uncompressed_size)
{
    const UInt32 items_count = uncompressed_size / data_bytes_size;
    static const auto DATA_BIT_LENGTH = getBitLengthOfLength(data_bytes_size);
    static const short LOG_NO_PREVIOUS_VALUES = static_cast<short>(std::log2(data_bytes_size * 16));
    // worst case (for 32-bit value):
    // 2 bits (flag) + 6 bits (previous values index) + 3 bits (no of leading zeroes) + 5 bits(data bit-size) + non-zero data bits.
    const UInt32 max_item_size_bits = 2 + LOG_NO_PREVIOUS_VALUES + LeadingZero::BIT_LENGTH + DATA_BIT_LENGTH + data_bytes_size * 8;
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
BinaryValueInfo getBinaryValueInfo(const T & value)
{
    constexpr UInt8 bit_size = sizeof(T) * 8;
    const UInt8 lz = LeadingZero::round[getLeadingZeroBits(value)];
    const UInt8 tz = getTrailingZeroBits(value);
    const UInt8 data_size = value == 0 ? 0 : static_cast<UInt8>(bit_size - lz - tz);
    return {lz, data_size, tz};
}

template <typename T>
UInt32 compressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 dest_size)
{
    if (source_size % sizeof(T) != 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with Chimp codec, data size {} is not aligned to {}", source_size, sizeof(T));

    const char * const source_end = source + source_size;
    const char * const dest_start = dest;
    const char * const dest_end = dest + dest_size;

    const UInt32 items_count = source_size / sizeof(T);

    static const short NO_PREVIOUS_VALUES = sizeof(T) * 16;
    T stored_values[NO_PREVIOUS_VALUES];
    for (int i = 0; i < NO_PREVIOUS_VALUES; i++)
    {
        stored_values[i] = 0;
    }
    static const short LOG_NO_PREVIOUS_VALUES = static_cast<short>(std::log2(NO_PREVIOUS_VALUES));
    static const short THRESHOLD = 6 + LOG_NO_PREVIOUS_VALUES;
    static const int ARRAY_SIZE = static_cast<int>(std::pow(2, THRESHOLD + 1));
    int indices[ARRAY_SIZE];
    for (int i = 0; i < ARRAY_SIZE; i++)
    {
        indices[i] = 0;
    }
    static const short setLsb = ARRAY_SIZE - 1;

    unalignedStoreLittleEndian<UInt32>(dest, items_count);
    dest += sizeof(items_count);

    T prev_value = 0;
    // That would cause first XORed value to be written in-full.
    BinaryValueInfo prev_xored_info{0, 0, 0};

    if (source < source_end)
    {
        prev_value = unalignedLoadLittleEndian<T>(source);
        unalignedStoreLittleEndian<T>(dest, prev_value);

        source += sizeof(prev_value);
        dest += sizeof(prev_value);
        stored_values[0] = prev_value;
    }

    BitWriter writer(dest, dest_end - dest);

    static const auto DATA_BIT_LENGTH = getBitLengthOfLength(sizeof(T));

    int total = 0;
    int previous_index = 0;
    int current_index = 0;

    while (source < source_end)
    {
        const T curr_value = unalignedLoadLittleEndian<T>(source);
        source += sizeof(curr_value);

        // find best matching previous value
        T xored_data;
        BinaryValueInfo curr_xored_info;
        int match_key = static_cast<int>(curr_value & setLsb);
        int match_index = indices[match_key];
        if ((total - match_index) < NO_PREVIOUS_VALUES)
        {
            T tempXor = curr_value ^ stored_values[match_index % NO_PREVIOUS_VALUES];
            curr_xored_info = getBinaryValueInfo(tempXor);
            // if match is good enough, use it
            if (curr_xored_info.trailing_zero_bits > THRESHOLD)
            {
                previous_index = match_index % NO_PREVIOUS_VALUES;
                xored_data = tempXor;
            }
            // otherwise use immediately previous value
            else
            {
                previous_index =  total % NO_PREVIOUS_VALUES;
                xored_data = curr_value ^ stored_values[previous_index];
                curr_xored_info = getBinaryValueInfo(xored_data);
            }
        }
        // if match is outside of range, use immediately previous value
        else
        {
            previous_index =  total % NO_PREVIOUS_VALUES;
            xored_data = curr_value ^ stored_values[previous_index];
            curr_xored_info = getBinaryValueInfo(xored_data);
        }

        // encode
        // 0b00 prefix
        if (xored_data == 0)
        {
            writer.writeBits(2, 0b00);
            writer.writeBits(LOG_NO_PREVIOUS_VALUES, previous_index);
            curr_xored_info.leading_zero_bits = 255; // max value so it can't be used
        }
        // 0b01 prefix
        else if (curr_xored_info.trailing_zero_bits > THRESHOLD)
        {
            writer.writeBits(2, 0b01);
            writer.writeBits(LOG_NO_PREVIOUS_VALUES, previous_index);
            writer.writeBits(LeadingZero::BIT_LENGTH, LeadingZero::binaryRepresentation[curr_xored_info.leading_zero_bits]);
            writer.writeBits(DATA_BIT_LENGTH, curr_xored_info.data_bits);
            writer.writeBits(curr_xored_info.data_bits, xored_data >> curr_xored_info.trailing_zero_bits);
            curr_xored_info.leading_zero_bits = 255; // max value so it can't be used
        }
        // 0b10 prefix
        else if (prev_xored_info.leading_zero_bits == curr_xored_info.leading_zero_bits)
        {
            writer.writeBits(2, 0b10);
            writer.writeBits(curr_xored_info.data_bits + curr_xored_info.trailing_zero_bits, xored_data);
        }
        // 0b11 prefix
        else
        {
            writer.writeBits(2, 0b11);
            writer.writeBits(LeadingZero::BIT_LENGTH, LeadingZero::binaryRepresentation[curr_xored_info.leading_zero_bits]);
            writer.writeBits(curr_xored_info.data_bits + curr_xored_info.trailing_zero_bits, xored_data);
        }

        // update stored previous values and indices
        prev_xored_info = curr_xored_info;
        prev_value = curr_value;
        current_index = (current_index + 1) % NO_PREVIOUS_VALUES;
        stored_values[current_index] = curr_value;
        total++;
        indices[match_key] = total;
    }
    writer.flush();

    return static_cast<UInt32>((dest - dest_start) + (writer.count() + 7) / 8);
}

template <typename T>
void decompressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 dest_size)
{
    static const short NO_PREVIOUS_VALUES = sizeof(T) * 16;
    static const short LOG_NO_PREVIOUS_VALUES = static_cast<short>(std::log2(NO_PREVIOUS_VALUES));
    int current_index = 0;
    T stored_values[NO_PREVIOUS_VALUES];
    for (int i = 0; i < NO_PREVIOUS_VALUES; i++)
    {
        stored_values[i] = 0;
    }

    const char * const source_end = source + source_size;

    if (source + sizeof(UInt32) > source_end)
        return;


    const UInt32 items_count = unalignedLoadLittleEndian<UInt32>(source);
    source += sizeof(items_count);

    T prev_value = 0;

    // decoding first item
    if (source + sizeof(T) > source_end || items_count < 1)
        return;

    if (static_cast<UInt64>(items_count) * sizeof(T) > dest_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Chimp-encoded data: corrupted input data.");

    prev_value = unalignedLoadLittleEndian<T>(source);
    unalignedStoreLittleEndian<T>(dest, prev_value);

    source += sizeof(prev_value);
    dest += sizeof(prev_value);
    stored_values[0] = prev_value;

    BitReader reader(source, source_size - sizeof(items_count) - sizeof(prev_value));

    BinaryValueInfo prev_xored_info{0, 0, 0};

    static const auto DATA_BIT_LENGTH = getBitLengthOfLength(sizeof(T));

    // since data is tightly packed, up to 1 bit per value, and last byte is padded with zeroes,
    // we have to keep track of items to avoid reading more than there is.
    for (UInt32 items_read = 1; items_read < items_count && !reader.eof(); ++items_read)
    {
        T curr_value = prev_value;
        BinaryValueInfo curr_xored_info = prev_xored_info;
        T xored_data = 0;
        UInt64 match_index;
        UInt8 flag = reader.readBits(2);
        switch (flag)
        {
            // 0b11 prefix
            case 3:
                curr_xored_info.leading_zero_bits = LeadingZero::reverseBinaryRepresentation[reader.readBits(LeadingZero::BIT_LENGTH)];
                curr_xored_info.data_bits = sizeof(T) * 8 - curr_xored_info.leading_zero_bits;
                xored_data = static_cast<T>(reader.readBits(curr_xored_info.data_bits));
                curr_value = prev_value ^ xored_data;
                break;
            // 0b10 prefix
            case 2:
                curr_xored_info.leading_zero_bits = prev_xored_info.leading_zero_bits;
                curr_xored_info.data_bits = sizeof(T) * 8 - curr_xored_info.leading_zero_bits;
                xored_data = static_cast<T>(reader.readBits(curr_xored_info.data_bits));
                curr_value = prev_value ^ xored_data;
                break;
            // 0b01 prefix
            case 1:
                match_index = reader.readBits(LOG_NO_PREVIOUS_VALUES);
                prev_value = stored_values[match_index];
                curr_xored_info.leading_zero_bits = LeadingZero::reverseBinaryRepresentation[reader.readBits(LeadingZero::BIT_LENGTH)];
                curr_xored_info.data_bits = reader.readBits(DATA_BIT_LENGTH);
                if (curr_xored_info.data_bits == 0)
                {
                    curr_xored_info.data_bits = sizeof(T) * 8;
                }
                curr_xored_info.trailing_zero_bits = sizeof(T) * 8 - curr_xored_info.leading_zero_bits - curr_xored_info.data_bits;
                xored_data = static_cast<T>(reader.readBits(curr_xored_info.data_bits));
                xored_data <<= curr_xored_info.trailing_zero_bits;
                curr_value = prev_value ^ xored_data;
                break;
            // 0b00 prefix
            case 0:
                match_index = reader.readBits(LOG_NO_PREVIOUS_VALUES);
                prev_value = stored_values[match_index];
                curr_value = prev_value;
                break;
        }
        unalignedStoreLittleEndian<T>(dest, curr_value);
        dest += sizeof(curr_value);

        current_index = (current_index + 1) % NO_PREVIOUS_VALUES;
        stored_values[current_index] = curr_value;
        prev_xored_info = curr_xored_info;
        prev_value = curr_value;
    }
}

UInt8 getDataBytesSize(const IDataType * column_type)
{
    if (!column_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec Chimp is not applicable for {} because the data type is not of fixed size",
            column_type->getName());

    size_t max_size = column_type->getSizeOfValueInMemory();
    if (max_size == 1 || max_size == 2 || max_size == 4 || max_size == 8)
        return static_cast<UInt8>(max_size);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec Chimp is only applicable for data types of size 1, 2, 4, 8 bytes. Given type {}",
            column_type->getName());
}

}


CompressionCodecChimp::CompressionCodecChimp(UInt8 data_bytes_size_)
    : data_bytes_size(data_bytes_size_)
{
    setCodecDescription("Chimp");
}

uint8_t CompressionCodecChimp::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Chimp);
}

void CompressionCodecChimp::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
    hash.update(data_bytes_size);
}

UInt32 CompressionCodecChimp::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    const auto result = 2 // common header
            + data_bytes_size // max bytes skipped if source is not properly aligned.
            + getCompressedHeaderSize(data_bytes_size) // data-specific header
            + getCompressedDataSize(data_bytes_size, uncompressed_size);
    return result;
}

UInt32 CompressionCodecChimp::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % data_bytes_size;
    dest[0] = data_bytes_size;
    dest[1] = bytes_to_skip; /// unused (backward compatibility)
    memcpy(&dest[2], source, bytes_to_skip);
    size_t start_pos = 2 + bytes_to_skip;
    UInt32 result_size = 0;

    const UInt32 compressed_size = getMaxCompressedDataSize(source_size);
    switch (data_bytes_size) // NOLINT(bugprone-switch-missing-default-case)
    {
    case 4:
        result_size = compressDataForType<UInt32>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], compressed_size);
        break;
    case 8:
        result_size = compressDataForType<UInt64>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], compressed_size);
        break;
    }
    return 2 + bytes_to_skip + result_size;
}

void CompressionCodecChimp::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Chimp-encoded data. File has wrong header");

    UInt8 bytes_size = source[0];

    if (bytes_size == 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Chimp-encoded data. File has wrong header");

    UInt8 bytes_to_skip = uncompressed_size % bytes_size;

    if (static_cast<UInt32>(2 + bytes_to_skip) > source_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Chimp-encoded data. File has wrong header");

    if (bytes_to_skip >= uncompressed_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Chimp-encoded data. File has wrong header");

    memcpy(dest, &source[2], bytes_to_skip);
    UInt32 source_size_no_header = source_size - bytes_to_skip - 2;
    UInt32 uncompressed_size_left = uncompressed_size - bytes_to_skip;
    switch (bytes_size) // NOLINT(bugprone-switch-missing-default-case)
    {
    case 4:
        decompressDataForType<UInt32>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], uncompressed_size_left);
        break;
    case 8:
        decompressDataForType<UInt64>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], uncompressed_size_left);
        break;
    default:
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Chimp-encoded data. File has wrong header");
    }
}

void registerCodecChimp(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::Chimp);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        /// Default bytes size is 1
        UInt8 data_bytes_size = 1;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "Chimp codec must have 1 parameter, given {}", arguments->children.size());

            const auto children = arguments->children;
            const auto * literal = children[0]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::UInt64)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Chimp codec argument must be unsigned integer");

            size_t user_bytes_size = literal->value.safeGet<UInt64>();
            if (user_bytes_size != 4 && user_bytes_size != 8)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Argument value for Chimp codec can be 4 or 8, given {}", user_bytes_size);
            data_bytes_size = static_cast<UInt8>(user_bytes_size);
        }
        else if (column_type)
        {
            data_bytes_size = getDataBytesSize(column_type);
        }

        return std::make_shared<CompressionCodecChimp>(data_bytes_size);
    };
    factory.registerCompressionCodecWithType("Chimp", method_code, codec_builder);
}
}
