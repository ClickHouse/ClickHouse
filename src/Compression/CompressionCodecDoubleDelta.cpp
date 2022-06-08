#ifdef HAS_RESERVED_IDENTIFIER
#pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <base/unaligned.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTIdentifier.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/BitHelpers.h>
#include <IO/WriteHelpers.h>

#include <cstring>
#include <algorithm>
#include <cstdlib>
#include <type_traits>
#include <limits>


namespace DB
{

/** NOTE DoubleDelta is surprisingly bad name. The only excuse is that it comes from an academic paper.
  * Most people will think that "double delta" is just applying delta transform twice.
  * But in fact it is something more than applying delta transform twice.
  */

/** DoubleDelta column codec implementation.
 *
 * Based on Gorilla paper: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf, which was extended
 * to support 64bit types. The drawback is 1 extra bit for 32-byte wide deltas: 5-bit prefix
 * instead of 4-bit prefix.
 *
 * This codec is best used against monotonic integer sequences with constant (or almost constant)
 * stride, like event timestamp for some monitoring application.
 *
 * Given input sequence a: [a0, a1, ... an]:
 *
 * First, write number of items (sizeof(int32)*8 bits):                n
 * Then write first item as is (sizeof(a[0])*8 bits):                  a[0]
 * Second item is written as delta (sizeof(a[0])*8 bits):              a[1] - a[0]
 * Loop over remaining items and calculate double delta:
 *   double_delta = a[i] - 2 * a[i - 1] + a[i - 2]
 *   Write it in compact binary form with `BitWriter`
 *   if double_delta == 0:
 *      write 1bit:                                                    0
 *   else if -63 < double_delta < 64:
 *      write 2 bit prefix:                                            10
 *      write sign bit (1 if signed):                                  x
 *      write 7-1 bits of abs(double_delta - 1):                       xxxxxx
 *   else if -255 < double_delta < 256:
 *      write 3 bit prefix:                                            110
 *      write sign bit (1 if signed):                                  x
 *      write 9-1 bits of abs(double_delta - 1):                       xxxxxxxx
 *   else if -2047 < double_delta < 2048:
 *      write 4 bit prefix:                                            1110
 *      write sign bit (1 if signed):                                  x
 *      write 12-1 bits of abs(double_delta - 1):                      xxxxxxxxxxx
 *   else if double_delta fits into 32-bit int:
 *      write 5 bit prefix:                                            11110
 *      write sign bit (1 if signed):                                  x
 *      write 32-1 bits of abs(double_delta - 1):                      xxxxxxxxxxx...
 *   else
 *      write 5 bit prefix:                                            11111
 *      write sign bit (1 if signed):                                  x
 *      write 64-1 bits of abs(double_delta - 1):                      xxxxxxxxxxx...
 *
 * @example sequence of UInt8 values [1, 2, 3, 4, 5, 6, 7, 8, 9 10] is encoded as (codec header is omitted):
 *
 * .- 4-byte little-endian sequence length (10 == 0xa)
 * |               .- 1 byte (sizeof(UInt8) a[0]                                            : 0x01
 * |               |   .- 1 byte of delta: a[1] - a[0] = 2 - 1 = 1                          : 0x01
 * |               |   |   .- 8 zero bits since double delta for remaining 8 elements was 0 : 0x00
 * v_______________v___v___v___
 * \x0a\x00\x00\x00\x01\x01\x00
 *
 * @example sequence of Int16 values [-10, 10, -20, 20, -40, 40] is encoded as:
 *
 * .- 4-byte little endian sequence length = 6                                 : 0x00000006
 * |                .- 2 bytes (sizeof(Int16) a[0] as UInt16 = -10             : 0xfff6
 * |                |       .- 2 bytes of delta: a[1] - a[0] = 10 - (-10) = 20 : 0x0014
 * |                |       |       .- 4 encoded double deltas (see below)
 * v_______________ v______ v______ v______________________
 * \x06\x00\x00\x00\xf6\xff\x14\x00\xb8\xe2\x2e\xb1\xe4\x58
 *
 * 4 binary encoded double deltas (\xb8\xe2\x2e\xb1\xe4\x58):
 * double_delta (DD) = -20 - 2 * 10 + (-10) = -50
 * .- 2-bit prefix                                                         : 0b10
 * | .- sign-bit                                                           : 0b1
 * | |.- abs(DD - 1) = 49                                                  : 0b110001
 * | ||
 * | ||      DD = 20 - 2 * (-20) + 10 = 70
 * | ||      .- 3-bit prefix                                               : 0b110
 * | ||      |  .- sign bit                                                : 0b0
 * | ||      |  |.- abs(DD - 1) = 69                                       : 0b1000101
 * | ||      |  ||
 * | ||      |  ||        DD = -40 - 2 * 20 + (-20) = -100
 * | ||      |  ||        .- 3-bit prefix                                  : 0b110
 * | ||      |  ||        |    .- sign-bit                                 : 0b0
 * | ||      |  ||        |    |.- abs(DD - 1) = 99                        : 0b1100011
 * | ||      |  ||        |    ||
 * | ||      |  ||        |    ||       DD = 40 - 2 * (-40) + 20 = 140
 * | ||      |  ||        |    ||       .- 3-bit prefix                    : 0b110
 * | ||      |  ||        |    ||       |  .- sign bit                     : 0b0
 * | ||      |  ||        |    ||       |  |.- abs(DD - 1) = 139           : 0b10001011
 * | ||      |  ||        |    ||       |  ||
 * V_vv______V__vv________V____vv_______V__vv________,- padding bits
 * 10111000 11100010 00101110 10110001 11100100 01011000
 *
 * Please also see unit tests for:
 *   * Examples on what output `BitWriter` produces on predefined input.
 *   * Compatibility tests solidifying encoded binary output on set of predefined sequences.
 */
class CompressionCodecDoubleDelta : public ICompressionCodec
{
public:
    explicit CompressionCodecDoubleDelta(UInt8 data_bytes_size_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:

    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }

private:
    UInt8 data_bytes_size;
};


namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int BAD_ARGUMENTS;
}

namespace
{

inline Int64 getMaxValueForByteSize(Int8 byte_size)
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

// delta size prefix and data lengths based on few high bits peeked from binary stream
const WriteSpec WRITE_SPEC_LUT[32] = {
    // 0b0 - 1-bit prefix, no data to read
    /* 00000 */ {1, 0b0, 0},
    /* 00001 */ {1, 0b0, 0},
    /* 00010 */ {1, 0b0, 0},
    /* 00011 */ {1, 0b0, 0},
    /* 00100 */ {1, 0b0, 0},
    /* 00101 */ {1, 0b0, 0},
    /* 00110 */ {1, 0b0, 0},
    /* 00111 */ {1, 0b0, 0},
    /* 01000 */ {1, 0b0, 0},
    /* 01001 */ {1, 0b0, 0},
    /* 01010 */ {1, 0b0, 0},
    /* 01011 */ {1, 0b0, 0},
    /* 01100 */ {1, 0b0, 0},
    /* 01101 */ {1, 0b0, 0},
    /* 01110 */ {1, 0b0, 0},
    /* 01111 */ {1, 0b0, 0},

    // 0b10 - 2 bit prefix, 7 bits of data
    /* 10000 */ {2, 0b10, 7},
    /* 10001 */ {2, 0b10, 7},
    /* 10010 */ {2, 0b10, 7},
    /* 10011 */ {2, 0b10, 7},
    /* 10100 */ {2, 0b10, 7},
    /* 10101 */ {2, 0b10, 7},
    /* 10110 */ {2, 0b10, 7},
    /* 10111 */ {2, 0b10, 7},

    // 0b110 - 3 bit prefix, 9 bits of data
    /* 11000 */ {3, 0b110, 9},
    /* 11001 */ {3, 0b110, 9},
    /* 11010 */ {3, 0b110, 9},
    /* 11011 */ {3, 0b110, 9},

    // 0b1110 - 4 bit prefix, 12 bits of data
    /* 11100 */ {4, 0b1110, 12},
    /* 11101 */ {4, 0b1110, 12},

    // 5-bit prefixes
    /* 11110 */ {5, 0b11110, 32},
    /* 11111 */ {5, 0b11111, 64},
};


template <typename T>
WriteSpec getDeltaWriteSpec(const T & value)
{
    // TODO: to speed up things a bit by counting number of leading zeroes instead of doing lots of comparisons
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
    // Since only unsigned int has granted 2-complement overflow handling,
    // we are doing math here only on unsigned types.
    // To simplify and booletproof code, we enforce ValueType to be unsigned too.
    static_assert(is_unsigned_v<ValueType>, "ValueType must be unsigned.");
    using UnsignedDeltaType = ValueType;

    // We use signed delta type to turn huge unsigned values into smaller signed:
    // ffffffff => -1
    using SignedDeltaType = typename std::make_signed_t<UnsignedDeltaType>;

    if (source_size % sizeof(ValueType) != 0)
        throw Exception("Cannot compress, data size " + toString(source_size)
                        + " is not aligned to " + toString(sizeof(ValueType)), ErrorCodes::CANNOT_COMPRESS);
    const char * source_end = source + source_size;
    const char * dest_start = dest;

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

    BitWriter writer(dest, getCompressedDataSize(sizeof(ValueType), source_size - sizeof(ValueType)*2));

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
            const auto sign = signed_dd < 0;

            // -1 shrinks dd down to fit into number of bits, and there can't be 0, so it is OK.
            const auto abs_value = static_cast<UnsignedDeltaType>(std::abs(signed_dd) - 1);
            const auto write_spec = getDeltaWriteSpec(signed_dd);

            writer.writeBits(write_spec.prefix_bits, write_spec.prefix);
            writer.writeBits(1, sign);
            writer.writeBits(write_spec.data_bits - 1, abs_value);
        }
    }

    writer.flush();

    return (dest - dest_start) + (writer.count() + 7) / 8;
}

template <typename ValueType>
void decompressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    static_assert(is_unsigned_v<ValueType>, "ValueType must be unsigned.");
    using UnsignedDeltaType = ValueType;

    const char * source_end = source + source_size;
    const char * output_end = dest + output_size;

    if (source + sizeof(UInt32) > source_end)
        return;

    const UInt32 items_count = unalignedLoad<UInt32>(source);
    source += sizeof(items_count);

    ValueType prev_value{};
    UnsignedDeltaType prev_delta{};

    // decoding first item
    if (source + sizeof(ValueType) > source_end || items_count < 1)
        return;

    prev_value = unalignedLoad<ValueType>(source);
    if (dest + sizeof(prev_value) > output_end)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress the data");
    unalignedStore<ValueType>(dest, prev_value);

    source += sizeof(prev_value);
    dest += sizeof(prev_value);

    // decoding second item
    if (source + sizeof(UnsignedDeltaType) > source_end || items_count < 2)
        return;

    prev_delta = unalignedLoad<UnsignedDeltaType>(source);
    prev_value = prev_value + static_cast<ValueType>(prev_delta);
    if (dest + sizeof(prev_value) > output_end)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress the data");
    unalignedStore<ValueType>(dest, prev_value);

    source += sizeof(prev_delta);
    dest += sizeof(prev_value);

    BitReader reader(source, source_size - sizeof(prev_value) - sizeof(prev_delta) - sizeof(items_count));

    // since data is tightly packed, up to 1 bit per value, and last byte is padded with zeroes,
    // we have to keep track of items to avoid reading more that there is.
    for (UInt32 items_read = 2; items_read < items_count && !reader.eof(); ++items_read)
    {
        UnsignedDeltaType double_delta = 0;

        static_assert(sizeof(WRITE_SPEC_LUT)/sizeof(WRITE_SPEC_LUT[0]) == 32); // 5-bit prefix lookup table
        const auto write_spec = WRITE_SPEC_LUT[reader.peekByte() >> (8 - 5)]; // only 5 high bits of peeked byte value

        reader.skipBufferedBits(write_spec.prefix_bits); // discard the prefix value, since we've already used it
        if (write_spec.data_bits != 0)
        {
            const UInt8 sign = reader.readBit();
            double_delta = reader.readBits(write_spec.data_bits - 1) + 1;
            if (sign)
            {
                /// It's well defined for unsigned data types.
                /// In contrast, it's undefined to do negation of the most negative signed number due to overflow.
                double_delta = -double_delta;
            }
        }

        const UnsignedDeltaType delta = double_delta + prev_delta;
        const ValueType curr_value = prev_value + delta;
        if (dest + sizeof(curr_value) > output_end)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress the data");
        unalignedStore<ValueType>(dest, curr_value);
        dest += sizeof(curr_value);

        prev_delta = curr_value - prev_value;
        prev_value = curr_value;
    }
}

UInt8 getDataBytesSize(const IDataType * column_type)
{
    if (!column_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec DoubleDelta is not applicable for {} because the data type is not of fixed size",
            column_type->getName());

    size_t max_size = column_type->getSizeOfValueInMemory();
    if (max_size == 1 || max_size == 2 || max_size == 4 || max_size == 8)
        return static_cast<UInt8>(max_size);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec Delta is only applicable for data types of size 1, 2, 4, 8 bytes. Given type {}",
            column_type->getName());
}

}


CompressionCodecDoubleDelta::CompressionCodecDoubleDelta(UInt8 data_bytes_size_)
    : data_bytes_size(data_bytes_size_)
{
    setCodecDescription("DoubleDelta");
}

uint8_t CompressionCodecDoubleDelta::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::DoubleDelta);
}

void CompressionCodecDoubleDelta::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
    hash.update(data_bytes_size);
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

    if (bytes_size == 0)
        throw Exception("Cannot decompress. File has wrong header", ErrorCodes::CANNOT_DECOMPRESS);

    UInt8 bytes_to_skip = uncompressed_size % bytes_size;
    UInt32 output_size = uncompressed_size - bytes_to_skip;

    if (static_cast<UInt32>(2 + bytes_to_skip) > source_size)
        throw Exception("Cannot decompress. File has wrong header", ErrorCodes::CANNOT_DECOMPRESS);

    memcpy(dest, &source[2], bytes_to_skip);
    UInt32 source_size_no_header = source_size - bytes_to_skip - 2;
    switch (bytes_size)
    {
    case 1:
        decompressDataForType<UInt8>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
        break;
    case 2:
        decompressDataForType<UInt16>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
        break;
    case 4:
        decompressDataForType<UInt32>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
        break;
    case 8:
        decompressDataForType<UInt64>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
        break;
    }
}

void registerCodecDoubleDelta(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::DoubleDelta);
    factory.registerCompressionCodecWithType("DoubleDelta", method_code,
        [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        if (arguments)
            throw Exception("Codec DoubleDelta does not accept any arguments", ErrorCodes::BAD_ARGUMENTS);

        UInt8 data_bytes_size = column_type ? getDataBytesSize(column_type) : 0;
        return std::make_shared<CompressionCodecDoubleDelta>(data_bytes_size);
    });
}

CompressionCodecPtr getCompressionCodecDoubleDelta(UInt8 data_bytes_size)
{
    return std::make_shared<CompressionCodecDoubleDelta>(data_bytes_size);
}

}
