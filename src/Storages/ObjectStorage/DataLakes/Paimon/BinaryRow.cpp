#include <bit>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/BinaryRow.h>
#include <base/Decimal.h>
#include <base/defines.h>
#include <base/types.h>
#include <Poco/BinaryReader.h>
#include <Poco/ByteOrder.h>
#include <Poco/Logger.h>
#include <Common/Logger.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace Paimon
{

inline UInt64 flipBytes(UInt64 value)
{
#if defined(POCO_HAVE_MSC_BYTESWAP)
    return _byteswap_uint64(value);
#elif defined(POCO_HAVE_GCC_BYTESWAP)
    return __builtin_bswap64(value);
#else
    UInt32 hi = UInt32(value >> 32);
    UInt32 lo = UInt32(value & 0xFFFFFFFF);
    return UInt64(flipBytes(hi)) | (UInt64(flipBytes(lo)) << 32);
#endif
}

BinaryRow::BinaryRow(const String & bytes_)
    : reader(bytes_)
    , need_flip(!isLittleEndian())
    , log(getLogger("BinaryRow"))
{
    auto to_hex_string = [](const std::string & input)
    {
        static const char hex_digits[] = "0123456789abcdef";
        WriteBufferFromOwnString result;

        for (unsigned char c : input)
        {
            writeChar(hex_digits[(c >> 4) & 0xF], result);
            writeChar(hex_digits[c & 0xF], result);
        }

        return result.str();
    };

    readIntBinary(arity, reader);
    /// arity stores as big endian
    if (!need_flip)
        arity = Poco::ByteOrder::flipBytes(arity);
    LOG_TEST(log, "arity: {} need_flip: {} bytes_size: {} bytes: {}", arity, need_flip, reader.length(), to_hex_string(reader));
}

bool BinaryRow::isLittleEndian()
{
    return std::endian::native == std::endian::little;
}

bool BinaryRow::isNullAt(Int32 pos)
{
    chassert(pos >= 0 && pos < arity);
    Int32 bits_index = HEADER_SIZE_IN_BITS + pos;
    seek(offset() + byteIndex(bits_index));
    Int8 res = 0;
    readIntBinary(res, reader);
    return (res & (1 << (bits_index & BIT_BYTE_INDEX_MASK))) != 0;
}

template <typename T>
T BinaryRow::getFixedSizeDataAt(Int32 absolute_offset)
{
    seek(absolute_offset);
    T t;
    readIntBinary(t, reader);
    LOG_TEST(log, "read value: {}, need_flip: {}", t, need_flip);

    if (need_flip)
    {
        if constexpr (std::is_same_v<Int64, T> || std::is_same_v<UInt64, T>)
        {
            t = static_cast<T>(flipBytes(static_cast<UInt64>(t)));
        }
        else
        {
            t = static_cast<T>(Poco::ByteOrder::flipBytes(static_cast<char>(t)));
        }
    }
    return t;
}

template <typename T>
T BinaryRow::getFixedSizeData(Int32 pos)
{
    chassert(pos >= 0 && pos < arity);
    return getFixedSizeDataAt<T>(getFieldOffset(pos));
}

/// `getDecimal` is defined in the header, so it instantiates this in other translation units and
/// needs a definition to link against. An implicit instantiation would not do: it has vague
/// linkage, and the compiler is free to drop it once the wrapper has been inlined into every
/// caller in this file, which is what happens in an optimized build.
template Int64 BinaryRow::getFixedSizeData<Int64>(Int32 pos);

bool BinaryRow::getBoolean(Int32 pos)
{
    char res = getFixedSizeData<char>(pos);
    return res != 0;
}

Int8 BinaryRow::getByte(Int32 pos)
{
    return getFixedSizeData<char>(pos);
}

Int16 BinaryRow::getShort(Int32 pos)
{
    return getFixedSizeData<int16_t>(pos);
}
Int32 BinaryRow::getInt(Int32 pos)
{
    return getFixedSizeData<Int32>(pos);
}

Int64 BinaryRow::getLong(Int32 pos)
{
    return getFixedSizeData<Int64>(pos);
}

Float32 BinaryRow::getFloat(Int32 pos)
{
    Int32 res = getFixedSizeData<Int32>(pos);
    auto * float_res = reinterpret_cast<Float32 *>(&res);
    return *float_res;
}

Float64 BinaryRow::getDouble(Int32 pos)
{
    Int64 res = getFixedSizeData<Int64>(pos);
    auto * float_res = reinterpret_cast<Float64 *>(&res);
    return *float_res;
}

String BinaryRow::copyBytes(Int32 offset, Int32 num_bytes)
{
    String result(num_bytes, 0);
    seek(offset);
    reader.readStrict(result.data(), num_bytes);
    return result;
}

String BinaryRow::getString(Int32 pos)
{
    chassert(pos >= 0 && pos < arity);
    Int32 field_offset = getFieldOffset(pos);
    Int64 offset_and_len = static_cast<Int64>(getFixedSizeData<UInt64>(pos));
    Int64 mark = offset_and_len & HIGHEST_FIRST_BIT;
    if (mark == 0)
    {
        Int32 sub_offset = static_cast<Int32>((offset_and_len >> 32));
        Int32 len = static_cast<Int32>(offset_and_len);
        LOG_TEST(log, "sub_offset: {}, len: {}, offset_and_len: {}", sub_offset, len, offset_and_len);
        return copyBytes(offset() + sub_offset, len);
    }
    else
    {
        Int32 len = static_cast<Int32>(static_cast<UInt64>((offset_and_len & HIGHEST_SECOND_TO_EIGHTH_BIT)) >> 56);
        LOG_TEST(log, "mark: {}, len: {}, offset_and_len: {}", mark, len, offset_and_len);
        if (isLittleEndian())
        {
            return copyBytes(field_offset, len);
        }
        else
        {
            return copyBytes(field_offset + 1, len);
        }
    }
}

String BinaryRow::getBinary(Int32 pos)
{
    return getString(pos);
}

Timestamp BinaryRow::getTimestamp(Int32 pos, Int32 precision)
{
    /// A timestamp of precision <= 3 - `Timestamp.isCompact` in Paimon - fits in the 8-byte field
    /// slot and is stored there as a plain number of milliseconds.
    if (precision <= 3)
        return Timestamp{.millisecond = getFixedSizeData<Int64>(pos), .nano_of_millisecond = 0};

    /// Above that Paimon splits the value: the field slot holds
    /// `(sub_offset << 32) | nano_of_millisecond` and the milliseconds live in the variable-length
    /// part of the row, at `offset() + sub_offset`. See `BinaryRow.getTimestamp` and
    /// `MemorySegmentUtils.readTimestampData` in Paimon.
    const Int64 offset_and_nano_of_millisecond = getFixedSizeData<Int64>(pos);
    const auto nano_of_millisecond = static_cast<Int32>(offset_and_nano_of_millisecond);
    const auto sub_offset = static_cast<Int32>(offset_and_nano_of_millisecond >> 32);

    /// Paimon rejects anything outside this range when it constructs a `Timestamp`, so a value out
    /// of it means the bytes are not the timestamp we expect rather than an unusual timestamp.
    if (nano_of_millisecond < 0 || nano_of_millisecond > 999999)
        throw DB::Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Paimon timestamp of precision {} has nanoOfMillisecond {} outside of the range [0, 999999]",
            precision,
            nano_of_millisecond);

    if (sub_offset < 0)
        throw DB::Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Paimon timestamp of precision {} has negative variable-length offset {}",
            precision,
            sub_offset);

    const auto absolute_offset = static_cast<size_t>(offset()) + static_cast<size_t>(sub_offset);
    const auto row_size = reader.length();
    if (absolute_offset > static_cast<size_t>(std::numeric_limits<Int32>::max())
        || absolute_offset > row_size
        || row_size - absolute_offset < sizeof(Int64))
        throw DB::Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Paimon timestamp of precision {} requires {} bytes at variable-length offset {}, but the row has {} bytes",
            precision,
            sizeof(Int64),
            sub_offset,
            row_size);

    const Int64 millisecond = getFixedSizeDataAt<Int64>(static_cast<Int32>(absolute_offset));
    LOG_TEST(log, "sub_offset: {}, millisecond: {}, nano_of_millisecond: {}", sub_offset, millisecond, nano_of_millisecond);
    return Timestamp{.millisecond = millisecond, .nano_of_millisecond = nano_of_millisecond};
}

}
