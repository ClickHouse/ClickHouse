#include <cstdint>
#include <type_traits>
#include <alloca.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/BinaryRow.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Poco/BinaryReader.h>
#include "Interpreters/Context_fwd.h"
#include "base/Decimal.h"
#include "base/defines.h"
#include "base/types.h"
#include <Poco/ByteOrder.h>
#include <Poco/Logger.h>
#include "Common/Exception.h"
#include <bit>
#include <Common/logger_useful.h>

namespace Paimon
{

inline UInt64 flipBytes(UInt64 value)
{
#    if defined(POCO_HAVE_MSC_BYTESWAP)
    return _byteswap_uint64(value);
#    elif defined(POCO_HAVE_GCC_BYTESWAP)
    return __builtin_bswap64(value);
#    else
    UInt32 hi = UInt32(value >> 32);
    UInt32 lo = UInt32(value & 0xFFFFFFFF);
    return UInt64(flipBytes(hi)) | (UInt64(flipBytes(lo)) << 32);
#    endif
}

BinaryRow::BinaryRow(const String & bytes_): reader(bytes_), need_flip(!isLittleEndian())
{
    auto to_hex_string = [](const std::string& input) 
    {
        std::ostringstream oss;
        oss << std::hex << std::setfill('0');
        
        for (unsigned char c : input) {
            oss << std::setw(2) << static_cast<int>(c);
        }
        
        return oss.str();
    };

    // seek(LENGTH_SIZE);
    readIntBinary(arity, reader);
    /// arity stores as big endian
    if (!need_flip)
        arity = Poco::ByteOrder::flipBytes(arity);
    LOG_DEBUG(&Poco::Logger::get("BinaryRow"), "arity: {} need_flip: {} bytes_size: {} bytes: {}", arity, need_flip, reader.length(), to_hex_string(reader));
}

bool BinaryRow::isLittleEndian()
{
    return std::endian::native == std::endian::little;
}

bool BinaryRow::isNullAt(Int32 pos)
{
    chassert(pos >=0 && pos < arity);
    Int32 bits_index = HEADER_SIZE_IN_BITS + pos;
    seek(offset() + byteIndex(bits_index));
    Int8 res = 0;
    readIntBinary(res, reader);
    return (res & (1 << (bits_index & BIT_BYTE_INDEX_MASK))) != 0;
}

template<typename T> T BinaryRow::getFixedSizeData(Int32 pos)
{
    chassert(pos >=0 && pos < arity);
    seek(getFieldOffset(pos));
    T t;
    readIntBinary(t, reader);
    LOG_DEBUG(&Poco::Logger::get("BinaryRow"), "read value: {}, need_flip: {}", t, need_flip);
    
    if (need_flip)
    {
        if constexpr (std::is_same_v<Int64, T> || std::is_same_v<UInt64, T>)
        {
            t = static_cast<T>(flipBytes(static_cast<UInt64>(t)));
        }
        else 
        {
            t = Poco::ByteOrder::flipBytes(t);
        }
    }
    return t;
}

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
    return getFixedSizeData<Int32>(pos);
}

Float32 BinaryRow::getFloat(Int32 pos)
{
    Int32 res = getFixedSizeData<Int32>(pos);
    auto * float_res = reinterpret_cast<Float32*>(&res);
    return *float_res;
}

Float64 BinaryRow::getDouble(Int32 pos)
{
    Int64 res = getFixedSizeData<Int64>(pos);
    auto * float_res = reinterpret_cast<Float64*>(&res);
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
    chassert(pos >=0 && pos < arity);
    Int32 field_offset = getFieldOffset(pos);
    Int64 offset_and_len = static_cast<Int64>(getFixedSizeData<UInt64>(pos));
    Int64 mark = offset_and_len & HIGHEST_FIRST_BIT;
    if (mark == 0) 
    {
        Int32 sub_offset = static_cast<Int32>((offset_and_len >> 32));
        Int32 len = static_cast<Int32>(offset_and_len);
        LOG_DEBUG(&Poco::Logger::get("BinaryRow"), "sub_offset: {}, len: {}, offset_and_len: {}",sub_offset, len,  offset_and_len);
        return copyBytes(offset() + sub_offset, len);
    } else {
        Int32 len = static_cast<Int32>(static_cast<UInt64>((offset_and_len & HIGHEST_SECOND_TO_EIGHTH_BIT)) >> 56);
        LOG_DEBUG(&Poco::Logger::get("BinaryRow"), "mark: {}, len: {}, offset_and_len: {}",mark, len,  offset_and_len);
        if (isLittleEndian())
        {
            return copyBytes(field_offset, len);
        }
        else
        {
            return copyBytes(field_offset + 1, len);
        }
        // if (LITTLE_ENDIAN) {
        //     // return BinaryString.fromAddress(segments, fieldOffset, len);
        // } else {
        //     // fieldOffset + 1 to skip header.
        //     return BinaryString.fromAddress(segments, fieldOffset + 1, len);
        // }
    }
}

String BinaryRow::getBinary(Int32 pos)
{
    return getString(pos);
}

// Decimal32 BinaryRow::getDecimal32(Int32 pos, Int32 precision, Int32 )
// {
//     chassert(pos >=0 && pos < arity);
//     chassert(precision >0 && precision <= 9);
//     Int64 value = getFixedSizeData<Int64>(pos);
//     return Decimal<Int32>(static_cast<Int32>(value));
// }

// Decimal64 BinaryRow::getDecimal64(Int32 pos, Int32 precision, Int32 )
// {
//     chassert(pos >=0 && pos < arity);
//     chassert(precision >9 && precision <= 18);
//     Int64 value = getFixedSizeData<Int64>(pos);
//     return Decimal<Int64>(value);
// }

// String addLeadingZero(const String & data, size_t target_size)
// {
//     if (data.size() == target_size)
//         return data;
//     else if (data.size() > target_size)
//         throw Exception();
//     String result(target_size, 0);
//     size_t start_pos = target_size - data.size();
//     for (size_t i = 0; i < data.size(); ++i)
//         result[start_pos + i] = data[i];
//     return result;
// }

// UInt64 getUint64BigEndian(const char * bytes)
// {
//     return (static_cast<UInt64>(bytes[0]) << 56) |
//             (static_cast<UInt64>(bytes[1]) << 48) |
//             (static_cast<UInt64>(bytes[2]) << 40) |
//             (static_cast<UInt64>(bytes[3]) << 32) |
//             (static_cast<UInt64>(bytes[4]) << 24) |
//             (static_cast<UInt64>(bytes[5]) << 16) |
//             (static_cast<UInt64>(bytes[6]) << 8)  |
//             (static_cast<UInt64>(bytes[7]));
// }

// Decimal128 BinaryRow::getDecimal128(Int32 pos, Int32 precision, Int32 )
// {
//     chassert(pos >=0 && pos < arity);
//     chassert(precision >18 && precision <= 38);
//     int field_offset = getFieldOffset(pos);
//     Int64 offset_and_size = getFixedSizeData<Int64>(field_offset);
//     Int32 size = static_cast<Int32>(offset_and_size);
//     Int32 sub_offset = static_cast<Int32>(offset_and_size >> 32);
//     String bytes_string = copyBytes(offset() + sub_offset, size);
//     if (bytes_string.length() > 16)
//         throw Exception();
//     bytes_string = addLeadingZero(bytes_string, 16);
//     UInt64 high = getUint64BigEndian(bytes_string.data());
//     UInt64 low = getUint64BigEndian(bytes_string.data() + 8);
//     Int128 value({low, high});
//     return Decimal<Int128>(value);
// }

// Decimal256 BinaryRow::getDecimal256(Int32 pos, Int32 precision, Int32 )
// {
//     chassert(pos >=0 && pos < arity);
//     chassert(precision >38 && precision <= 76);
//     int field_offset = getFieldOffset(pos);
//     Int64 offset_and_size = getFixedSizeData<Int64>(field_offset);
//     Int32 size = static_cast<Int32>(offset_and_size);
//     Int32 sub_offset = static_cast<Int32>(offset_and_size >> 32);
//     String bytes_string = copyBytes(offset() + sub_offset, size);
//     if (bytes_string.length() > 32)
//             throw Exception();
//         bytes_string = addLeadingZero(bytes_string, 32);
//         UInt64 ele1 = getUint64BigEndian(bytes_string.data());
//         UInt64 ele2 = getUint64BigEndian(bytes_string.data() + 8);
//         UInt64 ele3 = getUint64BigEndian(bytes_string.data() + 16);
//         UInt64 ele4 = getUint64BigEndian(bytes_string.data() + 24);
//         Int256 value({ele4, ele3, ele2, ele1});
//         return Decimal<Int256>(value);
// }

// template<typename T>
// Decimal<T> BinaryRow::getDecimal(Int32 pos, Int32 precision, Int32 )
// {
//     chassert(pos >=0 && pos < arity);
//     chassert(precision >0 && precision <= 76);
//     if (precision <= 18)
//     {
//         Int64 value = getFixedSizeData<Int64>(pos);
//         return Decimal<T>(static_cast<T>(value));
//     }
//     else
//     {
//         int field_offset = getFieldOffset(pos);
//         Int64 offset_and_size = getFixedSizeData<Int64>(field_offset);
//         Int32 size = static_cast<Int32>(offset_and_size);
//         Int32 sub_offset = static_cast<Int32>(offset_and_size >> 32);
//         String bytes_string = copyBytes(offset() + sub_offset, size);
//         if (bytes_string.length() > 32)
//                 throw Exception();
//         auto add_leading_zero = [](const String & data, size_t target_size)
//         {
//             if (data.size() == target_size)
//                 return data.data();
//             else if (data.size() > target_size)
//                 throw Exception();
//             String result(target_size, 0);
//             size_t start_pos = target_size - data.size();
//             for (size_t i = 0; i < data.size(); ++i)
//                 result[start_pos + i] = data[i];
//             return result;
//         };
//         auto get_uint64_big_endian = [] (const char * bytes)
//         {
//             return (static_cast<UInt64>(bytes[0]) << 56) |
//                     (static_cast<UInt64>(bytes[1]) << 48) |
//                     (static_cast<UInt64>(bytes[2]) << 40) |
//                     (static_cast<UInt64>(bytes[3]) << 32) |
//                     (static_cast<UInt64>(bytes[4]) << 24) |
//                     (static_cast<UInt64>(bytes[5]) << 16) |
//                     (static_cast<UInt64>(bytes[6]) << 8)  |
//                     (static_cast<UInt64>(bytes[7]));
//         };
//         if (precision <= 38)
//         {
//             if (bytes_string.length() > 16)
//                 throw Exception();
//             bytes_string = add_leading_zero(bytes_string, 16);
//             UInt64 high = get_uint64_big_endian(bytes_string.data());
//             UInt64 low = get_uint64_big_endian(bytes_string.data() + 8);
//             Int128 value({low, high});
//             return Decimal<T>(value);
//         }
//         else
//         {
//             if (bytes_string.length() > 32)
//                 throw Exception();
//             bytes_string = add_leading_zero(bytes_string, 32);
//             UInt64 ele1 = get_uint64_big_endian(bytes_string.data());
//             UInt64 ele2 = get_uint64_big_endian(bytes_string.data() + 8);
//             UInt64 ele3 = get_uint64_big_endian(bytes_string.data() + 16);
//             UInt64 ele4 = get_uint64_big_endian(bytes_string.data() + 24);
//             Int256 value({ele4, ele3, ele2, ele1});
//             return Decimal<T>(value);
//         }
//     }
// }

DateTime64 BinaryRow::getTimestamp(Int32 pos, Int32 scale)
{
    if (scale <= 3)
    {
        // auto res = getFixedSizeData<Int64>(pos);
        return DateTime64(getFixedSizeData<Int64>(pos));
    }
    /// TODO: support larger precision
    throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "scale {} is not supported, only support scale <= 3", scale);
}

Array BinaryRow::getArray(Int32 )
{
    throw Exception();
}

Map BinaryRow::getMap(Int32 )
{
    throw Exception();
}

}
