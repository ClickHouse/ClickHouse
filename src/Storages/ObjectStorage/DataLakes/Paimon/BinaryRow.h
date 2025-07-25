#pragma once

#include <cstddef>
#include <memory>
#include <string_view>
#include <base/types.h>
#include <Poco/BinaryReader.h>
#include <Poco/Logger.h>
#include "Common/logger_useful.h"
#include "Core/Field.h"
#include "IO/ReadBufferFromString.h"
#include <IO/ReadBufferFromIStream.h>
#include <fmt/ranges.h>
#include "base/Decimal.h"

namespace Paimon
{
using namespace DB;
class BinaryRow
{
public:
    using BinaryReaderPtr = std::shared_ptr<Poco::BinaryReader>;
    explicit BinaryRow(const String & bytes_);

    bool isNullAt(Int32 pos);

    bool getBoolean(Int32 pos);

    Int8 getByte(Int32 pos);

    Int16 getShort(Int32 pos);

    Int32 getInt(Int32 pos);

    Int64 getLong(Int32 pos);

    Float32 getFloat(Int32 pos);

    Float64 getDouble(Int32 pos);

    String getString(Int32 pos);

    String getBinary(Int32 pos);

    // Decimal32 getDecimal32(Int32 pos, Int32 precision, Int32 scale);
    // Decimal64 getDecimal64(Int32 pos, Int32 precision, Int32 scale);
    // Decimal128 getDecimal128(Int32 pos, Int32 precision, Int32 scale);
    // Decimal256 getDecimal256(Int32 pos, Int32 precision, Int32 scale);
    template<typename T>
    Decimal<T> getDecimal(Int32 pos, Int32 precision, Int32 /* scale */)
    {
        chassert(pos >=0 && pos < arity);
        chassert(precision >0 && precision <= 76);
        if (precision <= 18)
        {
            Int64 value = getFixedSizeData<Int64>(pos);
            if (precision <= 9) 
            {
                return Decimal<Int32>(static_cast<Int32>(value));
            }
            return Decimal<Int64>(static_cast<Int64>(value));
        }
        else
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
            // int field_offset = getFieldOffset(pos);
            Int64 offset_and_size = getFixedSizeData<Int64>(pos);
            Int32 size = static_cast<Int32>(offset_and_size);
            Int32 sub_offset = static_cast<Int32>(offset_and_size >> 32);
            String bytes_string = copyBytes(offset() + sub_offset, size);
            LOG_DEBUG(&Poco::Logger::get("BinaryRow"), "bytes_string: {}", to_hex_string(bytes_string));
            if (bytes_string.length() > 32)
                    throw Exception();
            auto add_leading_zero = [](const String & data, size_t target_size)
            {
                if (data.size() == target_size)
                    return data;
                else if (data.size() > target_size)
                    throw Exception();
                String result(target_size, 0);
                size_t start_pos = target_size - data.size();
                for (size_t i = 0; i < data.size(); ++i)
                    result[start_pos + i] = data[i];
                return result;
            };
            auto get_uint64_big_endian = [] (std::string_view bytes)
            {
                uint64_t result = 0;
                for (size_t i = 0; i < 8; ++i) 
                {
                    result = (result << 8) | static_cast<UInt8>(bytes[i]);
                }
                return result;
            };
            if (precision <= 38)
            {
                if (bytes_string.length() > 16)
                    throw Exception();
                bytes_string = add_leading_zero(bytes_string, 16);
                UInt64 high = get_uint64_big_endian(bytes_string);
                UInt64 low = get_uint64_big_endian(bytes_string.substr(8));
                Int128 value({low, high});
                return Decimal<Int128>(value);
            }
            else
            {
                if (bytes_string.length() > 32)
                    throw Exception();
                bytes_string = add_leading_zero(bytes_string, 32);
                UInt64 ele1 = get_uint64_big_endian(bytes_string);
                UInt64 ele2 = get_uint64_big_endian(bytes_string.substr(8));
                UInt64 ele3 = get_uint64_big_endian(bytes_string.substr(16));
                UInt64 ele4 = get_uint64_big_endian(bytes_string.substr(24));
                Int256 value({ele4, ele3, ele2, ele1});
                return Decimal<Int256>(value);
            }
        }
    }

    DateTime64 getTimestamp(Int32 pos, Int32 scale);

    Array getArray(Int32 pos);

    Map getMap(Int32 pos);

private:
    // String bytes;
    ReadBufferFromOwnString reader;
    bool need_flip;
    Int32 arity;
    // std::istringstream bytes_stream;
    // BinaryReaderPtr binary_reader;
    // const static Int32 LENGTH_SIZE{4};
    const static Int32 ARITY_SIZE{4};
    const static Int32 HEADER_SIZE_IN_BITS{8};
    const static Int32 ADDRESS_BITS_PER_WORD{3};
    const static Int32 BIT_BYTE_INDEX_MASK{7};
    const static Int64 HIGHEST_FIRST_BIT = 0x80L << 56;
    const static Int64 HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7FL << 56;
    String tmp_value;

    Int32 calculateBitSetWidthInBytes() const { return ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8; }
    Int32 offset() const { return ARITY_SIZE;}
    Int32 getFieldOffset(Int32 pos) const 
    { 
        Int32 res = offset() + calculateBitSetWidthInBytes() + pos * 8;
        LOG_DEBUG(&Poco::Logger::get("BinaryRow"), "pos: {}, offset: {}", pos, res);
        return res; 
    }
    Int32 byteIndex(Int32 bit_index) const { return bit_index >> ADDRESS_BITS_PER_WORD; }
    void seek(Int32 offset) { reader.seek(offset, SEEK_SET); }
    static bool isLittleEndian();
    template<typename T> T getFixedSizeData(Int32 pos);
    String copyBytes(Int32 offset, Int32 num_bytes);
};

}
