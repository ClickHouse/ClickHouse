#pragma once

#include <Functions/FunctionBaseXXConversion.h>

#include <Common/Base32.h>

namespace DB
{
struct Base32EncodeTraits
{
    template <typename Col>
    static size_t getBufferSize(Col const & src_column)
    {
        auto const src_length = src_column.getChars().size();
        auto const string_count = src_column.size();
        /// Every 5 bytes becomes 8 bytes in base32
        /// Add padding for incomplete blocks and round up
        /// Plus one byte for null terminator for each string
        return ((src_length + 4) / 5 * 8 + 1) * string_count;
    }

    static size_t perform(std::string_view src, UInt8 * dst)
    {
        return encodeBase32(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst);
    }
};

struct Base32DecodeTraits
{
    template <typename Col>
    static size_t getBufferSize(Col const & src_column)
    {
        auto const string_length = src_column.byteSize();
        auto const string_count = src_column.size();
        /// decoded size is at most length of encoded (every 8 bytes becomes at most 5 bytes)
        /// plus one byte for null terminator for each string
        return ((string_length * 5 + 7) / 8 + 1) * string_count;
    }

    static std::optional<size_t> perform(std::string_view src, UInt8 * dst)
    {
        return decodeBase32(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst);
    }
};
}
