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
        auto const src_length = src_column.getChars().size() + src_column.size();
        /// Every 5 bytes becomes 8 bytes in base32
        /// Add padding for incomplete blocks and round up
        return (src_length + 4) / 5 * 8;
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
        /// This function can be used for FixedString columns so we need to take into account NULL terminator
        auto const string_length = src_column.getChars().size() + src_column.size();
        /// decoded size is at most length of encoded (every 8 bytes becomes at most 5 bytes)
        return (string_length * 5 + 7) / 8;
    }

    static std::optional<size_t> perform(std::string_view src, UInt8 * dst)
    {
        return decodeBase32(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst);
    }
};
}
