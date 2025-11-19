#pragma once

#include <Functions/FunctionBaseXXConversion.h>

#include <Common/Base58.h>

namespace DB
{
struct Base58EncodeTraits
{
    template <typename Col>
    static size_t getBufferSize(Col const & src_column)
    {
        auto const src_length = src_column.getChars().size();
        /// Base58 has efficiency of 73% (8/11) [https://monerodocs.org/cryptography/base58/],
        /// and we take double scale to avoid any reallocation.
        constexpr auto oversize = 2;
        return static_cast<size_t>(ceil(oversize * src_length + 1));
    }

    static size_t perform(std::string_view src, UInt8 * dst)
    {
        return encodeBase58(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst);
    }
};

struct Base58DecodeTraits
{
    template <typename Col>
    static size_t getBufferSize(Col const & src_column)
    {
        /// This function can be used for FixedString columns so we need to take into account NULL terminator
        auto const string_length = src_column.getChars().size() + src_column.size();
        /// decoded size is at most length of encoded (every 8 bytes becomes at most 6 bytes)
        return (string_length * 6 + 7) / 8;
    }

    static std::optional<size_t> perform(std::string_view src, UInt8 * dst)
    {
        return decodeBase58(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst);
    }
};
}
