#pragma once

#include <Functions/FunctionBaseXXConversion.h>

#include <Common/Base58.h>

namespace DB
{
struct Base58Traits
{
    static constexpr auto encodeName = "base58Encode";

    template<typename Col>
    static size_t getMaxEncodedSize(Col const& src_column)
    {
        auto const src_length = src_column.getChars().size();
        /// Base58 has efficiency of 73% (8/11) [https://monerodocs.org/cryptography/base58/],
        /// and we take double scale to avoid any reallocation.
        constexpr auto oversize = 2;
        return static_cast<size_t>(ceil(oversize * src_length + 1));
    }

    static size_t encode(const UInt8 * src, size_t src_length, UInt8 * dst) { return encodeBase58(src, src_length, dst); }
    static std::optional<size_t> decode(const UInt8 * src, size_t src_length, UInt8 * dst) { return decodeBase58(src, src_length, dst); }
};
}
