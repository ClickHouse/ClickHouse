#pragma once

#include <Functions/FunctionBaseXXConversion.h>

#include <Common/Base32.h>

namespace DB
{
struct Base32Traits
{
    static constexpr auto encodeName = "base32Encode";

    static size_t getMaxEncodedSize(size_t src_length)
    {
        /// Base32 has efficiency of 62.5% (5/8)
        /// and we take double scale to avoid any reallocation.
        /// Also, at least 8 bytes are needed for the result.
        constexpr auto oversize = 2;
        return std::max<size_t>(static_cast<size_t>(ceil(oversize * src_length + 1)), 8);
    }

    static size_t encode(const UInt8 * src, size_t src_length, UInt8 * dst) { return encodeBase32(src, src_length, dst); }
    static std::optional<size_t> decode(const UInt8 * src, size_t src_length, UInt8 * dst) { return decodeBase32(src, src_length, dst); }
};
}
