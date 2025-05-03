#pragma once

#include <Functions/FunctionBaseXXConversion.h>

#include <Common/Base32.h>

namespace DB
{
struct Base32Traits
{
    static constexpr auto encodeName = "base32Encode";
    /// Base32 has efficiency of 62.5% (5/8)
    /// and we take double scale to avoid any reallocation.
    static constexpr auto oversize = 2;

    static size_t encode(const UInt8 * src, size_t src_length, UInt8 * dst) { return encodeBase32(src, src_length, dst); }
    static std::optional<size_t> decode(const UInt8 * src, size_t src_length, UInt8 * dst) { return decodeBase32(src, src_length, dst); }
};
}
