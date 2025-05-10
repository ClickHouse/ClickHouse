#pragma once

#include <Functions/FunctionBaseXXConversion.h>

#include <Common/Base32.h>

namespace DB
{
struct Base32Traits
{
    static constexpr auto encodeName = "base32Encode";

    template<typename Col>
    static size_t getMaxEncodedSize(Col const& src_column)
    {
        auto const src_length = src_column.getChars().size();
        auto const string_count = src_column.size();
        /// Every 5 bytes becomes 8 bytes in base32
        /// Add padding for incomplete blocks and round up
        /// Plus one byte for null terminator for each string
        return ((src_length + 4) / 5 * 8 + 1) * string_count;
    }

    static size_t encode(const UInt8 * src, size_t src_length, UInt8 * dst) { return encodeBase32(src, src_length, dst); }
    static std::optional<size_t> decode(const UInt8 * src, size_t src_length, UInt8 * dst) { return decodeBase32(src, src_length, dst); }
};
}
