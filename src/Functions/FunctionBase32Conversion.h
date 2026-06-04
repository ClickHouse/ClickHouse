#pragma once

#include <Functions/FunctionBaseXXConversion.h>

#include <Common/Base32.h>

#include <functional>

namespace DB
{
struct Base32EncodeTraits
{
    template <typename Col>
    static size_t getBufferSize(Col const & src_column)
    {
        auto const src_length = src_column.getChars().size();
        /// Every 5 bytes becomes 8 bytes in base32
        /// Add padding for incomplete blocks and round up
        return ((src_length + src_column.size() * 4) / 5) * 8;
    }

    /// Base32 conversion is linear in the input length, so the cancellation callback is unused.
    static size_t perform(std::string_view src, UInt8 * dst, const std::function<void()> & = {})
    {
        return encodeBase32(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst);
    }
};

struct Base32DecodeTraits
{
    static constexpr bool has_size_optimization = false;

    template <typename Col>
    static size_t getBufferSize(Col const & src_column)
    {
        auto const string_length = src_column.getChars().size();
        /// decoded size is at most length of encoded (every 8 bytes becomes at most 5 bytes)
        return (string_length * 5 + 7) / 8;
    }

    /// Base32 conversion is linear in the input length, so the cancellation callback is unused.
    static std::optional<size_t> perform(std::string_view src, UInt8 * dst, const std::function<void()> & = {})
    {
        return decodeBase32(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst);
    }
};
}
