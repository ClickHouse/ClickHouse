#pragma once

#include <Functions/FunctionBaseXXConversion.h>

#include <Core/Settings.h>
#include <Common/Base62.h>

#include <functional>

namespace DB
{

namespace Setting
{
extern const SettingsUInt64 function_base62_max_input_size;
}

/// The base62 conversion is quadratic in the input length, so it can be extremely slow on large
/// values. base62 is meant for short data (identifiers, keys, hashes), so we reject oversized
/// inputs instead of letting a single value run for minutes. 10 KB allows e.g. 32/64-byte keys
/// with plenty of margin.
///
/// This is only the compile-time default that marks base62 as size-limited; the effective limit is the
/// runtime setting `function_base62_max_input_size` (default 10 KB, `0` disables it). It is enforced by
/// FunctionBaseXXConversion, which is gated on `Traits::max_input_size != 0` (so the linear base32/base64,
/// whose `max_input_size` is 0, are never limited).
static constexpr size_t MAX_BASE62_INPUT_SIZE = 10000;

struct Base62EncodeTraits
{
    static constexpr size_t max_input_size = MAX_BASE62_INPUT_SIZE;

    static size_t maxInputSize(const Settings & settings) { return settings[Setting::function_base62_max_input_size]; }

    template <typename Col>
    static size_t getBufferSize(Col const & src_column)
    {
        auto const src_length = src_column.getChars().size();
        /// Base62 encodes log2(62) ≈ 5.95 bits per character, so the output is at most
        /// ~1.35 times longer than the input; we take double scale to avoid any reallocation.
        constexpr auto oversize = 2;
        return static_cast<size_t>(ceil(oversize * src_length + 1));
    }

    static size_t perform(std::string_view src, UInt8 * dst, const std::function<void()> & check_cancellation = {})
    {
        return encodeBase62(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst, check_cancellation);
    }
};

struct Base62DecodeTraits
{
    static constexpr bool has_size_optimization = false;
    static constexpr size_t max_input_size = MAX_BASE62_INPUT_SIZE;

    static size_t maxInputSize(const Settings & settings) { return settings[Setting::function_base62_max_input_size]; }

    template <typename Col>
    static size_t getBufferSize(Col const & src_column)
    {
        /// Like base58, base62 doesn't have a clean bitsequence-to-character mapping.
        /// Instead, it uses division by 62 and modulo operations on big integers.
        /// In addition all the leading zero bytes are converted to "0"s as is.
        /// Thus, the decoded result can have at most the same amount of bytes as the input.
        /// Example:
        /// "00000" (5 chars) -> b'\x00\x00\x00\x00\x00' (5 bytes)
        return src_column.getChars().size();
    }

    static std::optional<size_t> perform(std::string_view src, UInt8 * dst, const std::function<void()> & check_cancellation = {})
    {
        return decodeBase62(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst, check_cancellation);
    }
};
}
