#pragma once

#include <Functions/FunctionBaseXXConversion.h>

#include <Common/Base58.h>

#include <functional>

namespace DB
{

/// The generic (variable-length) base58 conversion is quadratic in the input length, so it can be
/// extremely slow on large values. base58 is meant for short data (keys, hashes, addresses), so we
/// reject oversized inputs instead of letting a single value run for minutes. 10 KB allows e.g.
/// 32/64-byte keys with plenty of margin.
///
/// This is only the compile-time default that marks base58 as size-limited; the effective limit is the
/// runtime setting `function_base58_max_input_size` (default 10 KB, `0` disables it). It is enforced by
/// FunctionBaseXXConversion, which is gated on `Traits::max_input_size != 0` (so the linear base32/base64,
/// whose `max_input_size` is 0, are never limited).
static constexpr size_t MAX_BASE58_INPUT_SIZE = 10000;

struct Base58EncodeTraits
{
    static constexpr size_t max_input_size = MAX_BASE58_INPUT_SIZE;

    template <typename Col>
    static size_t getBufferSize(Col const & src_column)
    {
        auto const src_length = src_column.getChars().size();
        /// Base58 has efficiency of 73% (8/11) [https://monerodocs.org/cryptography/base58/],
        /// and we take double scale to avoid any reallocation.
        constexpr auto oversize = 2;
        return static_cast<size_t>(ceil(oversize * src_length + 1));
    }

    static size_t perform(std::string_view src, UInt8 * dst, const std::function<void()> & check_cancellation = {})
    {
        if (src.size() == 32)
            return encodeBase58_32(reinterpret_cast<const UInt8 *>(src.data()), dst);
        else if (src.size() == 64)
            return encodeBase58_64(reinterpret_cast<const UInt8 *>(src.data()), dst);
        else
            return encodeBase58(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst, check_cancellation);
    }
};

struct Base58DecodeTraits
{
    static constexpr bool has_size_optimization = true;
    static constexpr size_t max_input_size = MAX_BASE58_INPUT_SIZE;

    template <typename Col>
    static size_t getBufferSize(Col const & src_column)
    {
        /// According to the RFC https://datatracker.ietf.org/doc/html/draft-msporny-base58-03
        /// base58 doesn't have a clean bitsequence-to-character mapping like base32 or base64.
        /// Instead, it uses division by 58 and modulo operations on big integers.
        /// In addition all the leading zeros are converted to "1"s as is.
        /// Thus, if we decode the can have at most same amount of bytes as a result.
        /// Example:
        /// "11111" (5 chars) -> b'\x00\x00\x00\x00\x00' (5 bytes)
        return src_column.getChars().size();
    }

    static std::optional<size_t> perform(std::string_view src, UInt8 * dst, const std::function<void()> & check_cancellation = {})
    {
        return decodeBase58(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst, check_cancellation);
    }

    /// The size argument is a requirement on the decoded size at every value, not only at the two the
    /// removed fixed-size decoders served. Zero means no requirement and never reaches this function.
    static std::optional<size_t> performWithSizeHint(std::string_view src, UInt8 * dst, size_t expected_size, const std::function<void()> & check_cancellation = {})
    {
        /// An `n`-byte value encodes to between `n` and `maxBase58EncodedLength(n)` characters, so an input
        /// outside that window cannot decode to `expected_size` and is rejected without paying the quadratic
        /// conversion. The first comparison also bounds `expected_size` by the input length, which is what
        /// keeps the product in the second one from overflowing.
        if (src.size() < expected_size || src.size() > maxBase58EncodedLength(expected_size))
            return {};

        const std::optional<size_t> decoded_size
            = decodeBase58(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst, check_cancellation);
        if (decoded_size && *decoded_size != expected_size)
            return {};
        return decoded_size;
    }
};
}
