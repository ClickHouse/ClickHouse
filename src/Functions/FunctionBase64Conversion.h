#pragma once
#include "config.h"

#if USE_SIMDUTF
#    include <Columns/ColumnFixedString.h>
#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeString.h>
#    include <Functions/FunctionBaseXXConversion.h>
#    include <Interpreters/Context_fwd.h>
#    include <simdutf.h>

#    include <cstddef>
#    include <functional>
#    include <string>
#    include <string_view>

namespace DB
{

enum class Base64Variant : uint8_t
{
    Normal,
    URL
};

template<Base64Variant variant>
struct Base64EncodeTraits
{
    /// Base64 conversion is linear, so there is no size limit.
    static constexpr size_t max_input_size = 0;

    template<typename Col>
    static size_t getBufferSize(Col const& src_column)
    {
        auto const string_length = src_column.byteSize();
        auto const string_count = src_column.size();
        return ((string_length - string_count) / 3 + string_count) * 4 + string_count;
    }

    /// Base64 conversion is linear in the input length, so the cancellation callback is unused.
    static size_t perform(std::string_view src, UInt8 * dst, const std::function<void()> & = {})
    {
        /// simdutf emits the base64url alphabet ('-' and '_') without padding for the URL variant directly.
        constexpr auto options = (variant == Base64Variant::URL) ? simdutf::base64_url : simdutf::base64_default;
        return simdutf::binary_to_base64(src.data(), src.size(), reinterpret_cast<char *>(dst), options);
    }
};

template<Base64Variant variant>
struct Base64DecodeTraits
{
    static constexpr bool has_size_optimization = false;
    /// Base64 conversion is linear, so there is no size limit.
    static constexpr size_t max_input_size = 0;

    template<typename Col>
    static size_t getBufferSize(Col const& src_column)
    {
        auto const string_length = src_column.byteSize();
        auto const string_count = src_column.size();
        return ((string_length - string_count) / 4 + string_count) * 3 + string_count;
    }

    /// The whitespace characters simdutf ignores in base64 input (RFC 2045 transparency, minus vertical tab).
    static bool isIgnoredWhitespace(char c)
    {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f';
    }

    /// Base64 conversion is linear in the input length, so the cancellation callback is unused.
    static std::optional<size_t> perform(std::string_view src, UInt8 * dst, const std::function<void()> & = {})
    {
        /// The URL variant decodes the standard/base64url hybrid alphabet: the previous implementation only
        /// translated '-' and '_' before decoding and left '+' and '/' untouched, so it accepted both alphabets
        /// (e.g. base64URLDecode('+w==')). It is also defined to omit padding, so partial final chunks are valid.
        /// The standard variant requires a complete, padded final chunk and rejects truncated input such as the
        /// 3-character "foo".
        constexpr auto options = (variant == Base64Variant::URL) ? simdutf::base64_default_or_url : simdutf::base64_default;
        constexpr auto last_chunk = (variant == Base64Variant::URL) ? simdutf::loose : simdutf::strict;
        simdutf::result res
            = simdutf::base64_to_binary(src.data(), src.size(), reinterpret_cast<char *>(dst), options, last_chunk);
        if (res.error != simdutf::SUCCESS) [[unlikely]]
        {
            if constexpr (variant == Base64Variant::Normal)
            {
                /// The previous implementation (aklomp-base64) silently dropped non-zero leftover bits
                /// in a complete, properly padded final chunk, e.g. base64Decode('Zh==') = 'f'.
                /// simdutf::strict reports them as BASE64_EXTRA_BITS; retry in loose mode, which drops
                /// them. This cannot accept anything else that strict rejects: BASE64_EXTRA_BITS is
                /// only reported after the final chunk passed the completeness and padding checks.
                if (res.error == simdutf::BASE64_EXTRA_BITS)
                    res = simdutf::base64_to_binary(src.data(), src.size(), reinterpret_cast<char *>(dst), options, simdutf::loose);
            }
            else
            {
                /// The previous implementation padded the input with '=' to a multiple of four characters
                /// before decoding, so an underpadded final chunk was accepted, e.g. base64URLDecode('Zg=')
                /// was decoded as 'Zg=='. simdutf rejects it: in loose mode padding, if present, must
                /// complete the final chunk. Retry with the missing '=' appended. Only the "two significant
                /// characters plus one '='" form can be underpadded: one leftover character is invalid
                /// regardless of padding, and three leftover characters plus one '=' form a complete chunk.
                size_t significant = 0;
                size_t padding = 0;
                for (char c : src)
                {
                    if (c == '=')
                        ++padding;
                    else if (!isIgnoredWhitespace(c))
                        ++significant;
                }
                if (significant % 4 == 2 && padding == 1)
                {
                    std::string padded_src;
                    padded_src.reserve(src.size() + 1);
                    padded_src = src;
                    padded_src += '=';
                    res = simdutf::base64_to_binary(padded_src.data(), padded_src.size(), reinterpret_cast<char *>(dst), options, last_chunk);
                }
            }

            if (res.error != simdutf::SUCCESS)
                return std::nullopt;
        }

        return res.count;
    }
};

}

#endif
