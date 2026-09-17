#pragma once
#include "config.h"

#if USE_BASE64
#    include <base/MemorySanitizer.h>
#    include <Columns/ColumnFixedString.h>
#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeString.h>
#    include <Functions/FunctionBaseXXConversion.h>
#    include <Interpreters/Context_fwd.h>
#    include <libbase64.h>

#    include <cstddef>
#    include <string_view>

namespace DB
{

enum class Base64Variant : uint8_t
{
    Normal,
    URL
};

inline std::string preprocessBase64URL(std::string_view src)
{
    std::string padded_src;
    padded_src.reserve(src.size() + 3);

    // Do symbol substitution as described in https://datatracker.ietf.org/doc/html/rfc4648#section-5
    for (auto s : src)
    {
        switch (s)
        {
        case '_':
            padded_src += '/';
            break;
        case '-':
            padded_src += '+';
            break;
        default:
            padded_src += s;
            break;
        }
    }

    /// Insert padding to please aklomp library
    size_t remainder = src.size() % 4;
    switch (remainder)
    {
        case 0:
            break; // no padding needed
        case 1:
            padded_src.append("==="); // this case is impossible to occur with valid base64-URL encoded input, however, we'll insert padding anyway
            break;
        case 2:
            padded_src.append("=="); // two bytes padding
            break;
        default: // remainder == 3
            padded_src.append("="); // one byte padding
            break;
    }

    return padded_src;
}

inline size_t postprocessBase64URL(UInt8 * dst, size_t out_len)
{
    // Do symbol substitution as described in https://datatracker.ietf.org/doc/html/rfc4648#section-5
    for (size_t i = 0; i < out_len; ++i)
    {
        switch (dst[i])
        {
        case '/':
            dst[i] = '_';
            break;
        case '+':
            dst[i] = '-';
            break;
        case '=': // stop when padding is detected
            return i;
        default:
            break;
        }
    }
    return out_len;
}


template<Base64Variant variant>
struct Base64EncodeTraits
{
    template<typename Col>
    static size_t getBufferSize(Col const& src_column)
    {
        auto const string_length = src_column.byteSize();
        auto const string_count = src_column.size();
        return ((string_length - string_count) / 3 + string_count) * 4 + string_count;
    }

    static size_t perform(std::string_view src, UInt8 * dst)
    {
        size_t outlen = 0;
        base64_encode(src.data(), src.size(), reinterpret_cast<char *>(dst), &outlen, 0);

        /// Base64 library is using AVX-512 with some shuffle operations.
        /// Memory sanitizer doesn't understand if there was uninitialized memory in SIMD register but it was not used in the result of shuffle.
        __msan_unpoison(dst, outlen);

        if constexpr (variant == Base64Variant::URL)
            outlen = postprocessBase64URL(dst, outlen);

        return outlen;
    }
};

template<Base64Variant variant>
struct Base64DecodeTraits
{
    template<typename Col>
    static size_t getBufferSize(Col const& src_column)
    {
        auto const string_length = src_column.byteSize();
        auto const string_count = src_column.size();
        return ((string_length - string_count) / 4 + string_count) * 3 + string_count;
    }

    static std::optional<size_t> perform(std::string_view src, UInt8 * dst)
    {
        int rc;
        size_t outlen = 0;
        if constexpr (variant == Base64Variant::URL)
        {
            std::string src_padded = preprocessBase64URL(src);
            rc = base64_decode(src_padded.data(), src_padded.size(), reinterpret_cast<char *>(dst), &outlen, 0);
        }
        else
        {
            rc = base64_decode(src.data(), src.size(), reinterpret_cast<char *>(dst), &outlen, 0);
        }
        if (rc != 1) [[unlikely]]
            return std::nullopt;
        return outlen;
    }
};

}

#endif
