#pragma once
#include "config.h"

#if USE_BASE64
#    include <base/MemorySanitizer.h>
#    include <Columns/ColumnFixedString.h>
#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeString.h>
#    include <Functions/FunctionBaseXXConversion.h>
#    include <Interpreters/Context_fwd.h>
#    include <simdutf.h>

#    include <cstddef>
#    include <functional>
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
        const size_t outlen = simdutf::binary_to_base64(src.data(), src.size(), reinterpret_cast<char *>(dst), options);

        /// simdutf may use AVX-512 with some shuffle operations.
        /// Memory sanitizer doesn't understand if there was uninitialized memory in SIMD register but it was not used in the result of shuffle.
        __msan_unpoison(dst, outlen);

        return outlen;
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

    /// Base64 conversion is linear in the input length, so the cancellation callback is unused.
    static std::optional<size_t> perform(std::string_view src, UInt8 * dst, const std::function<void()> & = {})
    {
        constexpr auto options = (variant == Base64Variant::URL) ? simdutf::base64_url : simdutf::base64_default;
        const simdutf::result res = simdutf::base64_to_binary(src.data(), src.size(), reinterpret_cast<char *>(dst), options);
        if (res.error != simdutf::SUCCESS) [[unlikely]]
            return std::nullopt;

        __msan_unpoison(dst, res.count);
        return res.count;
    }
};

}

#endif
