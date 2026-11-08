#pragma once

#include "config.h"

#if USE_ICU

#    include <Columns/ColumnString.h>
#    include <Functions/LowerUpperImpl.h>
#    include <base/scope_guard.h>
#    include <unicode/ucasemap.h>
#    include <unicode/unistr.h>
#    include <unicode/urename.h>
#    include <unicode/utypes.h>
#    include <Common/StringUtils.h>

#    include <algorithm>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

template <char not_case_lower_bound, char not_case_upper_bound, bool upper>
struct LowerUpperUTF8Impl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        if (input_rows_count == 0)
            return;

        bool all_ascii = isAllASCII(data.data(), data.size());
        if (all_ascii)
        {
            LowerUpperImpl<not_case_lower_bound, not_case_upper_bound>::vector(data, offsets, res_data, res_offsets, input_rows_count);
            return;
        }

        res_data.resize(data.size());
        res_offsets.resize_exact(input_rows_count);

        UErrorCode error_code = U_ZERO_ERROR;
        UCaseMap * case_map = ucasemap_open("", U_FOLD_CASE_DEFAULT, &error_code);
        if (U_FAILURE(error_code))
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Error calling ucasemap_open: {}", u_errorName(error_code));

        SCOPE_EXIT(
        {
            ucasemap_close(case_map);
        });

        size_t curr_offset = 0;
        for (size_t row_i = 0; row_i < input_rows_count; ++row_i)
        {
            const auto * src = reinterpret_cast<const char *>(&data[offsets[row_i - 1]]);
            size_t src_size = offsets[row_i] - offsets[row_i - 1];

            /// ICU APIs accept `int32_t` for buffer sizes and return the required output
            /// length as `int32_t` on `U_BUFFER_OVERFLOW_ERROR`. Unicode full case mapping
            /// (Unicode `SpecialCasing.txt`, e.g. `U+0390` maps to 3 code points / 6 bytes
            /// from a 2-byte input) expands UTF-8 output by at most 3x. Reject inputs
            /// whose worst-case case-mapped output could exceed `INT32_MAX` — the retry
            /// path could otherwise receive an overflowed `dst_size` and corrupt `res_data`.
            if (static_cast<int64_t>(src_size) * 3 > INT32_MAX)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "String size {} exceeds the maximum supported length for {}: "
                    "case mapping could produce output larger than the 2 GiB ICU API limit",
                    src_size,
                    upper ? "upperUTF8" : "lowerUTF8");

            /// `res_data` accumulates output for all rows and may exceed `INT32_MAX`. Cap
            /// the destination capacity passed to ICU; the `U_BUFFER_OVERFLOW_ERROR` retry
            /// path enlarges `res_data` to fit and the guard above keeps the per-row
            /// requested length representable as `int32_t`.
            auto safe_dest_capacity = static_cast<int32_t>(std::min<size_t>(res_data.size() - curr_offset, INT32_MAX));
            auto safe_src_size = static_cast<int32_t>(src_size);

            int32_t dst_size;
            if constexpr (upper)
                dst_size = ucasemap_utf8ToUpper(
                    case_map,
                    reinterpret_cast<char *>(&res_data[curr_offset]),
                    safe_dest_capacity,
                    src,
                    safe_src_size,
                    &error_code);
            else
                dst_size = ucasemap_utf8ToLower(
                    case_map,
                    reinterpret_cast<char *>(&res_data[curr_offset]),
                    safe_dest_capacity,
                    src,
                    safe_src_size,
                    &error_code);

            if (error_code == U_BUFFER_OVERFLOW_ERROR)
            {
                size_t new_size = curr_offset + dst_size;
                res_data.resize(new_size);

                safe_dest_capacity = static_cast<int32_t>(std::min<size_t>(res_data.size() - curr_offset, INT32_MAX));

                error_code = U_ZERO_ERROR;
                if constexpr (upper)
                    dst_size = ucasemap_utf8ToUpper(
                        case_map,
                        reinterpret_cast<char *>(&res_data[curr_offset]),
                        safe_dest_capacity,
                        src,
                        safe_src_size,
                        &error_code);
                else
                    dst_size = ucasemap_utf8ToLower(
                        case_map,
                        reinterpret_cast<char *>(&res_data[curr_offset]),
                        safe_dest_capacity,
                        src,
                        safe_src_size,
                        &error_code);
            }

            if (error_code != U_ZERO_ERROR && error_code != U_STRING_NOT_TERMINATED_WARNING)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Error calling {}: {} input: {} input_size: {}",
                    upper ? "ucasemap_utf8ToUpper" : "ucasemap_utf8ToLower",
                    u_errorName(error_code),
                    std::string_view(src, src_size),
                    src_size);

            curr_offset += dst_size;
            res_offsets[row_i] = curr_offset;
        }

        res_data.resize(curr_offset);
    }

    static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Functions lowerUTF8 and upperUTF8 cannot work with FixedString argument");
    }
};

}

#endif
