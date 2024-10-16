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
            size_t src_size = offsets[row_i] - offsets[row_i - 1] - 1;

            int32_t dst_size;
            if constexpr (upper)
                dst_size = ucasemap_utf8ToUpper(
                    case_map, reinterpret_cast<char *>(&res_data[curr_offset]), res_data.size() - curr_offset, src, src_size, &error_code);
            else
                dst_size = ucasemap_utf8ToLower(
                    case_map, reinterpret_cast<char *>(&res_data[curr_offset]), res_data.size() - curr_offset, src, src_size, &error_code);

            if (error_code == U_BUFFER_OVERFLOW_ERROR || error_code == U_STRING_NOT_TERMINATED_WARNING)
            {
                size_t new_size = curr_offset + dst_size + 1;
                res_data.resize(new_size);

                error_code = U_ZERO_ERROR;
                if constexpr (upper)
                    dst_size = ucasemap_utf8ToUpper(
                        case_map, reinterpret_cast<char *>(&res_data[curr_offset]), res_data.size() - curr_offset, src, src_size, &error_code);
                else
                    dst_size = ucasemap_utf8ToLower(
                        case_map, reinterpret_cast<char *>(&res_data[curr_offset]), res_data.size() - curr_offset, src, src_size, &error_code);
            }

            if (error_code != U_ZERO_ERROR)
                throw DB::Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Error calling {}: {} input: {} input_size: {}",
                    upper ? "ucasemap_utf8ToUpper" : "ucasemap_utf8ToLower",
                    u_errorName(error_code),
                    std::string_view(src, src_size),
                    src_size);

            res_data[curr_offset + dst_size] = 0;
            curr_offset += dst_size + 1;
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
