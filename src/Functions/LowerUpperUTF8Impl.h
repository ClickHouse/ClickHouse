#pragma once

#include "config.h"

#if USE_ICU

#include <Columns/ColumnString.h>
#include <Functions/LowerUpperImpl.h>
#include <unicode/unistr.h>
#include <unicode/ucasemap.h>
#include <unicode/utypes.h>
#include <unicode/urename.h>
#include <Common/StringUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
        if (data.empty())
            return;

        bool all_ascii = isAllASCII(data.data(), data.size());
        if (all_ascii)
        {
            LowerUpperImpl<not_case_lower_bound, not_case_upper_bound>::vector(data, offsets, res_data, res_offsets, input_rows_count);
            return;
        }


        UErrorCode error_code = U_ZERO_ERROR;
        UCaseMap * csm = ucasemap_open(nullptr, 0, &error_code);
        if (U_FAILURE(error_code))
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Call ucasemap_open error:{}", u_errorName(error_code));

        // String output;
        size_t curr_offset = 0;
        res_data.resize(data.size());
        res_offsets.resize_exact(offsets.size());
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto * data_start = reinterpret_cast<const char *>(&data[offsets[i - 1]]);
            size_t size = offsets[i] - offsets[i - 1] - 1;

            int32_t out_size;
            if constexpr (upper)
                out_size = ucasemap_utf8ToUpper(
                    csm, reinterpret_cast<char *>(&res_data[curr_offset]), res_data.size() - curr_offset, data_start, size, &error_code);
            else
                out_size = ucasemap_utf8ToLower(
                    csm, reinterpret_cast<char *>(&res_data[curr_offset]), res_data.size() - curr_offset, data_start, size, &error_code);
            // std::cout << size << ":" << out_size << ":" << static_cast<size_t>(res_data[curr_offset + out_size - 1]) << ":" << error_code
            //   << std::endl;

            if (error_code == U_BUFFER_OVERFLOW_ERROR)
            {
                size_t new_size = curr_offset + out_size + 1;
                res_data.resize(new_size);

                error_code = U_ZERO_ERROR;
                if constexpr (upper)
                    out_size = ucasemap_utf8ToUpper(
                        csm,
                        reinterpret_cast<char *>(&res_data[curr_offset]),
                        res_data.size() - curr_offset,
                        data_start,
                        size,
                        &error_code);
                else
                    out_size = ucasemap_utf8ToLower(
                        csm,
                        reinterpret_cast<char *>(&res_data[curr_offset]),
                        res_data.size() - curr_offset,
                        data_start,
                        size,
                        &error_code);
            }

            if (error_code != U_ZERO_ERROR)
                throw DB::Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Call {} error:{} input:{} input_size:{}",
                    upper ? "ucasemap_utf8ToUpper" : "ucasemap_utf8ToLower",
                    u_errorName(error_code),
                    std::string_view(data_start, size),
                    size);

            res_data[curr_offset + out_size] = 0;
            curr_offset += out_size + 1;
            res_offsets[i] = curr_offset;
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
