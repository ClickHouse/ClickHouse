#pragma once

#include "config.h"

#if USE_ICU

#include <Columns/ColumnString.h>
#include <Functions/LowerUpperImpl.h>
#include <base/find_symbols.h>
#include <unicode/unistr.h>
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

        res_data.resize(data.size());
        res_offsets.resize_exact(offsets.size());

        String output;
        size_t curr_offset = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const auto * data_start = reinterpret_cast<const char *>(&data[offsets[i - 1]]);
            size_t size = offsets[i] - offsets[i - 1];

            icu::UnicodeString input(data_start, static_cast<int32_t>(size), "UTF-8");
            if constexpr (upper)
                input.toUpper();
            else
                input.toLower();

            output.clear();
            input.toUTF8String(output);

            /// For valid UTF-8 input strings, ICU sometimes produces output with extra '\0's at the end. Only the data before the first
            /// '\0' is valid. It the input is not valid UTF-8, then the behavior of lower/upperUTF8 is undefined by definition. In this
            /// case, the behavior is also reasonable.
            const char * res_end = find_last_not_symbols_or_null<'\0'>(output.data(), output.data() + output.size());
            size_t valid_size = res_end ? res_end - output.data() + 1 : 0;

            res_data.resize(curr_offset + valid_size + 1);
            memcpy(&res_data[curr_offset], output.data(), valid_size);
            res_data[curr_offset + valid_size] = 0;

            curr_offset += valid_size + 1;
            res_offsets[i] = curr_offset;
        }
    }

    static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Functions lowerUTF8 and upperUTF8 cannot work with FixedString argument");
    }
};

}

#endif
