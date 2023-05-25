#pragma once

#include <Columns/ColumnString.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/format.h>
#include <Common/memcpySmall.h>


#include <algorithm>
#include <optional>
#include <string>
#include <utility>
#include <vector>


namespace DB
{

struct FormatImpl
{
    static constexpr size_t right_padding = 15;

    template <typename... Args>
    static inline void formatExecute(bool possibly_has_column_string, bool possibly_has_column_fixed_string, Args &&... args)
    {
        if (possibly_has_column_string && possibly_has_column_fixed_string)
            format<true, true>(std::forward<Args>(args)...);
        else if (!possibly_has_column_string && possibly_has_column_fixed_string)
            format<false, true>(std::forward<Args>(args)...);
        else if (possibly_has_column_string && !possibly_has_column_fixed_string)
            format<true, false>(std::forward<Args>(args)...);
        else
            format<false, false>(std::forward<Args>(args)...);
    }

    /// data for ColumnString and ColumnFixed. Nullptr means no data, it is const string.
    /// offsets for ColumnString, nullptr is an indicator that there is a fixed string rather than ColumnString.
    /// fixed_string_N for savings N to fixed strings.
    /// constant_strings for constant strings. If data[i] is nullptr, it is constant string.
    /// res_data is result_data, res_offsets is offset result.
    /// input_rows_count is the number of rows processed.
    /// Precondition: data.size() == offsets.size() == fixed_string_N.size() == constant_strings.size().
    template <bool has_column_string, bool has_column_fixed_string>
    static inline void format(
        String pattern,
        const std::vector<const ColumnString::Chars *> & data,
        const std::vector<const ColumnString::Offsets *> & offsets,
        [[maybe_unused]] /* Because sometimes !has_column_fixed_string */ const std::vector<size_t> & fixed_string_N,
        const std::vector<std::optional<String>> & constant_strings,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        const size_t argument_number = offsets.size();

        /// The subsequent indexes of strings we should use. e.g `Hello world {1} {3} {1} {0}` this
        /// array will be filled with [1, 3, 1, 0] but without constant string indices.
        Format::IndexPositions index_positions;

        /// Vector of substrings of pattern that will be copied to the answer, not string view because of escaping and iterators invalidation.
        /// These are exactly what is between {} tokens, for `Hello {} world {}` we will have [`Hello `, ` world `, ``].
        std::vector<String> substrings;

        Format::init(pattern, argument_number, constant_strings, index_positions, substrings);

        UInt64 final_size = 0;

        for (String & str : substrings)
        {
            /// To use memcpySmallAllowReadWriteOverflow15 for substrings we should allocate a bit more to each string.
            /// That was chosen due to performance issues.
            if (!str.empty())
                str.reserve(str.size() + right_padding);
            final_size += str.size();
        }

        /// The substring number is repeated input_rows_times.
        final_size *= input_rows_count;

        /// Strings without null termination.
        for (size_t i = 1; i < substrings.size(); ++i)
        {
            final_size += data[index_positions[i - 1]]->size();
            /// Fixed strings do not have zero terminating character.
            if (offsets[index_positions[i - 1]])
                final_size -= input_rows_count;
        }

        /// Null termination characters.
        final_size += input_rows_count;

        res_data.resize(final_size);
        res_offsets.resize(input_rows_count);

        UInt64 offset = 0;
        for (UInt64 i = 0; i < input_rows_count; ++i)
        {
            memcpySmallAllowReadWriteOverflow15(res_data.data() + offset, substrings[0].data(), substrings[0].size());
            offset += substrings[0].size();
            /// All strings are constant, we should have substrings.size() == 1.
            if constexpr (has_column_string || has_column_fixed_string)
            {
                for (size_t j = 1; j < substrings.size(); ++j)
                {
                    UInt64 arg = index_positions[j - 1];
                    const auto * offset_ptr = offsets[arg];
                    UInt64 arg_offset = 0;
                    UInt64 size = 0;

                    if constexpr (has_column_string)
                    {
                        if (!has_column_fixed_string || offset_ptr)
                        {
                            arg_offset = (*offset_ptr)[i - 1];
                            size = (*offset_ptr)[i] - arg_offset - 1;
                        }
                    }

                    if constexpr (has_column_fixed_string)
                    {
                        if (!has_column_string || !offset_ptr)
                        {
                            arg_offset = fixed_string_N[arg] * i;
                            size = fixed_string_N[arg];
                        }
                    }

                    memcpySmallAllowReadWriteOverflow15(res_data.data() + offset, data[arg]->data() + arg_offset, size);
                    offset += size;
                    memcpySmallAllowReadWriteOverflow15(res_data.data() + offset, substrings[j].data(), substrings[j].size());
                    offset += substrings[j].size();
                }
            }
            res_data[offset] = '\0';
            ++offset;
            res_offsets[i] = offset;
        }

        /*
         * Invariant of `offset == final_size` must be held.
         *
         * if (offset != final_size)
         *    abort();
         */
    }
};

}
