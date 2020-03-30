#pragma once

#include <Columns/ColumnString.h>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/memcpySmall.h>

#include <algorithm>
#include <optional>
#include <string>
#include <utility>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct FormatImpl
{
    static constexpr size_t small_argument_threshold = 1024;
    static constexpr size_t argument_threshold = std::numeric_limits<UInt32>::max();
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

    static void parseNumber(const String & description, UInt64 l, UInt64 r, UInt64 & res)
    {
        res = 0;
        for (UInt64 pos = l; pos < r; pos++)
        {
            if (!isNumericASCII(description[pos]))
                throw Exception("Not a number in curly braces at position " + std::to_string(pos), ErrorCodes::BAD_ARGUMENTS);
            res = res * 10 + description[pos] - '0';
            if (res >= argument_threshold)
                throw Exception(
                    "Too big number for arguments, must be at most " + std::to_string(argument_threshold), ErrorCodes::BAD_ARGUMENTS);
        }
    }

    static inline void init(
        const String & pattern,
        const std::vector<const ColumnString::Chars *> & data,
        size_t argument_number,
        const std::vector<String> & constant_strings,
        UInt64 * index_positions_ptr,
        std::vector<String> & substrings)
    {
        /// Is current position after open curly brace.
        bool is_open_curly = false;
        /// The position of last open token.
        size_t last_open = -1;

        /// Is formatting in a plain {} token.
        std::optional<bool> is_plain_numbering;
        UInt64 index_if_plain = 0;

        /// Left position of adding substrings, just to the closed brace position or the start of the string.
        /// Invariant --- the start of substring is in this position.
        size_t start_pos = 0;

        /// A flag to decide whether we should glue the constant strings.
        bool glue_to_next = false;

        /// Handling double braces (escaping).
        auto double_brace_removal = [](String & str)
        {
            size_t i = 0;
            bool should_delete = true;
            str.erase(
                std::remove_if(
                    str.begin(),
                    str.end(),
                    [&i, &should_delete, &str](char)
                    {
                        bool is_double_brace = (str[i] == '{' && str[i + 1] == '{') || (str[i] == '}' && str[i + 1] == '}');
                        ++i;
                        if (is_double_brace && should_delete)
                        {
                            should_delete = false;
                            return true;
                        }
                        should_delete = true;
                        return false;
                    }),
                str.end());
        };

        for (size_t i = 0; i < pattern.size(); ++i)
        {
            if (pattern[i] == '{')
            {
                /// Escaping handling
                /// It is safe to access because of null termination
                if (pattern[i + 1] == '{')
                {
                    ++i;
                    continue;
                }

                if (is_open_curly)
                    throw Exception("Two open curly braces without close one at position " + std::to_string(i), ErrorCodes::BAD_ARGUMENTS);

                String to_add = String(pattern.data() + start_pos, i - start_pos);
                double_brace_removal(to_add);
                if (!glue_to_next)
                    substrings.emplace_back(to_add);
                else
                    substrings.back() += to_add;

                glue_to_next = false;

                is_open_curly = true;
                last_open = i + 1;
            }
            else if (pattern[i] == '}')
            {
                if (pattern[i + 1] == '}')
                {
                    ++i;
                    continue;
                }

                if (!is_open_curly)
                    throw Exception("Closed curly brace without open one at position " + std::to_string(i), ErrorCodes::BAD_ARGUMENTS);

                is_open_curly = false;

                if (last_open == i)
                {
                    if (is_plain_numbering && !*is_plain_numbering)
                        throw Exception(
                            "Cannot switch from automatic field numbering to manual field specification", ErrorCodes::BAD_ARGUMENTS);
                    is_plain_numbering = true;
                    if (index_if_plain >= argument_number)
                        throw Exception("Argument is too big for formatting", ErrorCodes::BAD_ARGUMENTS);
                    *index_positions_ptr = index_if_plain++;
                }
                else
                {
                    if (is_plain_numbering && *is_plain_numbering)
                        throw Exception(
                            "Cannot switch from automatic field numbering to manual field specification", ErrorCodes::BAD_ARGUMENTS);
                    is_plain_numbering = false;

                    UInt64 arg;
                    parseNumber(pattern, last_open, i, arg);

                    if (arg >= argument_number)
                        throw Exception(
                            "Argument is too big for formatting. Note that indexing starts from zero", ErrorCodes::BAD_ARGUMENTS);

                    *index_positions_ptr = arg;
                }

                /// Constant string.
                if (!data[*index_positions_ptr])
                {
                    /// The next string should be glued to last `A {} C`.format('B') -> `A B C`.
                    glue_to_next = true;
                    substrings.back() += constant_strings[*index_positions_ptr];
                }
                else
                    ++index_positions_ptr; /// Otherwise we commit arg number and proceed.

                start_pos = i + 1;
            }
        }

        if (is_open_curly)
            throw Exception("Last open curly brace is not closed", ErrorCodes::BAD_ARGUMENTS);

        String to_add = String(pattern.data() + start_pos, pattern.size() - start_pos);
        double_brace_removal(to_add);

        if (!glue_to_next)
            substrings.emplace_back(to_add);
        else
            substrings.back() += to_add;
    }

    /// data for ColumnString and ColumnFixed. Nullptr means no data, it is const string.
    /// offsets for ColumnString, nullptr is an indicator that there is a fixed string rather than ColumnString.
    /// fixed_string_N for savings N to fixed strings.
    /// constant_strings for constant strings. If data[i] is nullptr, than it is constant string.
    /// res_data is result_data, res_offsets is offset result.
    /// input_rows_count is the number of rows processed.
    /// Precondition: data.size() == offsets.size() == fixed_string_N.size() == constant_strings.size().
    template <bool has_column_string, bool has_column_fixed_string>
    static inline void format(
        String pattern,
        const std::vector<const ColumnString::Chars *> & data,
        const std::vector<const ColumnString::Offsets *> & offsets,
        [[maybe_unused]] /* Because sometimes !has_column_fixed_string */ const std::vector<size_t> & fixed_string_N,
        const std::vector<String> & constant_strings,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        const size_t argument_number = offsets.size();

        UInt64 small_index_positions_buffer[small_argument_threshold];
        /// The subsequent indexes of strings we should use. e.g `Hello world {1} {3} {1} {0}` this array will be filled with [1, 3, 1, 0, ... (garbage)] but without constant string indices.
        UInt64 * index_positions = small_index_positions_buffer;

        std::unique_ptr<UInt64[]> big_index_positions_buffer;
        if (argument_number > small_argument_threshold)
        {
            big_index_positions_buffer.reset(new UInt64[argument_number]);
            index_positions = big_index_positions_buffer.get();
        }

        /// Vector of substrings of pattern that will be copied to the ans, not string view because of escaping and iterators invalidation.
        /// These are exactly what is between {} tokens, for `Hello {} world {}` we will have [`Hello `, ` world `, ``].
        std::vector<String> substrings;

        init(pattern, data, argument_number, constant_strings, index_positions, substrings);

        UInt64 final_size = 0;

        for (String & str : substrings)
        {
            /// To use memcpySmallAllowReadWriteOverflow15 for substrings we should allocate a bit more to each string.
            /// That was chosen due to perfomance issues.
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
                    auto offset_ptr = offsets[arg];
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
