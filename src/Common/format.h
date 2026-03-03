#pragma once

#include <base/types.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/StringUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace Format
{
    using IndexPositions = PODArrayWithStackMemory<UInt64, 64>;

    static inline UInt64 parseNumber(const String & description, UInt64 l, UInt64 r, UInt64 argument_number)
    {
        UInt64 res = 0;
        for (UInt64 pos = l; pos < r; ++pos)
        {
            if (!isNumericASCII(description[pos]))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not a number in curly braces at position {}", pos);
            res = res * 10 + description[pos] - '0';
            if (res >= argument_number)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Too big number for arguments, must be at most {}", argument_number - 1);
        }
        return res;
    }

    static inline void init(
        const String & pattern,
        size_t argument_number,
        const std::vector<std::optional<String>> & constant_strings,
        IndexPositions & index_positions,
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
            std::erase_if(
                str,
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
                });
        };

        index_positions.emplace_back();

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
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Two open curly braces without close one at position {}", i);

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
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Closed curly brace without open one at position {}", i);

                is_open_curly = false;

                if (last_open == i)
                {
                    if (is_plain_numbering && !*is_plain_numbering)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot switch from automatic field numbering to manual field specification");
                    is_plain_numbering = true;
                    if (index_if_plain >= argument_number)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not enough arguments to fill the placeholders in the format string");
                    index_positions.back() = index_if_plain++;
                }
                else
                {
                    if (is_plain_numbering && *is_plain_numbering)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot switch from automatic field numbering to manual field specification");
                    is_plain_numbering = false;

                    UInt64 arg = parseNumber(pattern, last_open, i, argument_number);

                    if (arg >= argument_number)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument is too big for formatting. Note that indexing starts from zero");

                    index_positions.back() = arg;
                }

                if (!constant_strings.empty() && constant_strings[index_positions.back()])
                {
                    /// The next string should be glued to last `A {} C`.format('B') -> `A B C`.
                    glue_to_next = true;
                    substrings.back() += *constant_strings[index_positions.back()];
                }
                else
                    index_positions.emplace_back(); /// Otherwise we commit arg number and proceed.

                start_pos = i + 1;
            }
        }

        if (is_open_curly)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Last open curly brace is not closed");

        String to_add = String(pattern.data() + start_pos, pattern.size() - start_pos);
        double_brace_removal(to_add);

        if (!glue_to_next)
            substrings.emplace_back(to_add);
        else
            substrings.back() += to_add;

        index_positions.pop_back();
    }
}

}
