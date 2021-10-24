#pragma once

#include <common/types.h>
#include <Common/Volnitsky.h>
#include <Columns/ColumnString.h>
#include <IO/WriteHelpers.h>

#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#    include <Common/config.h>
#endif

#if USE_RE2_ST
#    include <re2_st/re2.h>
#else
#    include <re2/re2.h>
#    define re2_st re2
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


/** Replace all matches of regexp 'needle' to string 'replacement'. 'needle' and 'replacement' are constants.
  * 'replacement' could contain substitutions, for example: '\2-\3-\1'
  */
template <bool replace_one = false>
struct ReplaceRegexpImpl
{
    /// Sequence of instructions, describing how to get resulting string.
    /// Each element is either:
    /// - substitution (in that case first element of pair is their number and second element is empty)
    /// - string that need to be inserted (in that case, first element of pair is that string and second element is -1)
    using Instructions = std::vector<std::pair<int, std::string>>;

    static const size_t max_captures = 10;


    static Instructions createInstructions(const std::string & s, int num_captures)
    {
        Instructions instructions;

        String now;
        for (size_t i = 0; i < s.size(); ++i)
        {
            if (s[i] == '\\' && i + 1 < s.size())
            {
                if (isNumericASCII(s[i + 1])) /// Substitution
                {
                    if (!now.empty())
                    {
                        instructions.emplace_back(-1, now);
                        now = "";
                    }
                    instructions.emplace_back(s[i + 1] - '0', String());
                }
                else
                    now += s[i + 1]; /// Escaping
                ++i;
            }
            else
                now += s[i]; /// Plain character
        }

        if (!now.empty())
        {
            instructions.emplace_back(-1, now);
            now = "";
        }

        for (const auto & it : instructions)
            if (it.first >= num_captures)
                throw Exception(
                    "Invalid replace instruction in replacement string. Id: " + toString(it.first) + ", but regexp has only "
                        + toString(num_captures - 1) + " subpatterns",
                    ErrorCodes::BAD_ARGUMENTS);

        return instructions;
    }


    static void processString(
        const re2_st::StringPiece & input,
        ColumnString::Chars & res_data,
        ColumnString::Offset & res_offset,
        re2_st::RE2 & searcher,
        int num_captures,
        const Instructions & instructions)
    {
        re2_st::StringPiece matches[max_captures];

        size_t start_pos = 0;
        bool is_first_match = true;
        bool is_start_pos_added_one = false;

        while (start_pos < static_cast<size_t>(input.length()))
        {
            /// If no more replacements possible for current string
            bool can_finish_current_string = false;

            if (searcher.Match(input, start_pos, input.length(), re2_st::RE2::Anchor::UNANCHORED, matches, num_captures))
            {
                if (is_start_pos_added_one)
                    start_pos -= 1;

                const auto & match = matches[0];
                size_t bytes_to_copy = (match.data() - input.data()) - start_pos;

                /// Copy prefix before matched regexp without modification
                res_data.resize(res_data.size() + bytes_to_copy);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data() + start_pos, bytes_to_copy);
                res_offset += bytes_to_copy;
                start_pos += bytes_to_copy + match.length();

                /// To avoid infinite loop.
                if (is_first_match && match.length() == 0 && !replace_one && input.length() > 1)
                {
                    start_pos += 1;
                    is_start_pos_added_one = true;
                }

                /// Do substitution instructions
                for (const auto & it : instructions)
                {
                    if (it.first >= 0)
                    {
                        res_data.resize(res_data.size() + matches[it.first].length());
                        memcpy(&res_data[res_offset], matches[it.first].data(), matches[it.first].length());
                        res_offset += matches[it.first].length();
                    }
                    else
                    {
                        res_data.resize(res_data.size() + it.second.size());
                        memcpy(&res_data[res_offset], it.second.data(), it.second.size());
                        res_offset += it.second.size();
                    }
                }

                if (replace_one || (!is_first_match && match.length() == 0))
                    can_finish_current_string = true;
                is_first_match = false;
            }
            else
                can_finish_current_string = true;

            /// If ready, append suffix after match to end of string.
            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + input.length() - start_pos);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data() + start_pos, input.length() - start_pos);
                res_offset += input.length() - start_pos;
                start_pos = input.length();
            }
        }

        res_data.resize(res_data.size() + 1);
        res_data[res_offset] = 0;
        ++res_offset;
    }


    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & needle,
        const std::string & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        ColumnString::Offset res_offset = 0;
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        typename re2_st::RE2::Options regexp_options;
        /// Never write error messages to stderr. It's ignorant to do it from library code.
        regexp_options.set_log_errors(false);
        re2_st::RE2 searcher(needle, regexp_options);
        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, static_cast<int>(max_captures));

        Instructions instructions = createInstructions(replacement, num_captures);

        /// Cannot perform search for whole columns. Will process each string separately.
        for (size_t i = 0; i < size; ++i)
        {
            int from = i > 0 ? offsets[i - 1] : 0;
            re2_st::StringPiece input(reinterpret_cast<const char *>(data.data() + from), offsets[i] - from - 1);

            processString(input, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }

    static void vectorFixed(
        const ColumnString::Chars & data,
        size_t n,
        const std::string & needle,
        const std::string & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        ColumnString::Offset res_offset = 0;
        size_t size = data.size() / n;
        res_data.reserve(data.size());
        res_offsets.resize(size);

        typename re2_st::RE2::Options regexp_options;
        /// Never write error messages to stderr. It's ignorant to do it from library code.
        regexp_options.set_log_errors(false);
        re2_st::RE2 searcher(needle, regexp_options);
        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, static_cast<int>(max_captures));

        Instructions instructions = createInstructions(replacement, num_captures);

        for (size_t i = 0; i < size; ++i)
        {
            int from = i * n;
            re2_st::StringPiece input(reinterpret_cast<const char *>(data.data() + from), n);

            processString(input, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }
};

}
