#pragma once

#include <base/types.h>
#include <Common/Volnitsky.h>
#include <Columns/ColumnString.h>
#include <IO/WriteHelpers.h>

#include "config_functions.h"
#include <Common/config.h>
#include <re2_st/re2.h>


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
    struct Instruction
    {
        /// If not negative - perform substitution of n-th subpattern from the regexp match.
        int substitution_num = -1;
        /// Otherwise - paste this string verbatim.
        std::string literal;

        Instruction(int substitution_num_) : substitution_num(substitution_num_) {} /// NOLINT
        Instruction(std::string literal_) : literal(std::move(literal_)) {} /// NOLINT
    };

    using Instructions = std::vector<Instruction>;

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
                        instructions.emplace_back(now);
                        now = "";
                    }
                    instructions.emplace_back(s[i + 1] - '0');
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
            instructions.emplace_back(now);
            now = "";
        }

        for (const auto & it : instructions)
            if (it.substitution_num >= num_captures)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Invalid replace instruction in replacement string. Id: {}, but regexp has only {} subpatterns",
                    it.substitution_num, num_captures - 1);

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

        size_t copy_pos = 0;
        size_t match_pos = 0;

        while (match_pos < static_cast<size_t>(input.length()))
        {
            /// If no more replacements possible for current string
            bool can_finish_current_string = false;

            if (searcher.Match(input, match_pos, input.length(), re2_st::RE2::Anchor::UNANCHORED, matches, num_captures))
            {
                const auto & match = matches[0];
                size_t bytes_to_copy = (match.data() - input.data()) - copy_pos;

                /// Copy prefix before matched regexp without modification
                res_data.resize(res_data.size() + bytes_to_copy);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data() + copy_pos, bytes_to_copy);
                res_offset += bytes_to_copy;
                copy_pos += bytes_to_copy + match.length();
                match_pos = copy_pos;

                /// Do substitution instructions
                for (const auto & it : instructions)
                {
                    if (it.substitution_num >= 0)
                    {
                        const auto & substitution = matches[it.substitution_num];

                        res_data.resize(res_data.size() + substitution.length());
                        memcpy(&res_data[res_offset], substitution.data(), substitution.length());
                        res_offset += substitution.length();
                    }
                    else
                    {
                        const auto & literal = it.literal;

                        res_data.resize(res_data.size() + literal.size());
                        memcpy(&res_data[res_offset], literal.data(), literal.size());
                        res_offset += literal.size();
                    }
                }

                if (replace_one)
                    can_finish_current_string = true;

                if (match.length() == 0)
                {
                    /// Step one character to avoid infinite loop
                    ++match_pos;
                    if (match_pos >= static_cast<size_t>(input.length()))
                        can_finish_current_string = true;
                }
            }
            else
                can_finish_current_string = true;

            /// If ready, append suffix after match to end of string.
            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + input.length() - copy_pos);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data() + copy_pos, input.length() - copy_pos);
                res_offset += input.length() - copy_pos;
                copy_pos = input.length();
                match_pos = copy_pos;
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
