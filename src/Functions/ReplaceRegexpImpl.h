#pragma once

#include <base/types.h>
#include <Columns/ColumnString.h>
#include <IO/WriteHelpers.h>

#include "config.h"
#include <re2_st/re2.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct ReplaceRegexpTraits
{
    enum class Replace
    {
        First,
        All
    };
};

/** Replace all matches of regexp 'needle' to string 'replacement'. 'needle' and 'replacement' are constants.
  * 'replacement' can contain substitutions, for example: '\2-\3-\1'
  */
template <ReplaceRegexpTraits::Replace replace>
struct ReplaceRegexpImpl
{
    struct Instruction
    {
        /// If not negative, perform substitution of n-th subpattern from the regexp match.
        int substitution_num = -1;
        /// Otherwise, paste this literal string verbatim.
        String literal;

        explicit Instruction(int substitution_num_) : substitution_num(substitution_num_) {}
        explicit Instruction(String literal_) : literal(std::move(literal_)) {}
    };

    /// Decomposes the replacement string into a sequence of substitutions and literals.
    /// E.g. "abc\1de\2fg\1\2" --> inst("abc"), inst(1), inst("de"), inst(2), inst("fg"), inst(1), inst(2)
    using Instructions = std::vector<Instruction>;

    static constexpr int max_captures = 10;

    static Instructions createInstructions(std::string_view replacement, int num_captures)
    {
        Instructions instructions;

        String literals;
        for (size_t i = 0; i < replacement.size(); ++i)
        {
            if (replacement[i] == '\\' && i + 1 < replacement.size())
            {
                if (isNumericASCII(replacement[i + 1])) /// Substitution
                {
                    if (!literals.empty())
                    {
                        instructions.emplace_back(literals);
                        literals = "";
                    }
                    instructions.emplace_back(replacement[i + 1] - '0');
                }
                else
                    literals += replacement[i + 1]; /// Escaping
                ++i;
            }
            else
                literals += replacement[i]; /// Plain character
        }

        if (!literals.empty())
            instructions.emplace_back(literals);

        for (const auto & instr : instructions)
            if (instr.substitution_num >= num_captures)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Id {} in replacement string is an invalid substitution, regexp has only {} capturing groups",
                    instr.substitution_num, num_captures - 1);

        return instructions;
    }

    static void processString(
        const char * haystack_data,
        size_t haystack_length,
        ColumnString::Chars & res_data,
        ColumnString::Offset & res_offset,
        const re2_st::RE2 & searcher,
        int num_captures,
        const Instructions & instructions)
    {
        re2_st::StringPiece haystack(haystack_data, haystack_length);
        re2_st::StringPiece matches[max_captures];

        size_t copy_pos = 0;
        size_t match_pos = 0;

        while (match_pos < haystack_length)
        {
            /// If no more replacements possible for current string
            bool can_finish_current_string = false;

            if (searcher.Match(haystack, match_pos, haystack_length, re2_st::RE2::Anchor::UNANCHORED, matches, num_captures))
            {
                const auto & match = matches[0]; /// Complete match (\0)
                size_t bytes_to_copy = (match.data() - haystack.data()) - copy_pos;

                /// Copy prefix before current match without modification
                res_data.resize(res_data.size() + bytes_to_copy);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], haystack.data() + copy_pos, bytes_to_copy);
                res_offset += bytes_to_copy;
                copy_pos += bytes_to_copy + match.length();
                match_pos = copy_pos;

                /// Substitute inside current match using instructions
                for (const auto & instr : instructions)
                {
                    std::string_view replacement;
                    if (instr.substitution_num >= 0)
                        replacement = std::string_view(matches[instr.substitution_num].data(), matches[instr.substitution_num].size());
                    else
                        replacement = instr.literal;
                    res_data.resize(res_data.size() + replacement.size());
                    memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                    res_offset += replacement.size();
                }

                if constexpr (replace == ReplaceRegexpTraits::Replace::First)
                    can_finish_current_string = true;

                if (match.empty())
                {
                    /// Step one character to avoid infinite loop
                    ++match_pos;
                    if (match_pos >= haystack_length)
                        can_finish_current_string = true;
                }
            }
            else
                can_finish_current_string = true;

            /// If ready, append suffix after match to end of string.
            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + haystack_length - copy_pos);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], haystack.data() + copy_pos, haystack_length - copy_pos);
                res_offset += haystack_length - copy_pos;
                copy_pos = haystack_length;
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
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        ColumnString::Offset res_offset = 0;
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        re2_st::RE2::Options regexp_options;
        /// Don't write error messages to stderr.
        regexp_options.set_log_errors(false);

        re2_st::RE2 searcher(needle, regexp_options);

        if (!searcher.ok())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The pattern argument is not a valid re2 pattern: {}",
                searcher.error());

        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);

        Instructions instructions = createInstructions(replacement, num_captures);

        /// Cannot perform search for whole columns. Will process each string separately.
        for (size_t i = 0; i < size; ++i)
        {
            size_t from = i > 0 ? offsets[i - 1] : 0;
            const char * haystack_data = reinterpret_cast<const char *>(data.data() + from);
            const size_t haystack_length = static_cast<unsigned>(offsets[i] - from - 1);

            processString(haystack_data, haystack_length, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }

    static void vectorFixed(
        const ColumnString::Chars & data,
        size_t n,
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        ColumnString::Offset res_offset = 0;
        size_t size = data.size() / n;
        res_data.reserve(data.size());
        res_offsets.resize(size);

        re2_st::RE2::Options regexp_options;
        /// Don't write error messages to stderr.
        regexp_options.set_log_errors(false);

        re2_st::RE2 searcher(needle, regexp_options);

        if (!searcher.ok())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The pattern argument is not a valid re2 pattern: {}",
                searcher.error());

        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);

        Instructions instructions = createInstructions(replacement, num_captures);

        for (size_t i = 0; i < size; ++i)
        {
            size_t from = i * n;
            const char * haystack_data = reinterpret_cast<const char *>(data.data() + from);
            const size_t haystack_length = n;

            processString(haystack_data, haystack_length, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }
};

}
