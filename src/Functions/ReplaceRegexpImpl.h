#pragma once

#include <Columns/ColumnString.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/re2.h>
#include <Functions/Regexps.h>
#include <Functions/ReplaceStringImpl.h>
#include <IO/WriteHelpers.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct ReplaceRegexpTraits
{
    enum class Replace : uint8_t
    {
        First,
        All
    };
};

/** Replace all matches of regexp 'needle' to string 'replacement'. 'needle' and 'replacement' are constants.
  * 'replacement' can contain substitutions, for example: '\2-\3-\1'
  */
template <typename Name, ReplaceRegexpTraits::Replace replace>
struct ReplaceRegexpImpl
{
    static constexpr auto name = Name::name;

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

    /// The replacement string references must not contain non-existing capturing groups.
    static void checkSubstitutions(std::string_view replacement, int num_captures)
    {
        for (size_t i = 0; i < replacement.size(); ++i)
        {
            if (replacement[i] == '\\' && i + 1 < replacement.size())
            {
                if (isNumericASCII(replacement[i + 1])) /// substitution
                {
                    int substitution_num = replacement[i + 1] - '0';
                    if (substitution_num >= num_captures)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Substitution '\\{}' in replacement argument is invalid, regexp has only {} capturing groups", substitution_num, num_captures - 1);
                }
            }
        }
    }

    static Instructions createInstructions(std::string_view replacement, int num_captures)
    {
        checkSubstitutions(replacement, num_captures);

        Instructions instructions;

        String literals;
        literals.reserve(replacement.size());

        for (size_t i = 0; i < replacement.size(); ++i)
        {
            if (replacement[i] == '\\' && i + 1 < replacement.size())
            {
                if (isNumericASCII(replacement[i + 1])) /// substitution
                {
                    if (!literals.empty())
                    {
                        instructions.emplace_back(literals);
                        literals = "";
                    }
                    int substitution_num = replacement[i + 1] - '0';
                    instructions.emplace_back(substitution_num);
                }
                else
                    literals += replacement[i + 1]; /// escaping
                ++i;
            }
            else
                literals += replacement[i]; /// plain character
        }

        if (!literals.empty())
            instructions.emplace_back(literals);

        return instructions;
    }

    static bool canFallbackToStringReplacement(const String & needle, const String & replacement, const re2::RE2 & searcher, int num_captures)
    {
        if (searcher.NumberOfCapturingGroups())
            return false;

        checkSubstitutions(replacement, num_captures);

        String required_substring;
        bool is_trivial;
        bool required_substring_is_prefix;
        std::vector<String> alternatives;
        OptimizedRegularExpression::analyze(needle, required_substring, is_trivial, required_substring_is_prefix, alternatives);
        return is_trivial && required_substring_is_prefix && required_substring == needle;
    }

    static void processString(
        const char * haystack_data,
        size_t haystack_length,
        ColumnString::Chars & res_data,
        ColumnString::Offset & res_offset,
        const re2::RE2 & searcher,
        int num_captures,
        const Instructions & instructions)
    {
        std::string_view haystack(haystack_data, haystack_length);
        std::string_view matches[max_captures];

        size_t copy_pos = 0;
        size_t match_pos = 0;

        while (match_pos < haystack_length)
        {
            /// If no more replacements possible for current string
            bool can_finish_current_string = false;

            if (searcher.Match(haystack, match_pos, haystack_length, re2::RE2::Anchor::UNANCHORED, matches, num_captures))
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
                        replacement = {matches[instr.substitution_num].data(), matches[instr.substitution_num].size()};
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

    static void vectorConstantConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        if (needle.empty())
        {
            res_data.assign(haystack_data);
            res_offsets.assign(haystack_offsets);
            return;
        }

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        re2::RE2::Options regexp_options;
        regexp_options.set_log_errors(false); /// don't write error messages to stderr

        re2::RE2 searcher(needle, regexp_options);
        if (!searcher.ok())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The pattern argument is not a valid re2 pattern: {}", searcher.error());

        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);

        /// Try to use non-regexp string replacement. This shortcut is implemented only for const-needles + const-replacement as
        /// pattern analysis incurs some cost too.
        if (canFallbackToStringReplacement(needle, replacement, searcher, num_captures))
        {
            auto convertTrait = [](ReplaceRegexpTraits::Replace first_or_all)
            {
                switch (first_or_all)
                {
                    case ReplaceRegexpTraits::Replace::First: return ReplaceStringTraits::Replace::First;
                    case ReplaceRegexpTraits::Replace::All:   return ReplaceStringTraits::Replace::All;
                }
            };
            ReplaceStringImpl<Name, convertTrait(replace)>::vectorConstantConstant(haystack_data, haystack_offsets, needle, replacement, res_data, res_offsets, input_rows_count);
            return;
        }

        Instructions instructions = createInstructions(replacement, num_captures);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t from = haystack_offsets[i - 1];

            const char * hs_data = reinterpret_cast<const char *>(haystack_data.data() + from);
            const size_t hs_length = static_cast<unsigned>(haystack_offsets[i] - from - 1);

            processString(hs_data, hs_length, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }

    static void vectorVectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        assert(haystack_offsets.size() == needle_offsets.size());

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        re2::RE2::Options regexp_options;
        regexp_options.set_log_errors(false); /// don't write error messages to stderr

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t hs_from = haystack_offsets[i - 1];
            const char * hs_data = reinterpret_cast<const char *>(haystack_data.data() + hs_from);
            const size_t hs_length = static_cast<unsigned>(haystack_offsets[i] - hs_from - 1);

            size_t ndl_from = needle_offsets[i - 1];
            const char * ndl_data = reinterpret_cast<const char *>(needle_data.data() + ndl_from);
            const size_t ndl_length = static_cast<unsigned>(needle_offsets[i] - ndl_from - 1);
            std::string_view needle(ndl_data, ndl_length);

            if (needle.empty())
            {
                res_data.insert(res_data.end(), hs_data, hs_data + hs_length);
                res_data.push_back(0);

                res_offset += hs_length + 1;
                res_offsets[i] = res_offset;
                continue;
            }

            re2::RE2 searcher(needle, regexp_options);
            if (!searcher.ok())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The pattern argument is not a valid re2 pattern: {}", searcher.error());

            int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);
            Instructions instructions = createInstructions(replacement, num_captures);

            processString(hs_data, hs_length, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }

    static void vectorConstantVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const String & needle,
        const ColumnString::Chars & replacement_data,
        const ColumnString::Offsets & replacement_offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        assert(haystack_offsets.size() == replacement_offsets.size());

        if (needle.empty())
        {
            res_data.assign(haystack_data);
            res_offsets.assign(haystack_offsets);
            return;
        }

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        re2::RE2::Options regexp_options;
        regexp_options.set_log_errors(false); /// don't write error messages to stderr

        re2::RE2 searcher(needle, regexp_options);
        if (!searcher.ok())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The pattern argument is not a valid re2 pattern: {}", searcher.error());

        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t hs_from = haystack_offsets[i - 1];
            const char * hs_data = reinterpret_cast<const char *>(haystack_data.data() + hs_from);
            const size_t hs_length = static_cast<unsigned>(haystack_offsets[i] - hs_from - 1);

            size_t repl_from = replacement_offsets[i - 1];
            const char * repl_data = reinterpret_cast<const char *>(replacement_data.data() + repl_from);
            const size_t repl_length = static_cast<unsigned>(replacement_offsets[i] - repl_from - 1);
            std::string_view replacement(repl_data, repl_length);

            Instructions instructions = createInstructions(replacement, num_captures);

            processString(hs_data, hs_length, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }

    static void vectorVectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnString::Chars & replacement_data,
        const ColumnString::Offsets & replacement_offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        assert(haystack_offsets.size() == needle_offsets.size());
        assert(needle_offsets.size() == replacement_offsets.size());

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        re2::RE2::Options regexp_options;
        regexp_options.set_log_errors(false); /// don't write error messages to stderr

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t hs_from = haystack_offsets[i - 1];
            const char * hs_data = reinterpret_cast<const char *>(haystack_data.data() + hs_from);
            const size_t hs_length = static_cast<unsigned>(haystack_offsets[i] - hs_from - 1);

            size_t ndl_from = needle_offsets[i - 1];
            const char * ndl_data = reinterpret_cast<const char *>(needle_data.data() + ndl_from);
            const size_t ndl_length = static_cast<unsigned>(needle_offsets[i] - ndl_from - 1);
            std::string_view needle(ndl_data, ndl_length);

            if (needle.empty())
            {
                res_data.insert(res_data.end(), hs_data, hs_data + hs_length);
                res_data.push_back(0);
                res_offsets[i] = res_offsets[i - 1] + hs_length + 1;
                res_offset = res_offsets[i];
                continue;
            }

            size_t repl_from = replacement_offsets[i - 1];
            const char * repl_data = reinterpret_cast<const char *>(replacement_data.data() + repl_from);
            const size_t repl_length = static_cast<unsigned>(replacement_offsets[i] - repl_from - 1);
            std::string_view replacement(repl_data, repl_length);

            re2::RE2 searcher(needle, regexp_options);
            if (!searcher.ok())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The pattern argument is not a valid re2 pattern: {}", searcher.error());

            int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);
            Instructions instructions = createInstructions(replacement, num_captures);

            processString(hs_data, hs_length, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }

    static void vectorFixedConstantConstant(
        const ColumnString::Chars & haystack_data,
        size_t n,
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        if (needle.empty())
        {
            chassert(input_rows_count == haystack_data.size() / n);
            /// Since ColumnFixedString does not have a zero byte at the end, while ColumnString does,
            /// we need to split haystack_data into strings of length n, add 1 zero byte to the end of each string
            /// and then copy to res_data, ref: ColumnString.h and ColumnFixedString.h
            res_data.reserve(haystack_data.size() + input_rows_count);
            res_offsets.resize(input_rows_count);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                res_data.insert(res_data.end(), haystack_data.begin() + i * n, haystack_data.begin() + (i + 1) * n);
                res_data.push_back(0);
                res_offsets[i] = res_offsets[i - 1] + n + 1;
            }
            return;
        }

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        re2::RE2::Options regexp_options;
        regexp_options.set_log_errors(false); /// don't write error messages to stderr

        re2::RE2 searcher(needle, regexp_options);
        if (!searcher.ok())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The pattern argument is not a valid re2 pattern: {}", searcher.error());

        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);
        Instructions instructions = createInstructions(replacement, num_captures);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t from = i * n;
            const char * hs_data = reinterpret_cast<const char *>(haystack_data.data() + from);
            const size_t hs_length = n;

            processString(hs_data, hs_length, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }
};

}
