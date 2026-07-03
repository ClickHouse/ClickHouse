#pragma once

#include <limits>
#include <type_traits>
#include <base/types.h>
#include <Common/Volnitsky.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/isValidUTF8.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnNumbers.h>
#include <Functions/Regexps.h>
#include <Interpreters/JIT/CompileRegexp.h>

namespace DB
{

namespace impl
{

/// Does the [I]LIKE pattern match every haystack, i.e. is it a non-empty run of '%' wildcards (`%`, `%%`, `%%%`, ...)?
inline bool likePatternMatchesEverything(std::string_view pattern)
{
    return !pattern.empty() && pattern.find_first_not_of('%') == std::string_view::npos;
}

/// Is the [I]LIKE expression equivalent to a substring search?
inline bool likePatternIsSubstring(std::string_view pattern, String & res)
{
    /// TODO: ignore multiple leading or trailing %
    if (pattern.size() < 2 || !pattern.starts_with('%') || !pattern.ends_with('%'))
        return false;

    res.clear();
    res.reserve(pattern.size() - 2);

    const char * pos = pattern.data() + 1;
    const char * const end = pattern.data() + pattern.size() - 1;

    while (pos < end)
    {
        switch (*pos)
        {
            case '%':
            case '_':
                return false;
            case '\\':
                ++pos;
                if (pos == end)
                    /// pattern ends with \% --> trailing % is to be taken literally and pattern doesn't qualify for substring search
                    return false;

                switch (*pos)
                {
                    /// Known LIKE escape sequences:
                    case '%':
                    case '_':
                    case '\\':
                        res += *pos;
                        break;
                    /// For all other escape sequences, the backslash loses its special meaning
                    default:
                        res += '\\';
                        res += *pos;
                        break;
                }

                break;
            default:
                res += *pos;
                break;
        }
        ++pos;
    }

    return true;
}

}

// For more readable instantiations of MatchImpl<>
struct MatchTraits
{
enum class Syntax : uint8_t
{
    Like,
    Re2
};

enum class Case : uint8_t
{
    Sensitive,
    Insensitive
};

enum class Result : uint8_t
{
    DontNegate,
    Negate
};
};

/** We want to run regexp search for whole columns by one call (as implemented in function 'position')
  */
template <typename Name, MatchTraits::Syntax syntax_, MatchTraits::Case case_, MatchTraits::Result result_>
struct MatchImpl
{
    static constexpr bool use_default_implementation_for_constants = true;
    static constexpr bool supports_start_pos = false;
    static constexpr auto name = Name::name;

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {2};}

    using ResultType = UInt8;

    static constexpr bool is_like = (syntax_ == MatchTraits::Syntax::Like);
    static constexpr bool case_insensitive = (case_ == MatchTraits::Case::Insensitive);
    static constexpr bool negate = (result_ == MatchTraits::Result::Negate);

    using Searcher = std::conditional_t<case_insensitive, VolnitskyCaseInsensitiveUTF8, VolnitskyUTF8>;

#ifndef NDEBUG
    /// Assert that the JIT-compiled matcher agrees with RE2 for every row. Debug builds only.
    static void verifyMatchAgainstRE2(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const String & needle,
        const PaddedPODArray<UInt8> & res,
        size_t input_rows_count)
    {
        const auto regexp = OptimizedRegularExpression(Regexps::createRegexp<is_like, /*no_capture*/ true, case_insensitive>(needle));
        size_t prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * data = reinterpret_cast<const char *>(&haystack_data[prev_offset]);
            const size_t size = haystack_offsets[i] - prev_offset;
            /// Byte-wise matching only matches RE2 (UTF-8 mode) on valid UTF-8; invalid UTF-8 may differ.
            if (UTF8::isValidUTF8(&haystack_data[prev_offset], size))
            {
                const bool re2_match = regexp.match(data, size);
                chassert(res[i] == static_cast<UInt8>(negate ^ re2_match));
            }
            prev_offset = haystack_offsets[i];
        }
    }
#endif

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const String & needle,
        [[maybe_unused]] const ColumnPtr & start_pos_,
        PaddedPODArray<UInt8> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t input_rows_count,
        size_t regexp_jit_min_count = std::numeric_limits<size_t>::max())
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        chassert(!res_null);

        chassert(res.size() == haystack_offsets.size());
        chassert(res.size() == input_rows_count);
        chassert(start_pos_ == nullptr);

        if (input_rows_count == 0)
            return;

        /// Shortcut for the silly but practical case that the pattern matches everything/nothing independently of the haystack:
        /// - col [not] [i]like '%' / '%%' / any run of '%'
        /// - match(col, '.*')
        /// Kept ahead of the JIT fast path below: `.*` / `.*?` belong to the JIT-compilable subset, but this
        /// constant fill is `O(rows)` with no per-row work, whereas the compiled matcher would run (and set up
        /// captures) for every row. Letting the shortcut own these patterns keeps the cheap path cheap.
        if ((is_like && impl::likePatternMatchesEverything(needle))
            || (!is_like && (needle == ".*" || needle == ".*?")))
        {
            for (auto & x : res)
                x = !negate;
            return;
        }

        /// Fast path: a JIT-compiled matcher for a simple regular expression (see `CompileRegexp.h`).
        /// LIKE patterns are not regular expressions, so they never take this path.
        if constexpr (!is_like)
        {
            if (regexp_jit_min_count != std::numeric_limits<size_t>::max())
            {
                /// `match`/`extract` build RE2 with `RE_DOT_NL` (see `Regexps::createRegexp`), so `.` matches newline.
                if (auto matcher = getRegexpJITMatcher(needle, case_insensitive, /* dot_all */ true, regexp_jit_min_count))
                {
                    /// Allocated once per call (not per row); reused for every row below.
                    VectorWithMemoryTracking<const uint8_t *> capture_starts(matcher.num_captures);
                    VectorWithMemoryTracking<const uint8_t *> capture_ends(matcher.num_captures);

                    size_t prev_offset = 0;
                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        const auto * str_begin = reinterpret_cast<const uint8_t *>(haystack_data.data() + prev_offset);
                        const auto * str_end = reinterpret_cast<const uint8_t *>(haystack_data.data() + haystack_offsets[i]);
                        const bool matched = matcher.func(str_begin, str_end, str_begin, capture_starts.data(), capture_ends.data()) == 1;
                        res[i] = negate ^ matched;
                        prev_offset = haystack_offsets[i];
                    }

#ifndef NDEBUG
                    verifyMatchAgainstRE2(haystack_data, haystack_offsets, needle, res, input_rows_count);
#endif
                    return;
                }
            }
        }

        /// Special case that the [I]LIKE expression reduces to finding a substring in a string
        String strstr_pattern;
        if (is_like && impl::likePatternIsSubstring(needle, strstr_pattern))
        {
            const UInt8 * const begin = haystack_data.data();
            const UInt8 * const end = haystack_data.data() + haystack_data.size();
            const UInt8 * pos = begin;

            /// The current index in the array of strings.
            size_t i = 0;

            /// TODO You need to make that `searcher` is common to all the calls of the function.
            Searcher searcher(strstr_pattern.data(), strstr_pattern.size(), end - pos);

            /// We will search for the next occurrence in all rows at once.
            while (pos < end && end != (pos = searcher.search(pos, end - pos)))
            {
                /// Let's determine which index it refers to.
                while (begin + haystack_offsets[i] <= pos)
                {
                    res[i] = negate;
                    ++i;
                }

                /// We check that the entry does not pass through the boundaries of strings.
                if (pos + strstr_pattern.size() <= begin + haystack_offsets[i])
                    res[i] = !negate;
                else
                    res[i] = negate;

                pos = begin + haystack_offsets[i];
                ++i;
            }

            /// Tail, in which there can be no substring.
            if (i < res.size())
                memset(&res[i], negate, (res.size() - i) * sizeof(res[0]));

            return;
        }

        const auto & regexp = OptimizedRegularExpression(Regexps::createRegexp<is_like, /*no_capture*/ true, case_insensitive>(needle));

        String required_substring;
        bool is_trivial = false;
        bool required_substring_is_prefix = false; /// for `anchored` execution of the regexp.

        regexp.getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);

        if (required_substring.empty())
        {
            if (!regexp.getRE2()) /// An empty regexp. Always matches.
                memset(res.data(), !negate, input_rows_count * sizeof(res[0]));
            else
            {
                size_t prev_offset = 0;
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    const bool match = regexp.getRE2()->Match(
                        {reinterpret_cast<const char *>(&haystack_data[prev_offset]), haystack_offsets[i] - prev_offset},
                        0,
                        haystack_offsets[i] - prev_offset,
                        re2::RE2::UNANCHORED,
                        nullptr,
                        0);
                    res[i] = negate ^ match;

                    prev_offset = haystack_offsets[i];
                }
            }
        }
        else
        {
            /// NOTE This almost matches with the case of impl::likePatternIsSubstring.

            const UInt8 * const begin = haystack_data.data();
            const UInt8 * const end = haystack_data.begin() + haystack_data.size();
            const UInt8 * pos = begin;

            /// The current index in the array of strings.
            size_t i = 0;

            Searcher searcher(required_substring.data(), required_substring.size(), end - pos);

            /// We will search for the next occurrence in all rows at once.
            while (pos < end && end != (pos = searcher.search(pos, end - pos)))
            {
                /// Determine which index it refers to.
                while (begin + haystack_offsets[i] <= pos)
                {
                    res[i] = negate;
                    ++i;
                }

                /// We check that the entry does not pass through the boundaries of strings.
                if (pos + required_substring.size() <= begin + haystack_offsets[i])
                {
                    /// And if it does not, if necessary, we check the regexp.
                    if (is_trivial)
                        res[i] = !negate;
                    else
                    {
                        const char * str_data = reinterpret_cast<const char *>(&haystack_data[haystack_offsets[i - 1]]);
                        size_t str_size = haystack_offsets[i] - haystack_offsets[i - 1];

                        /** Even in the case of `required_substring_is_prefix` use UNANCHORED check for regexp,
                          *  so that it can match when `required_substring` occurs into the string several times,
                          *  and at the first occurrence, the regexp is not a match.
                          */
                        const size_t start_pos = (required_substring_is_prefix) ? (reinterpret_cast<const char *>(pos) - str_data) : 0;
                        const size_t end_pos = str_size;

                        const bool match = regexp.getRE2()->Match(
                            {str_data, str_size},
                            start_pos,
                            end_pos,
                            re2::RE2::UNANCHORED,
                            nullptr,
                            0);
                        res[i] = negate ^ match;
                    }
                }
                else
                    res[i] = negate;

                pos = begin + haystack_offsets[i];
                ++i;
            }

            /// Tail, in which there can be no substring.
            if (i < res.size())
                memset(&res[i], negate, (res.size() - i) * sizeof(res[0]));
        }
    }

    /// Very carefully crafted copy-paste.
    static void vectorFixedConstant(
        const ColumnString::Chars & haystack,
        size_t N,
        const String & needle,
        PaddedPODArray<UInt8> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        chassert(!res_null);

        chassert(res.size() == haystack.size() / N);
        chassert(res.size() == input_rows_count);

        if (input_rows_count == 0)
            return;

        /// Shortcut for the silly but practical case that the pattern matches everything/nothing independently of the haystack:
        /// - col [not] [i]like '%' / '%%' / any run of '%'
        /// - match(col, '.*')
        if ((is_like && impl::likePatternMatchesEverything(needle)) || (!is_like && (needle == ".*" || needle == ".*?")))
        {
            for (auto & x : res)
                x = !negate;
            return;
        }

        /// Special case that the [I]LIKE expression reduces to finding a substring in a string
        String strstr_pattern;
        if (is_like && impl::likePatternIsSubstring(needle, strstr_pattern))
        {
            const UInt8 * const begin = haystack.data();
            const UInt8 * const end = haystack.data() + haystack.size();
            const UInt8 * pos = begin;

            size_t i = 0;
            const UInt8 * next_pos = begin;

            /// If needle is larger than string size - it cannot be found.
            if (strstr_pattern.size() <= N)
            {
                Searcher searcher(strstr_pattern.data(), strstr_pattern.size(), end - pos);

                /// We will search for the next occurrence in all rows at once.
                while (pos < end && end != (pos = searcher.search(pos, end - pos)))
                {
                    /// Let's determine which index it refers to.
                    while (next_pos + N <= pos)
                    {
                        res[i] = negate;
                        next_pos += N;
                        ++i;
                    }
                    next_pos += N;

                    /// We check that the entry does not pass through the boundaries of strings.
                    if (pos + strstr_pattern.size() <= next_pos)
                        res[i] = !negate;
                    else
                        res[i] = negate;

                    pos = next_pos;
                    ++i;
                }
            }

            /// Tail, in which there can be no substring.
            if (i < res.size())
                memset(&res[i], negate, (res.size() - i) * sizeof(res[0]));

            return;
        }

        const auto & regexp = OptimizedRegularExpression(Regexps::createRegexp<is_like, /*no_capture*/ true, case_insensitive>(needle));

        String required_substring;
        bool is_trivial = false;
        bool required_substring_is_prefix = false; /// for `anchored` execution of the regexp.

        regexp.getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);

        if (required_substring.empty())
        {
            if (!regexp.getRE2()) /// An empty regexp. Always matches.
                memset(res.data(), !negate, input_rows_count * sizeof(res[0]));
            else
            {
                size_t offset = 0;
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    const bool match = regexp.getRE2()->Match(
                        {reinterpret_cast<const char *>(&haystack[offset]), N},
                        0,
                        N,
                        re2::RE2::UNANCHORED,
                        nullptr,
                        0);
                    res[i] = negate ^ match;

                    offset += N;
                }
            }
        }
        else
        {
            /// NOTE This almost matches with the case of likePatternIsSubstring.

            const UInt8 * const begin = haystack.data();
            const UInt8 * const end = haystack.data() + haystack.size();
            const UInt8 * pos = begin;

            size_t i = 0;
            const UInt8 * next_pos = begin;

            /// If required substring is larger than string size - it cannot be found.
            if (required_substring.size() <= N)
            {
                Searcher searcher(required_substring.data(), required_substring.size(), end - pos);

                /// We will search for the next occurrence in all rows at once.
                while (pos < end && end != (pos = searcher.search(pos, end - pos)))
                {
                    /// Let's determine which index it refers to.
                    while (next_pos + N <= pos)
                    {
                        res[i] = negate;
                        next_pos += N;
                        ++i;
                    }
                    next_pos += N;

                    if (pos + required_substring.size() <= next_pos)
                    {
                        /// And if it does not, if necessary, we check the regexp.
                        if (is_trivial)
                            res[i] = !negate;
                        else
                        {
                            const char * str_data = reinterpret_cast<const char *>(next_pos - N);

                            /** Even in the case of `required_substring_is_prefix` use UNANCHORED check for regexp,
                            *  so that it can match when `required_substring` occurs into the string several times,
                            *  and at the first occurrence, the regexp is not a match.
                            */
                            const size_t start_pos = (required_substring_is_prefix) ? (reinterpret_cast<const char *>(pos) - str_data) : 0;
                            const size_t end_pos = N;

                            const bool match = regexp.getRE2()->Match(
                                {str_data, N},
                                start_pos,
                                end_pos,
                                re2::RE2::UNANCHORED,
                                nullptr,
                                0);
                            res[i] = negate ^ match;
                        }
                    }
                    else
                        res[i] = negate;

                    pos = next_pos;
                    ++i;
                }
            }

            /// Tail, in which there can be no substring.
            if (i < res.size())
                memset(&res[i], negate, (res.size() - i) * sizeof(res[0]));
        }
    }

    /// Match a single haystack against a single needle pattern.
    /// Shared by `vectorVector`, `vectorFixedVector` and `constantVector` (the cases where the needle is non-constant)
    /// so that their per-row logic stays in sync. `regexp` and `required_substr` are reused scratch storage owned by the caller.
    static UInt8 matchOneRow(
        const char * haystack_data,
        size_t haystack_length,
        std::string_view needle,
        Regexps::LocalCacheTable & cache,
        Regexps::RegexpPtr & regexp,
        String & required_substr)
    {
        const auto * const haystack_begin = reinterpret_cast<const UInt8 *>(haystack_data);
        const auto * const haystack_end = haystack_begin + haystack_length;

        /// Shortcut for the silly but practical case that the pattern matches everything/nothing independently of the haystack:
        /// - 'foo' [not] [i]like '%' / '%%' / any run of '%'
        /// - match('foo', '.*')
        if ((is_like && impl::likePatternMatchesEverything(needle))
            || (!is_like && (needle == ".*" || needle == ".*?")))
            return !negate;

        if (is_like && impl::likePatternIsSubstring(needle, required_substr))
        {
            if (required_substr.size() > haystack_length)
                return negate;

            Searcher searcher(required_substr.data(), required_substr.size(), haystack_length);
            const auto * match = searcher.search(haystack_begin, haystack_length);
            return negate ^ (match != haystack_end);
        }

        regexp = cache.getOrSet<is_like, /*no_capture*/ true, case_insensitive>(String(needle));

        bool is_trivial = false;
        bool required_substring_is_prefix = false; /// for `anchored` execution of the regexp.
        regexp->getAnalyzeResult(required_substr, is_trivial, required_substring_is_prefix);

        if (required_substr.empty())
        {
            if (!regexp->getRE2()) /// An empty regexp. Always matches.
                return !negate;

            const bool match = regexp->getRE2()->Match(
                {haystack_data, haystack_length}, 0, haystack_length, re2::RE2::UNANCHORED, nullptr, 0);
            return negate ^ match;
        }

        Searcher searcher(required_substr.data(), required_substr.size(), haystack_length);
        const auto * match = searcher.search(haystack_begin, haystack_length);

        if (match == haystack_end)
            return negate; /// no match

        if (is_trivial)
            return !negate; /// no wildcards in pattern

        const size_t start_pos = required_substring_is_prefix ? (match - haystack_begin) : 0;
        const size_t end_pos = haystack_length;
        const bool match2 = regexp->getRE2()->Match(
            {haystack_data, haystack_length}, start_pos, end_pos, re2::RE2::UNANCHORED, nullptr, 0);
        return negate ^ match2;
    }

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offset,
        [[maybe_unused]] const ColumnPtr & start_pos_,
        PaddedPODArray<UInt8> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        chassert(!res_null);

        chassert(haystack_offsets.size() == needle_offset.size());
        chassert(res.size() == haystack_offsets.size());
        chassert(res.size() == input_rows_count);
        chassert(start_pos_ == nullptr);

        if (input_rows_count == 0)
            return;

        String required_substr;

        size_t prev_haystack_offset = 0;
        size_t prev_needle_offset = 0;

        Regexps::LocalCacheTable cache;
        Regexps::RegexpPtr regexp;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto * const cur_haystack_data = reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]);
            const size_t cur_haystack_length = haystack_offsets[i] - prev_haystack_offset;

            const std::string_view needle(
                reinterpret_cast<const char *>(&needle_data[prev_needle_offset]),
                needle_offset[i] - prev_needle_offset);

            res[i] = matchOneRow(cur_haystack_data, cur_haystack_length, needle, cache, regexp, required_substr);

            prev_haystack_offset = haystack_offsets[i];
            prev_needle_offset = needle_offset[i];
        }
    }

    static void vectorFixedVector(
        const ColumnString::Chars & haystack,
        size_t N,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offset,
        [[maybe_unused]] const ColumnPtr & start_pos_,
        PaddedPODArray<UInt8> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        chassert(!res_null);

        chassert(res.size() == input_rows_count);
        chassert(res.size() == haystack.size() / N);
        chassert(res.size() == needle_offset.size());
        chassert(start_pos_ == nullptr);

        if (haystack.empty())
            return;

        String required_substr;

        size_t prev_haystack_offset = 0;
        size_t prev_needle_offset = 0;

        Regexps::LocalCacheTable cache;
        Regexps::RegexpPtr regexp;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto * const cur_haystack_data = reinterpret_cast<const char *>(&haystack[prev_haystack_offset]);
            const size_t cur_haystack_length = N;

            const std::string_view needle(
                reinterpret_cast<const char *>(&needle_data[prev_needle_offset]),
                needle_offset[i] - prev_needle_offset);

            res[i] = matchOneRow(cur_haystack_data, cur_haystack_length, needle, cache, regexp, required_substr);

            prev_haystack_offset += N;
            prev_needle_offset = needle_offset[i];
        }
    }

    static void constantVector(
        const String & haystack,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        [[maybe_unused]] const ColumnPtr & start_pos_,
        PaddedPODArray<UInt8> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        chassert(!res_null);
        chassert(res.size() == needle_offsets.size());
        chassert(res.size() == input_rows_count);
        chassert(start_pos_ == nullptr);

        if (input_rows_count == 0)
            return;

        const char * const haystack_data = haystack.data();
        const size_t haystack_length = haystack.size();

        String required_substr;

        size_t prev_needle_offset = 0;

        Regexps::LocalCacheTable cache;
        Regexps::RegexpPtr regexp;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const std::string_view needle(
                reinterpret_cast<const char *>(&needle_data[prev_needle_offset]),
                needle_offsets[i] - prev_needle_offset);

            res[i] = matchOneRow(haystack_data, haystack_length, needle, cache, regexp, required_substr);

            prev_needle_offset = needle_offsets[i];
        }
    }
};

}
