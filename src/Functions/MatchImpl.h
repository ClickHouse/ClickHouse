#pragma once

#include <type_traits>
#include <common/types.h>
#include <Common/Volnitsky.h>
#include <Columns/ColumnString.h>
#include "Regexps.h"

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
    extern const int ILLEGAL_COLUMN;
}


/// Is the [I]LIKE expression reduced to finding a substring in a string?
static inline bool likePatternIsStrstr(const String & pattern, String & res)
{
    res = "";

    if (pattern.size() < 2 || pattern.front() != '%' || pattern.back() != '%')
        return false;

    res.reserve(pattern.size() * 2);

    const char * pos = pattern.data();
    const char * end = pos + pattern.size();

    ++pos;
    --end;

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
                    return false;
                else
                    res += *pos;
                break;
            default:
                res += *pos;
                break;
        }
        ++pos;
    }

    return true;
}

/** 'like' - if true, treat pattern as SQL LIKE or ILIKE; if false - treat pattern as re2 regexp.
  * NOTE: We want to run regexp search for whole block by one call (as implemented in function 'position')
  *  but for that, regexp engine must support \0 bytes and their interpretation as string boundaries.
  */
template <bool like, bool revert = false, bool case_insensitive = false>
struct MatchImpl
{
    static constexpr bool use_default_implementation_for_constants = true;

    using ResultType = UInt8;

    using Searcher = std::conditional_t<case_insensitive,
          VolnitskyCaseInsensitiveUTF8,
          VolnitskyUTF8>;

    static void vectorConstant(
        const ColumnString::Chars & data, const ColumnString::Offsets & offsets, const std::string & pattern, PaddedPODArray<UInt8> & res)
    {
        if (offsets.empty())
            return;

        String strstr_pattern;

        /// A simple case where the [I]LIKE expression reduces to finding a substring in a string
        if (like && likePatternIsStrstr(pattern, strstr_pattern))
        {
            const UInt8 * begin = data.data();
            const UInt8 * pos = begin;
            const UInt8 * end = pos + data.size();

            /// The current index in the array of strings.
            size_t i = 0;

            /// TODO You need to make that `searcher` is common to all the calls of the function.
            Searcher searcher(strstr_pattern.data(), strstr_pattern.size(), end - pos);

            /// We will search for the next occurrence in all rows at once.
            while (pos < end && end != (pos = searcher.search(pos, end - pos)))
            {
                /// Let's determine which index it refers to.
                while (begin + offsets[i] <= pos)
                {
                    res[i] = revert;
                    ++i;
                }

                /// We check that the entry does not pass through the boundaries of strings.
                if (pos + strstr_pattern.size() < begin + offsets[i])
                    res[i] = !revert;
                else
                    res[i] = revert;

                pos = begin + offsets[i];
                ++i;
            }

            /// Tail, in which there can be no substring.
            if (i < res.size())
                memset(&res[i], revert, (res.size() - i) * sizeof(res[0]));
        }
        else
        {
            size_t size = offsets.size();

            constexpr int flags = case_insensitive ?
                Regexps::Regexp::RE_CASELESS : 0;

            auto regexp = Regexps::get<like, true>(pattern, flags);

            std::string required_substring;
            bool is_trivial;
            bool required_substring_is_prefix; /// for `anchored` execution of the regexp.

            regexp->getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);

            if (required_substring.empty())
            {
                if (!regexp->getRE2()) /// An empty regexp. Always matches.
                {
                    if (size)
                        memset(res.data(), 1, size * sizeof(res[0]));
                }
                else
                {
                    size_t prev_offset = 0;
                    for (size_t i = 0; i < size; ++i)
                    {
                        res[i] = revert
                            ^ regexp->getRE2()->Match(
                                  re2_st::StringPiece(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1),
                                  0,
                                  offsets[i] - prev_offset - 1,
                                  re2_st::RE2::UNANCHORED,
                                  nullptr,
                                  0);

                        prev_offset = offsets[i];
                    }
                }
            }
            else
            {
                /// NOTE This almost matches with the case of LikePatternIsStrstr.

                const UInt8 * begin = data.data();
                const UInt8 * pos = begin;
                const UInt8 * end = pos + data.size();

                /// The current index in the array of strings.
                size_t i = 0;

                Searcher searcher(required_substring.data(), required_substring.size(), end - pos);

                /// We will search for the next occurrence in all rows at once.
                while (pos < end && end != (pos = searcher.search(pos, end - pos)))
                {
                    /// Determine which index it refers to.
                    while (begin + offsets[i] <= pos)
                    {
                        res[i] = revert;
                        ++i;
                    }

                    /// We check that the entry does not pass through the boundaries of strings.
                    if (pos + strstr_pattern.size() < begin + offsets[i])
                    {
                        /// And if it does not, if necessary, we check the regexp.

                        if (is_trivial)
                            res[i] = !revert;
                        else
                        {
                            const char * str_data = reinterpret_cast<const char *>(&data[offsets[i - 1]]);
                            size_t str_size = offsets[i] - offsets[i - 1] - 1;

                            /** Even in the case of `required_substring_is_prefix` use UNANCHORED check for regexp,
                              *  so that it can match when `required_substring` occurs into the string several times,
                              *  and at the first occurrence, the regexp is not a match.
                              */

                            if (required_substring_is_prefix)
                                res[i] = revert
                                    ^ regexp->getRE2()->Match(
                                          re2_st::StringPiece(str_data, str_size),
                                          reinterpret_cast<const char *>(pos) - str_data,
                                          str_size,
                                          re2_st::RE2::UNANCHORED,
                                          nullptr,
                                          0);
                            else
                                res[i] = revert
                                    ^ regexp->getRE2()->Match(
                                          re2_st::StringPiece(str_data, str_size), 0, str_size, re2_st::RE2::UNANCHORED, nullptr, 0);
                        }
                    }
                    else
                        res[i] = revert;

                    pos = begin + offsets[i];
                    ++i;
                }

                if (i < res.size())
                    memset(&res[i], revert, (res.size() - i) * sizeof(res[0]));
            }
        }
    }

    /// Very carefully crafted copy-paste.
    static void vectorFixedConstant(
        const ColumnString::Chars & data, size_t n, const std::string & pattern, PaddedPODArray<UInt8> & res)
    {
        if (data.empty())
            return;

        String strstr_pattern;
        /// A simple case where the LIKE expression reduces to finding a substring in a string
        if (like && likePatternIsStrstr(pattern, strstr_pattern))
        {
            const UInt8 * begin = data.data();
            const UInt8 * pos = begin;
            const UInt8 * end = pos + data.size();

            size_t i = 0;
            const UInt8 * next_pos = begin;

            /// If pattern is larger than string size - it cannot be found.
            if (strstr_pattern.size() <= n)
            {
                Searcher searcher(strstr_pattern.data(), strstr_pattern.size(), end - pos);

                /// We will search for the next occurrence in all rows at once.
                while (pos < end && end != (pos = searcher.search(pos, end - pos)))
                {
                    /// Let's determine which index it refers to.
                    while (next_pos + n <= pos)
                    {
                        res[i] = revert;
                        next_pos += n;
                        ++i;
                    }
                    next_pos += n;

                    /// We check that the entry does not pass through the boundaries of strings.
                    if (pos + strstr_pattern.size() <= next_pos)
                        res[i] = !revert;
                    else
                        res[i] = revert;

                    pos = next_pos;
                    ++i;
                }
            }

            /// Tail, in which there can be no substring.
            if (i < res.size())
                memset(&res[i], revert, (res.size() - i) * sizeof(res[0]));
        }
        else
        {
            size_t size = data.size() / n;

            auto regexp = Regexps::get<like, true>(pattern);

            std::string required_substring;
            bool is_trivial;
            bool required_substring_is_prefix; /// for `anchored` execution of the regexp.

            regexp->getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);

            if (required_substring.empty())
            {
                if (!regexp->getRE2()) /// An empty regexp. Always matches.
                {
                    if (size)
                        memset(res.data(), 1, size * sizeof(res[0]));
                }
                else
                {
                    size_t offset = 0;
                    for (size_t i = 0; i < size; ++i)
                    {
                        res[i] = revert
                            ^ regexp->getRE2()->Match(
                                  re2_st::StringPiece(reinterpret_cast<const char *>(&data[offset]), n),
                                  0,
                                  n,
                                  re2_st::RE2::UNANCHORED,
                                  nullptr,
                                  0);

                        offset += n;
                    }
                }
            }
            else
            {
                /// NOTE This almost matches with the case of LikePatternIsStrstr.

                const UInt8 * begin = data.data();
                const UInt8 * pos = begin;
                const UInt8 * end = pos + data.size();

                size_t i = 0;
                const UInt8 * next_pos = begin;

                /// If required substring is larger than string size - it cannot be found.
                if (strstr_pattern.size() <= n)
                {
                    Searcher searcher(required_substring.data(), required_substring.size(), end - pos);

                    /// We will search for the next occurrence in all rows at once.
                    while (pos < end && end != (pos = searcher.search(pos, end - pos)))
                    {
                        /// Let's determine which index it refers to.
                        while (next_pos + n <= pos)
                        {
                            res[i] = revert;
                            next_pos += n;
                            ++i;
                        }
                        next_pos += n;

                        if (pos + strstr_pattern.size() <= next_pos)
                        {
                            /// And if it does not, if necessary, we check the regexp.

                            if (is_trivial)
                                res[i] = !revert;
                            else
                            {
                                const char * str_data = reinterpret_cast<const char *>(next_pos - n);

                                /** Even in the case of `required_substring_is_prefix` use UNANCHORED check for regexp,
                                *  so that it can match when `required_substring` occurs into the string several times,
                                *  and at the first occurrence, the regexp is not a match.
                                */

                                if (required_substring_is_prefix)
                                    res[i] = revert
                                        ^ regexp->getRE2()->Match(
                                            re2_st::StringPiece(str_data, n),
                                            reinterpret_cast<const char *>(pos) - str_data,
                                            n,
                                            re2_st::RE2::UNANCHORED,
                                            nullptr,
                                            0);
                                else
                                    res[i] = revert
                                        ^ regexp->getRE2()->Match(
                                            re2_st::StringPiece(str_data, n), 0, n, re2_st::RE2::UNANCHORED, nullptr, 0);
                            }
                        }
                        else
                            res[i] = revert;

                        pos = next_pos;
                        ++i;
                    }
                }

                /// Tail, in which there can be no substring.
                if (i < res.size())
                    memset(&res[i], revert, (res.size() - i) * sizeof(res[0]));
            }
        }
    }

    template <typename... Args>
    static void vectorVector(Args &&...)
    {
        throw Exception("Functions 'like' and 'match' don't support non-constant needle argument", ErrorCodes::ILLEGAL_COLUMN);
    }

    /// Search different needles in single haystack.
    template <typename... Args>
    static void constantVector(Args &&...)
    {
        throw Exception("Functions 'like' and 'match' don't support non-constant needle argument", ErrorCodes::ILLEGAL_COLUMN);
    }
};

}
