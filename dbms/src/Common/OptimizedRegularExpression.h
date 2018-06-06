#pragma once

#include <string>
#include <vector>
#include <memory>
#include <Common/config.h>
#include <re2/re2.h>
#if USE_RE2_ST
    #include <re2_st/re2.h>
#else
    #define re2_st re2
#endif


/** Uses two ways to optimize a regular expression:
  * 1. If the regular expression is trivial (reduces to finding a substring in a string),
  *     then replaces the search with strstr or strcasestr.
  * 2. If the regular expression contains a non-alternative substring of sufficient length,
  *     then before testing, strstr or strcasestr of sufficient length is used;
  *     regular expression is only fully checked if a substring is found.
  * 3. In other cases, the re2 engine is used.
  *
  * This makes sense, since strstr and strcasestr in libc for Linux are well optimized.
  *
  * Suitable if the following conditions are simultaneously met:
  * - if in most calls, the regular expression does not match;
  * - if the regular expression is compatible with the re2 engine;
  * - you can use at your own risk, since, probably, not all cases are taken into account.
  *
  * NOTE: Multi-character metasymbols such as \Pl are handled incorrectly.
  */

namespace OptimizedRegularExpressionDetails
{
    struct Match
    {
        std::string::size_type offset;
        std::string::size_type length;
    };
}

template <bool thread_safe>
class OptimizedRegularExpressionImpl
{
public:
    enum Options
    {
        RE_CASELESS        = 0x00000001,
        RE_NO_CAPTURE    = 0x00000010,
        RE_DOT_NL        = 0x00000100
    };

    using Match = OptimizedRegularExpressionDetails::Match;
    using MatchVec = std::vector<Match>;

    using RegexType = std::conditional_t<thread_safe, re2::RE2, re2_st::RE2>;
    using StringPieceType = std::conditional_t<thread_safe, re2::StringPiece, re2_st::StringPiece>;

    OptimizedRegularExpressionImpl(const std::string & regexp_, int options = 0);

    bool match(const std::string & subject) const
    {
        return match(subject.data(), subject.size());
    }

    bool match(const std::string & subject, Match & match_) const
    {
        return match(subject.data(), subject.size(), match_);
    }

    unsigned match(const std::string & subject, MatchVec & matches) const
    {
        return match(subject.data(), subject.size(), matches);
    }

    unsigned match(const char * subject, size_t subject_size, MatchVec & matches) const
    {
        return match(subject, subject_size, matches, number_of_subpatterns + 1);
    }

    bool match(const char * subject, size_t subject_size) const;
    bool match(const char * subject, size_t subject_size, Match & match) const;
    unsigned match(const char * subject, size_t subject_size, MatchVec & matches, unsigned limit) const;

    unsigned getNumberOfSubpatterns() const { return number_of_subpatterns; }

    /// Get the regexp re2 or nullptr if the pattern is trivial (for output to the log).
    const std::unique_ptr<RegexType> & getRE2() const { return re2; }

    static void analyze(const std::string & regexp_, std::string & required_substring, bool & is_trivial, bool & required_substring_is_prefix);

    void getAnalyzeResult(std::string & out_required_substring, bool & out_is_trivial, bool & out_required_substring_is_prefix) const
    {
        out_required_substring = required_substring;
        out_is_trivial = is_trivial;
        out_required_substring_is_prefix = required_substring_is_prefix;
    }

private:
    bool is_trivial;
    bool required_substring_is_prefix;
    bool is_case_insensitive;
    std::string required_substring;
    std::unique_ptr<RegexType> re2;
    unsigned number_of_subpatterns;
};

using OptimizedRegularExpression = OptimizedRegularExpressionImpl<true>;

#include "OptimizedRegularExpression.inl.h"
