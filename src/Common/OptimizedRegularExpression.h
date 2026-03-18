#pragma once

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <Common/StringSearcher.h>
#include <Common/re2.h>
#include "config.h"

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

class OptimizedRegularExpression
{
public:
    enum Options
    {
        RE_CASELESS   = 0x00000001,
        RE_NO_CAPTURE = 0x00000010,
        RE_DOT_NL     = 0x00000100
    };

    using Match = OptimizedRegularExpressionDetails::Match;
    using MatchVec = std::vector<Match>;

    OptimizedRegularExpression(const std::string & regexp_, int options = 0); /// NOLINT
    /// StringSearcher store pointers to required_substring, it must be updated on move.
    OptimizedRegularExpression(OptimizedRegularExpression && rhs) noexcept;
    OptimizedRegularExpression(const OptimizedRegularExpression & rhs) = delete;

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
    const std::unique_ptr<re2::RE2> & getRE2() const { return re2; }

    void getAnalyzeResult(std::string & out_required_substring, bool & out_is_trivial, bool & out_required_substring_is_prefix) const
    {
        out_required_substring = required_substring;
        out_is_trivial = is_trivial;
        out_required_substring_is_prefix = required_substring_is_prefix;
    }

    /// analyze function will extract the longest string literal or multiple alternative string literals from regexp for pre-checking if
    /// a string contains the string literal(s). If not, we can tell this string can never match the regexp.
    static void analyze(
        std::string_view regexp_,
        std::string & required_substring,
        bool & is_trivial,
        bool & required_substring_is_prefix,
        std::vector<std::string> & alternatives);

private:
    bool is_trivial;
    bool required_substring_is_prefix;
    bool is_case_insensitive;
    std::string required_substring;
    std::optional<DB::ASCIICaseSensitiveStringSearcher> case_sensitive_substring_searcher;
    std::optional<DB::ASCIICaseInsensitiveStringSearcher> case_insensitive_substring_searcher;
    std::unique_ptr<re2::RE2> re2;
    unsigned number_of_subpatterns;
};
