#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <Common/re2.h>

namespace DB
{

class CaseSensitiveStringSearcher;
class ASCIICaseInsensitiveStringSearcher;

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

/// How `match` evaluates the pattern. Everything except `General` is a comparison against a literal, without re2.
enum class RegexpMatchKind : uint8_t
{
    General,   /// The re2 engine is required.
    Substring, /// The whole pattern is a literal: matches iff the literal occurs anywhere (same as `is_trivial`).
    Exact,     /// `^literal$`: matches iff the subject is equal to the literal.
    Prefix,    /// `^literal`: matches iff the subject starts with the literal.
    Suffix,    /// `literal$`: matches iff the subject ends with the literal.
};

inline bool isAnchoredLiteralMatchKind(RegexpMatchKind kind)
{
    /// A switch instead of a boolean expression, so that a new enum value cannot be silently ignored here.
    switch (kind)
    {
        case RegexpMatchKind::Exact:
        case RegexpMatchKind::Prefix:
        case RegexpMatchKind::Suffix:
            return true;
        case RegexpMatchKind::General:
        case RegexpMatchKind::Substring:
            return false;
    }
}

struct RegexpAnalysisResult
{
    std::string required_substring;
    bool is_trivial = false;
    bool has_capture = false;
    bool required_substring_is_prefix = false;
    std::vector<std::string> alternatives;

    /// For an anchored kind, `required_substring` is the whole literal, and it must occur at exactly one offset.
    RegexpMatchKind match_kind = RegexpMatchKind::General;
};

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
    ~OptimizedRegularExpression();

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
        return match(subject, subject_size, 0, matches, number_of_subpatterns + 1);
    }

    /// Search starting at `start_pos` (a byte offset into `subject`), while keeping the whole `subject` available as
    /// context. This is required for the correct evaluation of zero-width assertions such as `^`, `$` and `\b`: they
    /// must see the characters surrounding `start_pos`. Iterative "match all" functions must use this overload and
    /// advance `start_pos` instead of shifting the `subject` pointer, otherwise every continuation point looks like the
    /// beginning of the text. The returned match offsets are relative to `subject` (not to `start_pos`).
    unsigned match(const char * subject, size_t subject_size, size_t start_pos, MatchVec & matches) const
    {
        return match(subject, subject_size, start_pos, matches, number_of_subpatterns + 1);
    }

    bool match(const char * subject, size_t subject_size) const;
    bool match(const char * subject, size_t subject_size, Match & match) const;

    unsigned match(const char * subject, size_t subject_size, MatchVec & matches, unsigned limit) const
    {
        return match(subject, subject_size, 0, matches, limit);
    }

    unsigned match(const char * subject, size_t subject_size, size_t start_pos, MatchVec & matches, unsigned limit) const;

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
    static RegexpAnalysisResult analyze(std::string_view regexp_);

    RegexpMatchKind getMatchKind() const { return match_kind; }

private:
    bool isAnchoredLiteral() const { return isAnchoredLiteralMatchKind(match_kind); }

    /// Compares the subject against `required_substring` at the only offset where it can occur.
    bool matchAnchoredLiteral(const char * subject, size_t subject_size, size_t & match_offset) const;

    std::string required_substring;
    bool is_trivial;
    bool has_capture{};
    bool required_substring_is_prefix;
    bool is_case_insensitive;
    RegexpMatchKind match_kind;
    std::unique_ptr<CaseSensitiveStringSearcher> case_sensitive_substring_searcher;
    std::unique_ptr<ASCIICaseInsensitiveStringSearcher> case_insensitive_substring_searcher;
    std::unique_ptr<re2::RE2> re2;
    unsigned number_of_subpatterns;
};

/// Finds the next non-empty match of `regexp` at or after byte offset `pos` in `[data, data + length)`,
/// keeping the whole buffer as context so that `^`, `$` and `\b` work; match offsets are relative to `data`.
/// On success returns `true`, sets `match_start` / `match_length` and advances `pos` past the match (so `pos`
/// strictly increases). Returns `false` and leaves `pos` unchanged when there is no further match or the
/// leftmost match is empty (an empty match is treated as "no separator", like `splitByRegexp`).
/// `matches` is caller-owned scratch, reused to avoid per-call allocation.
inline bool nextRegexpMatch(
    const OptimizedRegularExpression & regexp,
    const char * data,
    size_t length,
    size_t & pos,
    size_t & match_start,
    size_t & match_length,
    OptimizedRegularExpression::MatchVec & matches)
{
    if (pos > length)
        return false;

    if (regexp.match(data, length, pos, matches) == 0 || matches.empty() || matches[0].length == 0)
        return false;

    match_start = matches[0].offset;
    match_length = matches[0].length;
    pos = match_start + match_length;
    return true;
}

}
