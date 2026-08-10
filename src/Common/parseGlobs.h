#pragma once

#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace re2 { class RE2; }


namespace DB
{
    bool containsRangeGlob(const std::string & input);
    bool containsOnlyEnumGlobs(const std::string & input);
    bool hasExactlyOneBracketsExpansion(const std::string & input);

    /// Parse globs in string and make a regexp for it.
    std::string makeRegexpPatternFromGlobs(const std::string & initial_str_with_globs);

    /// Process {a,b,c...} globs:
    /// Don't match it against regex, but generate a,b,c strings instead and process each of them separately.
    /// E.g. for a string like `file{1,2,3}.csv` return vector of strings: {`file1.csv`,`file2.csv`,`file3.csv`}
    std::vector<std::string> expandSelectionGlob(const std::string & path);

/** Formal grammar of the glob expression syntax parsed by GlobAST::GlobString.
  *
  * This is the intended, regex-free language. It is derived from the legacy
  * makeRegexpPatternFromGlobs semantics (the parity oracle), cleaned up where legacy is
  * internally inconsistent. The character classes mirror the legacy enum_regex
  * `{([^{}*,]+[^{}*]*[^{}*,])}` and range_regex `{([\d]+\.\.[\d]+)}`.
  *
  *   glob         = { element } ;
  *
  *   element      = wildcard | range | enum | literal-char ;
  *
  *   wildcard     = globstar | "**" | "*" | "?" ;
  *   globstar     = "**" "/" ;   (* only when the "**" forms a whole path segment: it is
  *                                  preceded by '/' or the start of the glob *)
  *
  *   range        = "{" integer ".." integer "}" ;
  *   integer      = digit { digit } ;
  *   digit        = "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" ;
  *
  *   enum         = "{" enum-body "}" ;
  *   enum-body    = enum-edge { enum-mid } enum-edge ;   (* length >= 2 *)
  *   enum-edge    = ? any char except '{' '}' '*' ',' ? ;    (* first and last char *)
  *   enum-mid     = ? any char except '{' '}' '*' ? ;        (* ',' allowed -> empty alternatives *)
  *
  *   literal-char = ? any character ? ;   (* incl. a '{' that starts neither a range nor an enum *)
  *
  * An enum's alternatives are obtained by splitting enum-body on ','. Because both edges
  * are non-comma, the first and last alternatives are always non-empty; consecutive
  * interior commas produce empty interior alternatives ("{a,,b}" -> "a", "", "b").
  *
  * Disambiguation (resolution order at '{'):
  *   1. A whole-segment "**" followed by '/' is recognized as a globstar before "**";
  *      "**" is recognized before "*". A "**" adjacent to other characters in its
  *      segment (a preceding "a" or "?", or a run of 3+ stars) is not a globstar.
  *   2. At a '{':
  *      a. "{{" - the first '{' is a literal-char; scanning resumes at the second.
  *      b. otherwise consume the brace body up to the first '}':
  *         - if it is "{" integer ".." integer "}" -> range;
  *         - else if the body satisfies enum-body (>= 2 chars, no '{'/'}'/'*' inside,
  *           neither edge a ',') -> enum;
  *         - else the '{' is a literal-char and scanning resumes after it.
  *
  * Matching semantics (whole-string / FullMatch). NB: '**' below intentionally
  * reproduces the legacy matcher rather than the idealized "crosses '/', no braces"
  * grammar, for backward compatibility:
  *   literal-char c - matches exactly c.
  *   ?              - matches exactly one char, not '/'.
  *   *              - matches zero+ chars, none '/'.
  *   globstar       - a whole-segment "**" followed by '/': matches zero or more whole
  *                    directory components, each a run of non-'/' chars followed by '/',
  *                    so the consumed prefix is empty or ends with '/'. Mirrors
  *                    makeRegexpPatternFromGlobs and Bash `globstar`, where "**" is
  *                    special only as a complete path segment.
  *   **             - matches the legacy regex `[^/]*[^{}]*`: a run of non-'/' chars
  *                    followed by a run of non-'{','}' chars. It crosses '/', and a brace
  *                    is allowed only before the first '/'. For example a leading slash
  *                    then "**" matches both "/dir/file" and "/a{b}c" (the braces precede
  *                    any slash in the tail). A legacy-compatibility carryover, not an
  *                    idealized rule.
  *   range {M..N}   - matches a digit run whose value is in [min(M,N), max(M,N)],
  *                    subject to the zero-padding width rules.
  *   enum           - one alternative matches at the current position.
  *
  * The differential fuzzer GlobASTLegacyMatchFuzz in
  * gtest_makeRegexpPatternFromGlobs.cpp guards parity with the legacy oracle over the
  * input domain where legacy is correct; it excludes by construct the classes where
  * legacy is buggy relative to POSIX and this grammar is the cleaner behavior (brace
  * bodies of a legacy-escaped char such as "{-}", and wildcard runs mixing '?' with '*'
  * or containing 3+ consecutive stars). Exact "**" runs — including whole-segment
  * globstars — are inside the fuzzed domain.
  */
namespace GlobAST
{

/// A parsed "{M..N}" range: the endpoints as written (matching normalizes to
/// [min, max]) plus the zero-padding metadata of each endpoint's text.
struct Range
{
    size_t start = 0;
    size_t end = 0;

    bool start_zero_padded = false;
    size_t start_digit_count = 0;

    bool end_zero_padded = false;
    size_t end_digit_count = 0;
};

enum class WildcardType
{
    QUESTION,
    SINGLE_ASTERISK,
    DOUBLE_ASTERISK,
    /// A whole-segment "**/": matches zero or more directory components.
    GLOBSTAR,
};

enum class ExpressionType
{
    RANGE,
    CONSTANT,
    ENUM,
    WILDCARD,
};

using ExpressionData = std::variant<
    Range,
    std::string_view,
    std::vector<std::string_view>,
    WildcardType
>;

class Expression
{
public:
    explicit Expression (ExpressionData input): data(input) {}

    ExpressionType type() const
    {
        /// Pin the enum values to the variant alternative order that the cast relies on.
        static_assert(std::variant_size_v<ExpressionData> == 4);
        static_assert(std::is_same_v<std::variant_alternative_t<static_cast<size_t>(ExpressionType::RANGE), ExpressionData>, Range>);
        static_assert(std::is_same_v<std::variant_alternative_t<static_cast<size_t>(ExpressionType::CONSTANT), ExpressionData>, std::string_view>);
        static_assert(std::is_same_v<std::variant_alternative_t<static_cast<size_t>(ExpressionType::ENUM), ExpressionData>, std::vector<std::string_view>>);
        static_assert(std::is_same_v<std::variant_alternative_t<static_cast<size_t>(ExpressionType::WILDCARD), ExpressionData>, WildcardType>);
        return static_cast<ExpressionType>(data.index());
    }

    const ExpressionData& getData() const { return data; }

    std::string dump() const;

    size_t cardinality() const;

private:
    std::string dumpRange() const;
    std::string dumpEnum(char separator = ',') const;
    std::string dumpWildcard() const;

    ExpressionData data;
};

class GlobString
{
public:
    explicit GlobString(std::string input);

    /// Expressions contain string_views into input_data, so copies and moves are unsafe.
    GlobString(const GlobString &) = delete;
    GlobString & operator=(const GlobString &) = delete;
    GlobString(GlobString &&) = delete;
    GlobString & operator=(GlobString &&) = delete;

    const std::vector<Expression> & getExpressions() const { return expressions; }

    std::string dump() const;
    size_t cardinality() const;

    /// Number of strings expand(expand_ranges) would produce; unlike cardinality(),
    /// a wildcard does not saturate the product.
    size_t expansionSize(bool expand_ranges = false) const;

    /// Expand enum (and optionally range) globs into concrete strings via cartesian
    /// product; everything else is rendered as literal text. Throws above max_expansion.
    static constexpr size_t DEFAULT_MAX_EXPANSION = 1000;
    std::vector<std::string> expand(size_t max_expansion = DEFAULT_MAX_EXPANSION, bool expand_ranges = false) const;

    /// Whole-string match of a candidate against this glob pattern, without a regex.
    bool matches(std::string_view candidate) const;

    bool hasGlobs() const { return has_globs; }
    bool hasRanges() const { return has_ranges; }
    bool hasEnums() const { return has_enums; }
    bool hasQuestionOrAsterisk() const { return has_question_or_asterisk; }

    /// Byte offset of the first glob expression, or npos when the whole pattern is
    /// literal text (a literal brace group such as "{a}" does not count as a glob).
    size_t firstGlobPosition() const;

    /// True when the pattern holds exactly one enum group and no other brace text —
    /// the shape the legacy parser expands to exact keys (hasExactlyOneBracketsExpansion
    /// requires the enum's '{' to be the only one in the pattern). A literal brace group
    /// such as "{0}" fails the check even though it parses as constant text: the legacy
    /// path lists and filters such patterns, and probing exact keys instead would change
    /// missing-file semantics under strict `*_ignore_file_doesnt_exist = 0` mode.
    /// Note this does not check for '?' wildcards inside enum alternatives — callers
    /// must combine it with hasQuestionOrAsterisk (see GlobASTWildcardInsideEnums).
    bool hasExactlyOneEnum() const;

    /// True when at least one enum alternative contains a '/', i.e. the enum spans path
    /// segments (e.g. "{a/b,c/d}.csv"). Such a pattern is only meaningful after expansion:
    /// a directory-by-directory traversal splits the pattern at raw slashes, which cuts the
    /// enum body apart. Callers that may skip the expansion must reject these patterns.
    bool hasSlashInsideEnums() const;

private:
    /// Called once from the constructor; a second call would append duplicate expressions.
    void parse();

    std::string_view consumeConstantExpression(const std::string_view & input) const;
    std::string_view consumeMatcher(const std::string_view & input) const;

    std::vector<std::string_view> tryParseEnumMatcher(const std::string_view & input) const;
    std::optional<Range> tryParseRangeMatcher(const std::string_view & input) const;

    bool matchesImpl(std::string_view candidate, size_t pos, size_t expr_idx, std::vector<int8_t> & memo) const;

    std::vector<Expression> expressions;

    std::string input_data;

    bool has_globs = false;
    bool has_ranges = false;
    bool has_enums = false;
    bool has_question_or_asterisk = false;
};

}

/// Unified glob matcher that delegates to either GlobAST::GlobString (new)
/// or re2::RE2 (legacy), controlled by the use_glob_ast_parser setting.
class GlobMatcher
{
public:
    /// Create a matcher using the new AST-based glob parser.
    static GlobMatcher createNew(const std::string & glob_pattern);
    /// Same, adopting an already-parsed pattern instead of parsing it again.
    static GlobMatcher createNew(std::unique_ptr<GlobAST::GlobString> glob_string);

    static GlobMatcher createLegacy(const std::string & glob_pattern);

    bool matches(const std::string & candidate) const;

    GlobMatcher();
    ~GlobMatcher();

    GlobMatcher(const GlobMatcher &) = delete;
    GlobMatcher & operator=(const GlobMatcher &) = delete;
    GlobMatcher(GlobMatcher &&) noexcept;
    GlobMatcher & operator=(GlobMatcher &&) noexcept;

private:
    std::unique_ptr<GlobAST::GlobString> glob_string;
    std::unique_ptr<re2::RE2> re2_matcher;
};

}
