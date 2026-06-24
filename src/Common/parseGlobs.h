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
  *   wildcard     = "**" | "*" | "?" ;
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
  *   1. "**" is recognized before "*".
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
  * bodies of a legacy-escaped char such as "{-}", and wildcard runs such as "**"/"*?*").
  */
namespace GlobAST
{

/// fixme more clever range:
/// select start and end depending on which is higher or lower
/// calculate necessary padding to match values
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

    void parse();
    const std::vector<Expression> & getExpressions() const { return expressions; }

    std::string dump() const;
    size_t cardinality() const;

    /// Number of strings expand(expand_ranges) would produce: the product of the enum
    /// alternative counts (and range lengths when expand_ranges is set). Constants,
    /// wildcards, and unexpanded ranges contribute a factor of 1, because expand() renders
    /// them as literal text rather than enumerating them. Saturates at SIZE_MAX. Unlike
    /// cardinality(), a wildcard does not make the whole product SIZE_MAX. Use this to
    /// decide whether expand() stays within a budget without risking it throwing.
    size_t expansionSize(bool expand_ranges = false) const;

    /// Expand enum globs (and optionally range globs) into concrete path strings
    /// via cartesian product.
    /// Non-expanded expressions (constants, wildcards, unexpanded ranges) are rendered as literal text.
    /// E.g. "file{a,b}{1,2}.csv" → ["filea1.csv", "filea2.csv", "fileb1.csv", "fileb2.csv"]
    /// With expand_ranges=true: "file{1..3}.csv" → ["file1.csv", "file2.csv", "file3.csv"]
    /// Throws if the total expansion would exceed max_expansion.
    static constexpr size_t DEFAULT_MAX_EXPANSION = 1000;
    std::vector<std::string> expand(size_t max_expansion = DEFAULT_MAX_EXPANSION, bool expand_ranges = false) const;

    /// Match a candidate string against this glob pattern directly (no regex).
    /// Handles CONSTANT, WILDCARD (?, *, **), RANGE, and ENUM expressions.
    /// This is more efficient than converting to regex and using re2::RE2::FullMatch,
    /// especially for RANGE patterns where regex produces O(N) alternations
    /// but direct matching does O(1) numeric bounds checking.
    bool matches(std::string_view candidate) const;

    bool hasGlobs() const { return has_globs; }
    bool hasRanges() const { return has_ranges; }
    bool hasEnums() const { return has_enums; }
    bool hasQuestionOrAsterisk() const { return has_question_or_asterisk; }

    bool hasExactlyOneEnum() const;

private:
    std::string_view consumeConstantExpression(const std::string_view & input) const;
    std::string_view consumeMatcher(const std::string_view & input) const;

    std::vector<std::string_view> tryParseEnumMatcher(const std::string_view & input) const;
    std::optional<Range> tryParseRangeMatcher(const std::string_view & input) const;

    /// Recursive helper for matches(): tries to match candidate[pos..] against expressions[expr_idx..].
    /// memo is a flat array of size (candidate.size()+1) * (expressions.size()+1), tri-state:
    ///   0 = unknown, 1 = true, -1 = false.
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
