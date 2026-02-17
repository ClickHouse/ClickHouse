#pragma once

#include <optional>
#include <string>
#include <variant>
#include <vector>


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

namespace BetterGlob
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
    std::string asRegex() const;

    size_t cardinality() const;

private:
    std::string dumpRange() const;
    std::string dumpEnum(char separator = ',') const;
    std::string dumpWildcard() const;
    std::string rangeAsRegex() const;
    std::string enumAsRegex() const;
    std::string wildcardAsRegex() const;

    std::string escape(std::string_view input) const;

    ExpressionData data;
};

class GlobString
{
public:
    explicit GlobString(std::string input);

    void parse();
    const std::vector<Expression> & getExpressions() const { return expressions; }

    std::string dump() const;
    std::string asRegex() const;
    size_t cardinality() const;

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

    /// Returns true if the glob can be fully expanded into concrete paths
    /// (all expressions are CONSTANT, ENUM, or RANGE — no wildcards).
    bool isFullyExpandable() const { return (has_enums || has_ranges) && !has_question_or_asterisk; }

private:
    std::string_view consumeConstantExpression(const std::string_view & input) const;
    std::string_view consumeMatcher(const std::string_view & input) const;

    std::vector<std::string_view> tryParseEnumMatcher(const std::string_view & input) const;
    std::optional<Range> tryParseRangeMatcher(const std::string_view & input) const;

    /// Recursive helper for matches(): tries to match candidate[pos..] against expressions[expr_idx..].
    bool matchesImpl(std::string_view candidate, size_t pos, size_t expr_idx) const;

    std::vector<Expression> expressions;

    std::string input_data;

    bool has_globs = false;
    bool has_ranges = false;
    bool has_enums = false;
    bool has_question_or_asterisk = false;
};

}
}
