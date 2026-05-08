#include <Interpreters/ConvertFunctionOrLikeVisitor.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/checkHyperscanRegexp.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Common/likePatternToRegexp.h>
#include <Common/typeid_cast.h>

#include "config.h"

#include <vector>


namespace DB
{

namespace
{

/// Returns true if any function in the AST subtree is non-deterministic within a query
/// (e.g. `rand`, `generateUUIDv4`). Used to avoid grouping LIKE patterns whose left-hand
/// side renders to the same text but evaluates to different values across occurrences.
bool isExpressionNonDeterministic(const ASTPtr & ast, const ContextPtr & context)
{
    if (!ast || !context)
        return false;

    if (const auto * function = ast->as<ASTFunction>())
    {
        if (auto resolver = FunctionFactory::instance().tryGet(function->name, context))
            if (!resolver->isDeterministicInScopeOfQuery())
                return true;
    }

    for (const auto & child : ast->children)
        if (isExpressionNonDeterministic(child, context))
            return true;

    return false;
}

/// Stores information about a single LIKE/ILIKE/match pattern
struct PatternData
{
    String substring;           /// If pattern is %substring%, this holds the substring
    String regexp;              /// The regexp equivalent
    bool is_substring;          /// True if pattern is a pure substring match (%substring%)
    bool is_case_insensitive;   /// True if case-insensitive (ILIKE or (?i) prefix)
};

/// Tracks information about patterns for a single identifier/expression
struct PatternInfo
{
    std::vector<PatternData> patterns;

    bool canUseMultiSearchAny() const
    {
        if (patterns.empty())
            return false;

        /// Check if all patterns are pure substring matches with same case sensitivity
        bool all_substrings = true;
        bool has_case_sensitive = false;
        bool has_case_insensitive = false;

        for (const auto & p : patterns)
        {
            if (!p.is_substring)
                all_substrings = false;
            if (p.is_case_insensitive)
                has_case_insensitive = true;
            else
                has_case_sensitive = true;
        }

        /// Can use multiSearchAny only if:
        /// 1. All patterns are pure substring matches
        /// 2. All patterns have the same case sensitivity (not mixed)
        return all_substrings && !(has_case_sensitive && has_case_insensitive);
    }

    bool needsCaseInsensitive() const
    {
        for (const auto & p : patterns)
            if (p.is_case_insensitive)
                return true;
        return false;
    }

    Array getSubstrings() const
    {
        Array result;
        for (const auto & p : patterns)
            result.push_back(p.substring);
        return result;
    }

    /// Used by `multiMatchAny` when ClickHouse is built with Vectorscan.
    [[maybe_unused]] Array getRegexps() const
    {
        Array result;
        for (const auto & p : patterns)
            result.push_back(p.regexp);
        return result;
    }

    /// Build a combined regexp pattern using alternation: (pattern1)|(pattern2)|...
    /// Used as the `match` fallback when the binary is built without Vectorscan.
    String getCombinedRegexp() const
    {
        String result;
        for (const auto & p : patterns)
        {
            if (!result.empty())
                result += "|";
            result += "(" + p.regexp + ")";
        }
        return result;
    }

    /// Returns true if all per-pattern lengths and the total length fit within the hyperscan
    /// regexp size limits. A limit value of 0 means "unlimited".
    /// Used by `multiMatchAny` when ClickHouse is built with Vectorscan.
    [[maybe_unused]] bool fitsHyperscanLimits(size_t max_length, size_t max_total_length) const
    {
        if (max_length == 0 && max_total_length == 0)
            return true;

        size_t total = 0;
        for (const auto & p : patterns)
        {
            if (max_length > 0 && p.regexp.size() > max_length)
                return false;
            total += p.regexp.size();
        }
        return max_total_length == 0 || total <= max_total_length;
    }

    /// Returns true if any pattern would be rejected at runtime by the `multiMatchAny` slow-regexp
    /// guard (`SlowWithHyperscanChecker`). Used to pre-check `reject_expensive_hyperscan_regexps`
    /// so the rewrite cannot turn a previously-working query into a `HYPERSCAN_CANNOT_SCAN_TEXT`
    /// failure: if any pattern is "slow", we fall back to plain `match` instead of `multiMatchAny`.
    /// Used by `multiMatchAny` when ClickHouse is built with Vectorscan.
    [[maybe_unused]] bool hasExpensiveRegexp() const
    {
        SlowWithHyperscanChecker checker;
        for (const auto & p : patterns)
            if (checker.isSlow(p.regexp))
                return true;
        return false;
    }
};

}

void ConvertFunctionOrLikeData::visit(ASTFunction & function, ASTPtr & /*ast*/) const
{
    if (function.name != "or")
        return;

    for (auto & child : function.children)
    {
        if (auto * expr_list_fn = child->as<ASTExpressionList>())
        {
            ASTs unique_elems;
            std::unordered_map<String, PatternInfo> identifier_to_patterns;
            std::vector<String> pattern_keys;  /// To preserve order
            std::vector<size_t> match_function_positions;  /// Positions in unique_elems
            std::unordered_map<String, ASTPtr> identifier_to_ast;  /// Map key to AST node

            for (const auto & child_expr_fn : expr_list_fn->children)
            {
                unique_elems.push_back(child_expr_fn);
                if (const auto * child_fn = child_expr_fn->as<ASTFunction>())
                {
                    const bool is_like = child_fn->name == "like";
                    const bool is_ilike = child_fn->name == "ilike";
                    const bool is_match = child_fn->name == "match";

                    /// Not {i}like or match -> bail out.
                    if (!is_like && !is_ilike && !is_match)
                        continue;

                    const auto & arguments = child_fn->arguments->children;

                    /// They should have 2 arguments.
                    if (arguments.size() != 2)
                        continue;

                    /// Second one is string literal.
                    auto identifier = arguments[0];
                    auto * literal = arguments[1]->as<ASTLiteral>();
                    if (!identifier || !literal || literal->value.getType() != Field::Types::String)
                        continue;

                    /// Don't merge `f(x) LIKE 'a%' OR f(x) LIKE 'b%'` when `f` is non-deterministic
                    /// (e.g. `rand`). Both branches would render to the same `getAliasOrColumnName`
                    /// key, but at runtime they evaluate independently — collapsing them into one
                    /// `multiSearchAny`/`multiMatchAny` call would change query results.
                    if (isExpressionNonDeterministic(identifier, context))
                        continue;

                    const String & pattern_str = literal->value.safeGet<String>();

                    PatternData data;
                    data.is_case_insensitive = is_ilike;
                    data.is_substring = false;

                    if (is_match)
                    {
                        /// match() already has a regexp pattern - use as is.
                        /// A regexp can never be a pure substring, so it always falls through to the
                        /// combined `match` path; case-insensitivity flags inside the pattern (e.g.
                        /// `(?i)`, `(?mi:...)`) are preserved verbatim in the combined alternation.
                        data.regexp = pattern_str;
                        data.is_substring = false;
                    }
                    else
                    {
                        /// Check if LIKE pattern is a simple substring search
                        data.is_substring = likePatternIsSubstring(pattern_str, data.substring);

                        /// Always compute regexp for fallback
                        data.regexp = likePatternToRegexp(pattern_str);
                        if (is_ilike)
                            data.regexp = "(?i)" + data.regexp;
                    }

                    unique_elems.pop_back();
                    String key = identifier->getAliasOrColumnName();
                    auto it = identifier_to_patterns.find(key);

                    if (it == identifier_to_patterns.end())
                    {
                        it = identifier_to_patterns.insert({key, PatternInfo{}}).first;
                        identifier_to_ast[key] = identifier;
                        pattern_keys.push_back(key);
                        match_function_positions.push_back(unique_elems.size());
                        unique_elems.push_back(nullptr);  /// Placeholder
                    }

                    it->second.patterns.push_back(std::move(data));
                }
            }

            /// If no patterns were optimized, nothing to do
            if (identifier_to_patterns.empty())
                continue;

            /// Create appropriate function nodes and fill in placeholders
            for (size_t i = 0; i < pattern_keys.size(); ++i)
            {
                const String & key = pattern_keys[i];
                auto & info = identifier_to_patterns.at(key);
                size_t pos = match_function_positions[i];
                const ASTPtr & identifier_ast = identifier_to_ast.at(key);

                ASTPtr match_fn;
                if (info.canUseMultiSearchAny())
                {
                    /// Use `multiSearchAny` or `multiSearchAnyCaseInsensitiveUTF8` for pure substring patterns.
                    String func_name = info.needsCaseInsensitive() ? "multiSearchAnyCaseInsensitiveUTF8" : "multiSearchAny";
                    match_fn = makeASTFunction(func_name, identifier_ast, make_intrusive<ASTLiteral>(Field{info.getSubstrings()}));
                }
                else
                {
#if USE_VECTORSCAN
                    const bool can_use_multi_match = allow_hyperscan
                        && info.fitsHyperscanLimits(max_hyperscan_regexp_length, max_hyperscan_regexp_total_length)
                        && !(reject_expensive_hyperscan_regexps && info.hasExpensiveRegexp());
#else
                    constexpr bool can_use_multi_match = false;
#endif
                    if (can_use_multi_match)
                    {
                        /// Use `multiMatchAny` for non-substring patterns. It is significantly faster
                        /// than `match` with alternation in RE2 because it can leverage
                        /// Vectorscan/Hyperscan. We pre-check `max_hyperscan_regexp_length`,
                        /// `max_hyperscan_regexp_total_length` and `reject_expensive_hyperscan_regexps`,
                        /// so a query that previously worked as `OR LIKE` cannot be turned into a
                        /// `BAD_ARGUMENTS` / `HYPERSCAN_CANNOT_SCAN_TEXT` failure by this rewrite.
                        match_fn = makeASTFunction("multiMatchAny", identifier_ast, make_intrusive<ASTLiteral>(Field{info.getRegexps()}));
                    }
                    else
                    {
                        /// Fall back to `match` with combined alternation when Vectorscan is not
                        /// compiled in, `allow_hyperscan` is off, or the hyperscan size limits
                        /// would be exceeded.
                        match_fn = makeASTFunction("match", identifier_ast, make_intrusive<ASTLiteral>(Field{info.getCombinedRegexp()}));
                    }
                }

                unique_elems[pos] = std::move(match_fn);
            }

            /// OR must have at least two arguments.
            if (unique_elems.size() == 1)
                unique_elems.push_back(make_intrusive<ASTLiteral>(Field(false)));

            expr_list_fn->children = std::move(unique_elems);

            /// Note: indexHint wrapping is only supported in the new analyzer.
            /// The old analyzer doesn't support it due to visitor pattern limitations.
        }
    }
}

}
