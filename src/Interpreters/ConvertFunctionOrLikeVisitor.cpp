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

#include <limits>
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
    String substring;                   /// If pattern is %substring%, this holds the substring
    String regexp;                      /// The regexp equivalent
    bool is_substring = false;          /// True if pattern is a pure substring match (%substring%)
    bool is_case_insensitive = false;   /// True if case-insensitive (ILIKE or (?i) prefix)
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
        /// 3. The number of needles fits the `multiSearchAny` runtime limit. `MultiSearchImpl`
        ///    throws `TOO_MANY_ARGUMENTS_FOR_FUNCTION` for constant needle arrays larger than
        ///    `UInt8::max` (255). Without this guard a default-on rewrite of a long substring
        ///    `OR LIKE` chain would turn a previously-working query into an exception; instead we
        ///    fall through to the `multiMatchAny`/combined-`match` path, which has no such cap.
        return all_substrings
            && !(has_case_sensitive && has_case_insensitive)
            && patterns.size() <= std::numeric_limits<UInt8>::max();
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

    /// Predicted size of `getCombinedRegexp()` without actually building the string:
    /// `(p1)|(p2)|...` — 2 wrapper chars per pattern, plus `N - 1` separator chars.
    size_t combinedRegexpSize() const
    {
        if (patterns.empty())
            return 0;
        size_t total = 0;
        for (const auto & p : patterns)
            total += p.regexp.size();
        return total + 2 * patterns.size() + (patterns.size() - 1);
    }

    /// Returns true if all per-pattern lengths and the total length fit within the hyperscan
    /// regexp size limits. A limit value of 0 means "unlimited".
    /// Used as a pre-check for `multiMatchAny`; for the combined-`match` fallback, use
    /// `combinedRegexpFitsHyperscanLimits` instead so the `(`/`)`/`|` overhead is included.
    bool fitsHyperscanLimits(size_t max_length, size_t max_total_length) const
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

    /// Like `fitsHyperscanLimits`, but bounds the size of the alternation regexp that
    /// `getCombinedRegexp` would build. The alternation adds `(`, `)` and `|` overhead per
    /// branch, so the emitted `match` regexp can be substantially larger than the sum of raw
    /// pattern sizes. Pre-checking the combined size lets the rewrite back off to the original
    /// `OR LIKE` chain instead of emitting a `match` regexp that could blow up RE2 compile
    /// limits.
    bool combinedRegexpFitsHyperscanLimits(size_t max_length, size_t max_total_length) const
    {
        if (max_length == 0 && max_total_length == 0)
            return true;

        for (const auto & p : patterns)
            if (max_length > 0 && p.regexp.size() > max_length)
                return false;
        return max_total_length == 0 || combinedRegexpSize() <= max_total_length;
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
            /// Each "slot" in the OR's argument list. A slot is either a single non-LIKE branch we
            /// kept as-is, or a placeholder for a group of LIKE/ILIKE/match patterns sharing the
            /// same LHS expression. We resolve each placeholder to either a single optimized match
            /// function or (when the rewrite is unsafe) the original branches kept verbatim, then
            /// flatten back into the OR's argument list.
            std::vector<ASTs> slots;

            struct PerKeyData
            {
                ASTPtr identifier;
                PatternInfo info;
                ASTs originals;
                size_t slot_index = 0;
            };
            std::vector<PerKeyData> per_key_data;
            /// Key: (alias-or-column-name, alias-free-column-name). Using a pair (rather than a
            /// joined string) avoids any ambiguity from separator characters appearing inside
            /// identifier names.
            std::map<std::pair<String, String>, size_t> key_to_index;

            for (const auto & child_expr_fn : expr_list_fn->children)
            {
                const auto * child_fn = child_expr_fn->as<ASTFunction>();
                if (!child_fn)
                {
                    slots.push_back({child_expr_fn});
                    continue;
                }

                const bool is_like = child_fn->name == "like";
                const bool is_ilike = child_fn->name == "ilike";
                const bool is_match = child_fn->name == "match";

                /// Not {i}like or match -> keep as-is.
                if (!is_like && !is_ilike && !is_match)
                {
                    slots.push_back({child_expr_fn});
                    continue;
                }

                const auto & arguments = child_fn->arguments->children;
                if (arguments.size() != 2)
                {
                    slots.push_back({child_expr_fn});
                    continue;
                }

                auto identifier = arguments[0];
                auto * literal = arguments[1]->as<ASTLiteral>();
                if (!identifier || !literal || literal->value.getType() != Field::Types::String)
                {
                    slots.push_back({child_expr_fn});
                    continue;
                }

                /// Don't merge `f(x) LIKE 'a%' OR f(x) LIKE 'b%'` when `f` is non-deterministic
                /// (e.g. `rand`). Both branches would render to the same column-name key, but at
                /// runtime they evaluate independently — collapsing them into one
                /// `multiSearchAny`/`multiMatchAny` call would change query results.
                if (isExpressionNonDeterministic(identifier, context))
                {
                    slots.push_back({child_expr_fn});
                    continue;
                }

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

                /// Compound key: alias-or-column-name + alias-free column name. Both parts must match
                /// for two branches to be merged. This rejects two structurally distinct expressions
                /// sharing the same user-provided alias (e.g. `(toString(1) AS z) LIKE 'a' OR
                /// (toString(2) AS z) LIKE 'b'` — same alias `z`, different underlying expressions),
                /// while still treating two references to the same alias (e.g. `s1 LIKE 'a' OR s1
                /// LIKE 'b'` after old-analyzer alias substitution) as one group. Plain identifiers
                /// without explicit aliases produce identical first/second parts and merge normally.
                std::pair<String, String> key{identifier->getAliasOrColumnName(), identifier->getColumnNameWithoutAlias()};
                auto it = key_to_index.find(key);
                size_t idx = 0;
                if (it == key_to_index.end())
                {
                    idx = per_key_data.size();
                    key_to_index.emplace(key, idx);
                    per_key_data.emplace_back();
                    per_key_data.back().identifier = identifier;
                    per_key_data.back().slot_index = slots.size();
                    slots.emplace_back();  /// reserved placeholder slot
                }
                else
                {
                    idx = it->second;
                }

                per_key_data[idx].originals.push_back(child_expr_fn);
                per_key_data[idx].info.patterns.push_back(std::move(data));
            }

            /// If no patterns were collected, nothing to do
            if (per_key_data.empty())
                continue;

            bool any_rewrite = false;

            /// Decide per-key whether to rewrite. When the patterns exceed the configured hyperscan
            /// size limits, keep the original LIKE/ILIKE/match branches: emitting `multiMatchAny`
            /// would throw at runtime and emitting one combined `match` regexp could blow up RE2
            /// compile limits.
            for (auto & key_data : per_key_data)
            {
                auto & info = key_data.info;
                ASTs & slot = slots[key_data.slot_index];

                ASTPtr match_fn;

                /// Skip rewriting groups with too few patterns — the rewrite cost (fixed setup of
                /// `multiSearchAny`/Hyperscan) typically exceeds the benefit of short-circuit OR
                /// evaluation for short chains. Controlled by `optimize_or_like_chain_min_patterns`.
                if (info.patterns.size() < min_patterns_for_rewrite)
                {
                    slot = std::move(key_data.originals);
                    continue;
                }

                if (info.canUseMultiSearchAny())
                {
                    /// Use `multiSearchAny` or `multiSearchAnyCaseInsensitiveUTF8` for pure substring patterns.
                    /// `multiSearchAny*` operates on raw substrings, not regexps, so the hyperscan
                    /// regexp size limits are not applicable here.
                    String func_name = info.needsCaseInsensitive() ? "multiSearchAnyCaseInsensitiveUTF8" : "multiSearchAny";
                    match_fn = makeASTFunction(func_name, key_data.identifier, make_intrusive<ASTLiteral>(Field{info.getSubstrings()}));
                }
                else
                {
                    [[maybe_unused]] const bool fits_limits
                        = info.fitsHyperscanLimits(max_hyperscan_regexp_length, max_hyperscan_regexp_total_length);
#if USE_VECTORSCAN
                    const bool can_use_multi_match = allow_hyperscan
                        && fits_limits
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
                        match_fn = makeASTFunction("multiMatchAny", key_data.identifier, make_intrusive<ASTLiteral>(Field{info.getRegexps()}));
                    }
                    else if (info.combinedRegexpFitsHyperscanLimits(max_hyperscan_regexp_length, max_hyperscan_regexp_total_length))
                    {
                        /// Fall back to `match` with combined alternation when Vectorscan is not
                        /// compiled in, `allow_hyperscan` is off, or the patterns would be rejected
                        /// as expensive. `combinedRegexpFitsHyperscanLimits` accounts for the `(`,
                        /// `)` and `|` overhead added by `getCombinedRegexp`, so
                        /// `max_hyperscan_regexp_total_length` is a strict upper bound on the
                        /// emitted regexp and we cannot blow up RE2 compile limits here.
                        match_fn = makeASTFunction("match", key_data.identifier, make_intrusive<ASTLiteral>(Field{info.getCombinedRegexp()}));
                    }
                    else
                    {
                        /// Patterns exceed the configured hyperscan size limits. Skip the rewrite
                        /// for this key — keep the original `OR LIKE` branches so the query remains
                        /// executable and so we don't build an unbounded combined regexp.
                        slot = std::move(key_data.originals);
                        continue;
                    }
                }

                slot.push_back(std::move(match_fn));
                any_rewrite = true;
            }

            if (!any_rewrite)
                continue;

            /// Flatten the slot list back into the OR's argument list.
            ASTs flattened;
            for (auto & slot : slots)
                for (auto & elem : slot)
                    flattened.push_back(std::move(elem));

            /// OR must have at least two arguments.
            if (flattened.size() == 1)
                flattened.push_back(make_intrusive<ASTLiteral>(Field(false)));

            expr_list_fn->children = std::move(flattened);

            /// Note: indexHint wrapping is only supported in the new analyzer.
            /// The old analyzer doesn't support it due to visitor pattern limitations.
        }
    }
}

}
