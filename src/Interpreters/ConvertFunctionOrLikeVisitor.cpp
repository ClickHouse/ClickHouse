#include <Interpreters/ConvertFunctionOrLikeVisitor.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/checkHyperscanRegexp.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Common/likePatternToRegexp.h>
#include <Common/isValidUTF8.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/typeid_cast.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include "config.h"

#include <limits>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
}

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

/// Returns true if `combined_regexp` compiles within RE2's limits, using the same engine and flags
/// as the `match` function (`RE_DOT_NL | RE_NO_CAPTURE`, see `Functions/Regexps.h`). The
/// combined-`match` fallback merges an `OR` chain into a single `(p1)|(p2)|...` alternation; for a
/// long or repetition-heavy chain the merged RE2 program can exceed RE2's default 8 MiB budget
/// (`RE2::Options::kDefaultMaxMem`) and throw `CANNOT_COMPILE_REGEXP`, even though every original
/// per-branch `match`/`LIKE` compiled on its own (each is a far smaller program). The
/// `max_hyperscan_regexp_length` / `max_hyperscan_regexp_total_length` settings default to 0
/// ("unlimited") and so do not bound this. We therefore pre-compile the merged regexp and keep the
/// original branches when it does not compile, so a default-on rewrite cannot turn a
/// previously-working query into a regexp-compilation exception. The probe constructs the same
/// `OptimizedRegularExpression` as `match`, so it accepts exactly the regexps `match` would accept at
/// runtime — there are no false negatives, and a failure to compile is always fail-close (we keep the
/// originals and skip the optimization, never emit an uncompilable `match`).
bool combinedRegexpCompilesWithRE2(const String & combined_regexp)
{
    try
    {
        OptimizedRegularExpression re(
            combined_regexp,
            OptimizedRegularExpression::RE_DOT_NL | OptimizedRegularExpression::RE_NO_CAPTURE);
        return true;
    }
    catch (...)
    {
        /// Ok: a compilation failure is the signal we are probing for. We intentionally swallow it
        /// and fail-close (keep the original branches, skip the optimization) rather than propagate.
        return false;
    }
}

/// Stores information about a single LIKE/ILIKE/match pattern
struct PatternData
{
    String substring;                   /// If pattern is %substring%, this holds the substring
    String regexp;                      /// The regexp equivalent
    bool is_substring = false;          /// True if pattern is a pure substring match (%substring%)
    bool is_case_insensitive = false;   /// True if case-insensitive (ILIKE or (?i) prefix)
    bool is_raw_regexp = false;         /// True if the regexp came verbatim from a `match()` call,
                                        /// rather than being generated from a LIKE/ILIKE pattern.
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

    /// Returns true if every regexp is valid UTF-8. `multiMatchAny` compiles its patterns with
    /// Hyperscan's `HS_FLAG_UTF8` and throws (`CANNOT_COMPILE_REGEXP` / `BAD_ARGUMENTS`) for a
    /// pattern that is not valid UTF-8, whereas the original `match` uses RE2, which accepts such
    /// patterns (matching them as Latin-1 — see `04311_text_index_non_utf8_needle_no_prune`). So
    /// when any pattern is not valid UTF-8 we must not emit `multiMatchAny`; the combined-`match`
    /// fallback (also RE2) preserves behavior. Only the regexp path is affected: pure-substring
    /// patterns go to the byte-oriented `multiSearchAny*` path, which has no such restriction.
    /// Used by `multiMatchAny` when ClickHouse is built with Vectorscan.
    [[maybe_unused]] bool allRegexpsValidUTF8() const
    {
        for (const auto & p : patterns)
            if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(p.regexp.data()), p.regexp.size()))
                return false;
        return true;
    }

    /// Returns true if any pattern is a raw regexp taken verbatim from a `match()` call. Such a
    /// regexp can use RE2 syntax that Vectorscan rejects under `HS_FLAG_UTF8` even when the bytes are
    /// valid UTF-8 — for example `\C` ("match any byte"), which RE2 accepts but Vectorscan rejects
    /// with `BAD_ARGUMENTS` ("\C is unsupported in UTF8"). `multiMatchAny` compiles through
    /// Vectorscan, so emitting it for a chain that contains a raw `match()` regexp could turn a
    /// previously-working query into an exception (`allRegexpsValidUTF8` does not catch this — the
    /// pattern bytes are valid UTF-8). We therefore keep such chains on the combined-`match` (RE2) /
    /// original path, which uses the same engine as the original `match()` and preserves behavior.
    /// Regexps generated from LIKE/ILIKE patterns are produced by `likePatternToRegexp` from a
    /// restricted grammar (literals, `.`, `.*`, escaped metacharacters, optional `(?i)`) that
    /// Vectorscan always accepts, so they stay eligible for `multiMatchAny` (subject only to the
    /// `allRegexpsValidUTF8` byte check).
    /// Used by `multiMatchAny` when ClickHouse is built with Vectorscan.
    [[maybe_unused]] bool hasRawRegexp() const
    {
        for (const auto & p : patterns)
            if (p.is_raw_regexp)
                return true;
        return false;
    }

    /// Returns true if no regexp contains an embedded NUL byte. Used to gate both regexp rewrite
    /// targets — `multiMatchAny` and the combined-`match` fallback — because neither reproduces the
    /// original predicate faithfully when a pattern contains an embedded NUL: `multiMatchAny` compiles
    /// through a NUL-terminated Vectorscan API and truncates at the first NUL (so a `LIKE`-derived
    /// regexp such as `^a\x00.` matches a broader set than the original `like`/`ilike`, which uses
    /// length-aware RE2), and the combined `(p1)|(p2)|...` alternation defeats RE2's required-substring
    /// truncation (so it matches a different set than the per-branch chain). When any pattern has an
    /// embedded NUL we keep the originals on both paths. The byte-oriented `multiSearchAny*` substring
    /// path is length-aware and preserving, so it needs no such guard.
    [[maybe_unused]] bool allRegexpsHaveNoEmbeddedNul() const
    {
        for (const auto & p : patterns)
            if (p.regexp.find('\0') != String::npos)
                return false;
        return true;
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
                bool keep_originals = false;  /// Force keeping originals (e.g. a pattern failed to convert)
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

                bool conversion_failed = false;
                if (is_match)
                {
                    /// match() already has a regexp pattern - use as is.
                    /// A regexp can never be a pure substring, so it always falls through to the
                    /// `multiMatchAny`/combined-`match` path; case-insensitivity flags inside the
                    /// pattern (e.g. `(?i)`, `(?mi:...)`) are preserved verbatim. Mark it as a raw
                    /// regexp so the rewrite keeps it on the RE2 (`match`) path instead of
                    /// `multiMatchAny`, which uses Vectorscan and rejects RE2-only syntax such as
                    /// `\C` — see `hasRawRegexp`.
                    data.regexp = pattern_str;
                    data.is_substring = false;
                    data.is_raw_regexp = true;
                }
                else
                {
                    /// Check if LIKE pattern is a simple substring search (never throws).
                    data.is_substring = likePatternIsSubstring(pattern_str, data.substring);

                    /// Convert the LIKE/ILIKE pattern to a regexp. `likePatternToRegexp` throws
                    /// `CANNOT_PARSE_ESCAPE_SEQUENCE` for a malformed pattern (e.g. a trailing
                    /// backslash). The optimizer must not surface that error eagerly: the original `OR`
                    /// chain may never evaluate this branch at runtime (short-circuit), or the group
                    /// may be below the rewrite threshold and stay unchanged. On a parse failure we
                    /// mark the whole group to keep its original branches, preserving the query's
                    /// runtime error / short-circuit behavior. Only the expected parse error is
                    /// swallowed; anything else propagates.
                    try
                    {
                        data.regexp = likePatternToRegexp(pattern_str);
                        if (is_ilike)
                            data.regexp = "(?i)" + data.regexp;
                    }
                    catch (const Exception & e)
                    {
                        if (e.code() != ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE)
                            throw;
                        conversion_failed = true;
                    }
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
                if (conversion_failed)
                    per_key_data[idx].keep_originals = true;
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
                /// Also keep originals when a pattern failed to convert to a regexp (see the collection
                /// loop), so the query's runtime error / short-circuit behavior is preserved instead of
                /// failing eagerly during optimization.
                if (info.patterns.size() < min_patterns_for_rewrite || key_data.keep_originals)
                {
                    slot = std::move(key_data.originals);
                    continue;
                }

                /// `multiSearchAny*` and `multiMatchAny` accept only a `String` haystack (after the
                /// usual `LowCardinality`/`Nullable` unwrapping); for `FixedString`/`Enum` haystacks —
                /// which the original `like`/`ilike`/`match` predicates accept — they throw
                /// `ILLEGAL_TYPE_OF_ARGUMENT` during function resolution. With the rewrite default-on
                /// this would turn a previously working chain over such a column into an exception. The
                /// LHS type is looked up by column name (unknown for non-column expressions); when it is
                /// not provably `String` we keep both fast paths off and fall back to the combined
                /// `match`, which (like the original predicates) accepts `FixedString`/`Enum`.
                bool haystack_is_string = false;
                if (auto type_it = source_column_types.find(key_data.identifier->getColumnNameWithoutAlias());
                    type_it != source_column_types.end() && type_it->second)
                    haystack_is_string = WhichDataType(removeLowCardinalityAndNullable(type_it->second)).isString();

                if (haystack_is_string && info.canUseMultiSearchAny())
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
                        && haystack_is_string
                        && fits_limits
                        && !info.hasRawRegexp()
                        && info.allRegexpsValidUTF8()
                        && info.allRegexpsHaveNoEmbeddedNul()
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
                        /// and that all patterns are valid UTF-8, so a query that previously worked as
                        /// `OR LIKE` cannot be turned into a `BAD_ARGUMENTS` / `HYPERSCAN_CANNOT_SCAN_TEXT`
                        /// failure. `hasRawRegexp` additionally keeps chains that contain a raw `match()`
                        /// regexp off this path: Vectorscan rejects RE2-only syntax such as `\C` even when
                        /// the bytes are valid UTF-8 (see `hasRawRegexp`). `allRegexpsHaveNoEmbeddedNul`
                        /// keeps chains whose regexps contain an embedded NUL off this path:
                        /// `multiMatchAny` compiles each pattern through a NUL-terminated Vectorscan API
                        /// and truncates at the first NUL, whereas the original `like`/`ilike` compiles
                        /// the full pattern with RE2 (length-aware), so a `LIKE`-derived regexp such as
                        /// `^a\x00.` (from `LIKE 'a\0_%'`) matches a different set; such chains fall back
                        /// to the combined-`match` / originals below.
                        match_fn = makeASTFunction("multiMatchAny", key_data.identifier, make_intrusive<ASTLiteral>(Field{info.getRegexps()}));
                    }
                    else if (info.allRegexpsHaveNoEmbeddedNul()
                        && info.combinedRegexpFitsHyperscanLimits(max_hyperscan_regexp_length, max_hyperscan_regexp_total_length)
                        && combinedRegexpCompilesWithRE2(info.getCombinedRegexp()))
                    {
                        /// Fall back to `match` with combined alternation when Vectorscan is not
                        /// compiled in, `allow_hyperscan` is off, the patterns would be rejected
                        /// as expensive, the chain contains a raw `match()` regexp, or some pattern is
                        /// not valid UTF-8 (which `multiMatchAny` cannot compile but RE2 accepts).
                        /// `combinedRegexpFitsHyperscanLimits` accounts for the `(`, `)` and `|`
                        /// overhead added by `getCombinedRegexp`, so `max_hyperscan_regexp_total_length`
                        /// is a strict upper bound on the emitted regexp when the user sets it; but those
                        /// limits default to 0 ("unlimited"), so they alone do not stop a large or
                        /// repetition-heavy chain from being merged into a single RE2 program that exceeds
                        /// RE2's 8 MiB budget and throws `CANNOT_COMPILE_REGEXP`. We therefore additionally
                        /// pre-compile the merged regexp (`combinedRegexpCompilesWithRE2`) and only emit
                        /// the combined `match` when it compiles; otherwise we fall through to the `else`
                        /// below and keep the originals.
                        /// `allRegexpsHaveNoEmbeddedNul` excludes patterns with an embedded NUL: RE2
                        /// truncates a lone pattern at the first NUL but not the `(p1)|(p2)|...`
                        /// alternation, so the combined `match` would match a different (narrower) set
                        /// than the original chain. Such groups fall through to the `else` below and
                        /// keep the originals, so results are preserved regardless of `allow_hyperscan`.
                        match_fn = makeASTFunction("match", key_data.identifier, make_intrusive<ASTLiteral>(Field{info.getCombinedRegexp()}));
                    }
                    else
                    {
                        /// Patterns exceed the configured hyperscan size limits, or contain an embedded
                        /// NUL that the combined alternation cannot reproduce faithfully. Skip the
                        /// rewrite for this key — keep the original `OR LIKE` branches so the query
                        /// remains executable, we don't build an unbounded combined regexp, and the
                        /// result is preserved.
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
