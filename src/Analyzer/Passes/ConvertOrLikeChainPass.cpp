#include <Analyzer/Passes/ConvertOrLikeChainPass.h>

#include <limits>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config.h"

#include <Common/likePatternToRegexp.h>
#include <Common/isValidUTF8.h>

#include <Core/Field.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Functions/FunctionFactory.h>
#include <Functions/checkHyperscanRegexp.h>
#include <Functions/logical.h>

#include <Interpreters/Context.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Core/Settings.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_hyperscan;
    extern const SettingsUInt64 max_hyperscan_regexp_length;
    extern const SettingsUInt64 max_hyperscan_regexp_total_length;
    extern const SettingsBool optimize_or_like_chain;
    extern const SettingsUInt64 optimize_or_like_chain_min_patterns;
    extern const SettingsUInt64 optimize_or_like_chain_min_substrings;
    extern const SettingsBool reject_expensive_hyperscan_regexps;
}

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
}

namespace
{

/// Returns true if the QueryTree subtree contains any function call that is non-deterministic
/// within a single query (e.g. `rand`, `generateUUIDv4`). Used to avoid grouping LIKE patterns
/// whose left-hand side is structurally identical but evaluates to different values across
/// occurrences — collapsing them into one `multiSearchAny`/`multiMatchAny` call would change
/// query results.
///
/// Recursion follows all children, not just `FunctionNode` arguments: a non-deterministic call
/// can be nested inside a `LambdaNode` body (e.g. `arrayMap(x -> rand() + x, col)`), a subquery,
/// or any other intermediate node, and `FunctionNode`-only descent would miss those cases.
bool isExpressionNonDeterministic(const QueryTreeNodePtr & node)
{
    if (!node)
        return false;

    if (auto * function = node->as<FunctionNode>())
        if (function->isResolved())
            if (auto func = function->getFunctionOrThrow(); !func->isDeterministicInScopeOfQuery())
                return true;

    for (const auto & child : node->getChildren())
        if (isExpressionNonDeterministic(child))
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

    Array getRegexps() const
    {
        Array result;
        for (const auto & p : patterns)
            result.push_back(p.regexp);
        return result;
    }

    /// Returns true if all per-pattern lengths and the total length fit within the hyperscan
    /// regexp size limits. A limit value of 0 means "unlimited".
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

    /// Returns true if any pattern would be rejected at runtime by the `multiMatchAny` slow-regexp
    /// guard (`SlowWithHyperscanChecker`). Used to pre-check `reject_expensive_hyperscan_regexps`
    /// so the rewrite cannot turn a previously-working query into a `HYPERSCAN_CANNOT_SCAN_TEXT`
    /// failure: if any pattern is "slow", we fall back to plain `match` instead of `multiMatchAny`.
    bool hasExpensiveRegexp() const
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
    bool allRegexpsValidUTF8() const
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
    bool hasRawRegexp() const
    {
        for (const auto & p : patterns)
            if (p.is_raw_regexp)
                return true;
        return false;
    }

    /// Returns true if no regexp contains an embedded NUL byte. Used to gate the `multiMatchAny`
    /// rewrite off chains that contain an embedded NUL: `multiMatchAny` compiles each pattern through
    /// a NUL-terminated Vectorscan API and truncates it at the first NUL, whereas a `LIKE`/`ILIKE`-derived
    /// regexp (e.g. `^a\x00.` from `LIKE 'a\0_%'`) is matched by the original `like`/`ilike` with RE2 over
    /// the full length, so the truncated `multiMatchAny` pattern matches a *broader* set than the original.
    /// When any pattern has an embedded NUL we therefore keep the originals. The byte-oriented
    /// `multiSearchAny*` substring path is length-aware and preserving, so it needs no such guard.
    bool allRegexpsHaveNoEmbeddedNul() const
    {
        for (const auto & p : patterns)
            if (p.regexp.find('\0') != String::npos)
                return false;
        return true;
    }
};

class ConvertOrLikeChainVisitor : public InDepthQueryTreeVisitorWithContext<ConvertOrLikeChainVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertOrLikeChainVisitor>;
    using Base::Base;

    explicit ConvertOrLikeChainVisitor(FunctionOverloadResolverPtr or_function_resolver_,
        FunctionOverloadResolverPtr and_function_resolver_,
        ContextPtr context)
        : Base(std::move(context))
        , or_function_resolver(std::move(or_function_resolver_))
        , and_function_resolver(std::move(and_function_resolver_))
    {}

    bool needChildVisit(VisitQueryTreeNodeType & parent, VisitQueryTreeNodeType &)
    {
        const auto & settings = getSettings();

        if (!settings[Setting::optimize_or_like_chain])
            return false;

        /// Don't descend into indexHint - we put original expressions there specifically for index analysis,
        /// and we don't want them to be transformed
        if (auto * function = parent->as<FunctionNode>())
            if (function->getFunctionName() == "indexHint")
                return false;

        return true;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "or")
            return;

        /// Prevent infinite recursion - don't process the same OR node twice
        /// (this can happen when we wrap the transformed OR in an AND node)
        if (processed_nodes.contains(function_node))
            return;
        processed_nodes.insert(function_node);

        /// Each "slot" in the OR's argument list. A slot is either a single non-LIKE branch we kept
        /// as-is, or a placeholder for a group of LIKE/ILIKE/match patterns sharing the same LHS
        /// expression. We resolve the placeholder to either a single optimized match function or
        /// (when the rewrite is unsafe) the original branches kept verbatim, then flatten back into
        /// the OR's argument list.
        std::vector<QueryTreeNodes> slots;

        struct PerKeyData
        {
            QueryTreeNodePtr key;
            PatternInfo info;
            QueryTreeNodes originals;  /// Original LIKE/ILIKE/match argument nodes for this key
            size_t slot_index = 0;     /// Index into `slots` reserved for this key
            bool keep_originals = false;  /// Force keeping originals (e.g. a pattern failed to convert)
        };
        std::vector<PerKeyData> per_key_data;
        QueryTreeNodePtrWithHashMap<size_t> key_to_index;

        for (auto & argument : function_node->getArguments())
        {
            auto * argument_function = argument->as<FunctionNode>();
            if (!argument_function)
            {
                slots.push_back({argument});
                continue;
            }

            const bool is_like  = argument_function->getFunctionName() == "like";
            const bool is_ilike = argument_function->getFunctionName() == "ilike";
            const bool is_match = argument_function->getFunctionName() == "match";

            /// Not {i}like or match -> keep as-is.
            if (!is_like && !is_ilike && !is_match)
            {
                slots.push_back({argument});
                continue;
            }

            const auto & like_arguments = argument_function->getArguments().getNodes();
            if (like_arguments.size() != 2)
            {
                slots.push_back({argument});
                continue;
            }

            const auto & like_first_argument = like_arguments[0];
            const auto * pattern = like_arguments[1]->as<ConstantNode>();
            if (!pattern || !isString(pattern->getResultType()))
            {
                slots.push_back({argument});
                continue;
            }

            /// Don't merge `f(x) LIKE 'a%' OR f(x) LIKE 'b%'` when `f` is non-deterministic
            /// (e.g. `rand`). The structural hash treats both branches as equal, but at runtime
            /// they evaluate independently — collapsing would change query results.
            if (isExpressionNonDeterministic(like_first_argument))
            {
                slots.push_back({argument});
                continue;
            }

            const String & pattern_str = pattern->getValue().safeGet<String>();

            PatternData data;
            data.is_case_insensitive = is_ilike;
            data.is_substring = false;

            bool conversion_failed = false;
            if (is_match)
            {
                /// match() already has a regexp pattern - use as is.
                /// A regexp can never be a pure substring, so it always falls through to the
                /// `multiMatchAny`/combined-`match` path; case-insensitivity flags inside the pattern
                /// (e.g. `(?i)`, `(?mi:...)`) are preserved verbatim. Mark it as a raw regexp so the
                /// rewrite keeps it on the RE2 (`match`) path instead of `multiMatchAny`, which uses
                /// Vectorscan and rejects RE2-only syntax such as `\C` — see `hasRawRegexp`.
                data.regexp = pattern_str;
                data.is_substring = false;
                data.is_raw_regexp = true;
            }
            else
            {
                /// Check if LIKE pattern is a simple substring search (never throws).
                data.is_substring = likePatternIsSubstring(pattern_str, data.substring);

                /// Convert the LIKE/ILIKE pattern to a regexp. `likePatternToRegexp` throws
                /// `CANNOT_PARSE_ESCAPE_SEQUENCE` for a malformed pattern (e.g. a trailing backslash).
                /// The optimizer must not surface that error eagerly: the original `OR` chain may never
                /// evaluate this branch at runtime (short-circuit), or the group may be below the
                /// rewrite threshold and stay unchanged. On a parse failure we mark the whole group to
                /// keep its original branches, preserving the query's runtime error / short-circuit
                /// behavior. Only the expected parse error is swallowed; anything else propagates.
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

            auto it = key_to_index.find(like_first_argument);
            size_t idx = 0;
            if (it == key_to_index.end())
            {
                idx = per_key_data.size();
                key_to_index.emplace(like_first_argument, idx);
                per_key_data.emplace_back();
                per_key_data.back().key = like_first_argument;
                per_key_data.back().slot_index = slots.size();
                slots.emplace_back();  /// reserved placeholder slot
            }
            else
            {
                idx = it->second;
            }

            per_key_data[idx].originals.push_back(argument);
            per_key_data[idx].info.patterns.push_back(std::move(data));
            if (conversion_failed)
                per_key_data[idx].keep_originals = true;
        }

        /// If no LIKE/ILIKE/match patterns were collected, nothing to do
        if (per_key_data.empty())
            return;

        /// Cache context for later use
        auto context = getContext();
        /// `multiMatchAny` requires Vectorscan compiled in. When ClickHouse is built without it, the
        /// function is registered but throws `NOT_IMPLEMENTED` at execution time, so we must not
        /// generate it here regardless of the `allow_hyperscan` setting.
#if USE_VECTORSCAN
        const bool allow_hyperscan = context->getSettingsRef()[Setting::allow_hyperscan];
#else
        const bool allow_hyperscan = false;
#endif
        const size_t max_hyperscan_regexp_length = context->getSettingsRef()[Setting::max_hyperscan_regexp_length];
        const size_t max_hyperscan_regexp_total_length = context->getSettingsRef()[Setting::max_hyperscan_regexp_total_length];
        const bool reject_expensive_hyperscan_regexps = context->getSettingsRef()[Setting::reject_expensive_hyperscan_regexps];
        /// Per-target thresholds. Calibrated on `hits`: the `multiSearchAny` (substring) rewrite is
        /// beneficial from ~4 branches, while the `multiMatchAny` (regexp) rewrite only pays off from
        /// ~9 branches — its fixed Vectorscan setup is larger. See the settings' documentation.
        const size_t min_substrings_for_rewrite = context->getSettingsRef()[Setting::optimize_or_like_chain_min_substrings];
        const size_t min_patterns_for_rewrite = context->getSettingsRef()[Setting::optimize_or_like_chain_min_patterns];

        /// `indexHint(X) AND expr` restricts index analysis to granules that may satisfy `X`. To stay
        /// correct, `X` must be a *superset* of the rows the outer OR can match — otherwise we'd
        /// prune granules whose only matching rows come from branches missing in `X`. We therefore
        /// wrap with `indexHint(<original full OR chain>)` only when every OR branch is a
        /// LIKE/ILIKE/match (no `URL = 'foo'`-style branches that the index might match independently
        /// of the LIKE chain), and include originals for *all* per-key groups — both rewritten and
        /// the ones we keep as-is (below the threshold or ineligible for a fast path).
        const bool is_pure_like_chain = per_key_data.size() == slots.size();

        bool any_rewrite = false;
        QueryTreeNodes index_hint_originals;

        for (auto & key_data : per_key_data)
        {
            auto & info = key_data.info;
            QueryTreeNodes & slot = slots[key_data.slot_index];

            std::shared_ptr<FunctionNode> match_function;

            /// `multiSearchAny*` and `multiMatchAny` accept only a `String` haystack (after the usual
            /// `LowCardinality`/`Nullable` unwrapping); for `FixedString`/`Enum` haystacks — which the
            /// original `like`/`ilike`/`match` predicates accept — they throw `ILLEGAL_TYPE_OF_ARGUMENT`
            /// during function resolution. With the rewrite default-on this would turn a previously
            /// working chain over such a column into an exception, so a non-`String` haystack keeps its
            /// original branches.
            const auto haystack_type = key_data.key->getResultType();
            const bool haystack_is_string
                = haystack_type && WhichDataType(removeLowCardinalityAndNullable(haystack_type)).isString();

            /// A group is rewritten to `multiSearchAny` (pure substrings) or `multiMatchAny` (regexps).
            /// We never emit a combined `match('(p1)|(p2)|...')` alternation: benchmarking on `hits`
            /// showed the RE2 alternation is consistently slower than the original short-circuit `OR`
            /// across the whole realistic range (still ~1.8x slower at 20 branches), so when neither fast
            /// path applies we keep the original branches — always correct and never a regression.
            const bool can_use_multi_search = haystack_is_string && info.canUseMultiSearchAny();

            /// The two rewrite targets have different fixed setup costs, so each has its own minimum
            /// branch count: `multiSearchAny` (substrings) pays off from ~4 branches, `multiMatchAny`
            /// (Vectorscan) only from ~9. See `optimize_or_like_chain_min_substrings` /
            /// `optimize_or_like_chain_min_patterns`. Below the threshold the chain is kept as written.
            const size_t min_for_rewrite = can_use_multi_search ? min_substrings_for_rewrite : min_patterns_for_rewrite;
            const bool enough_patterns = info.patterns.size() >= min_for_rewrite;

            /// `keep_originals` is set when a pattern failed to convert to a regexp (see the collection
            /// loop): keep the original branches so the query's runtime error / short-circuit behavior
            /// is preserved instead of failing eagerly during optimization.
            const bool eligible = enough_patterns && !key_data.keep_originals;

            if (eligible && can_use_multi_search)
            {
                /// Use `multiSearchAny` or `multiSearchAnyCaseInsensitiveUTF8` for pure substring patterns.
                /// `multiSearchAny*` operates on raw substrings, not regexps, so the hyperscan
                /// regexp size limits are not applicable here.
                String func_name = info.needsCaseInsensitive() ? "multiSearchAnyCaseInsensitiveUTF8" : "multiSearchAny";
                match_function = std::make_shared<FunctionNode>(func_name);
                match_function->getArguments().getNodes().push_back(key_data.key);
                match_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(Field{info.getSubstrings()}));
                auto resolver = FunctionFactory::instance().get(func_name, context);
                match_function->resolveAsFunction(resolver);
            }
            else if (eligible && !can_use_multi_search && allow_hyperscan && haystack_is_string
                && info.fitsHyperscanLimits(max_hyperscan_regexp_length, max_hyperscan_regexp_total_length)
                && !info.hasRawRegexp() && info.allRegexpsValidUTF8() && info.allRegexpsHaveNoEmbeddedNul()
                && !(reject_expensive_hyperscan_regexps && info.hasExpensiveRegexp()))
            {
                /// Use `multiMatchAny` for non-substring patterns; it evaluates all regexps in a single
                /// Vectorscan pass instead of a short-circuit chain of `match`/`LIKE` calls.
                /// `multiMatchAny` enforces `max_hyperscan_regexp_length`, `max_hyperscan_regexp_total_length`
                /// and `reject_expensive_hyperscan_regexps` at execution time and compiles patterns as UTF-8,
                /// so we pre-check those guards and that all patterns are valid UTF-8; that way the rewrite
                /// cannot turn a previously-working query into a `BAD_ARGUMENTS` / `CANNOT_COMPILE_REGEXP` /
                /// `HYPERSCAN_CANNOT_SCAN_TEXT` failure. `hasRawRegexp` keeps chains that contain a raw
                /// `match()` regexp off this path (Vectorscan rejects RE2-only syntax such as `\C` even when
                /// the bytes are valid UTF-8), and `allRegexpsHaveNoEmbeddedNul` keeps chains whose regexps
                /// contain an embedded NUL off it (Vectorscan truncates at the first NUL, matching a broader
                /// set than the length-aware RE2 of the original `like`/`ilike`). A group that fails any of
                /// these checks keeps its original branches (below).
                match_function = std::make_shared<FunctionNode>("multiMatchAny");
                match_function->getArguments().getNodes().push_back(key_data.key);
                match_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(Field{info.getRegexps()}));
                auto resolver = FunctionFactory::instance().get("multiMatchAny", context);
                match_function->resolveAsFunction(resolver);
            }

            /// Collect originals for the indexHint payload. Done for *all* keys (including kept-as-is)
            /// so the hint represents the full original chain — see comment above.
            if (is_pure_like_chain)
                for (const auto & original : key_data.originals)
                    index_hint_originals.push_back(original->clone());

            if (!match_function)
            {
                /// We reach here when the group is below the per-target threshold, or no fast path
                /// applies: a `FixedString`/`Enum` haystack, Hyperscan disabled/unavailable, a raw
                /// `match()` regexp, a non-UTF-8 pattern, an embedded NUL, an over-limit or expensive
                /// regexp, or a pattern that failed to convert. We do not fall back to a combined
                /// `match` alternation (it regresses — see the comment above), so we keep the original
                /// `OR LIKE` branches: always executable and result-preserving.
                slot = std::move(key_data.originals);
                continue;
            }

            slot.push_back(std::move(match_function));
            any_rewrite = true;
        }

        if (!any_rewrite)
            return;

        QueryTreeNodePtr index_hint_node;
        if (is_pure_like_chain && !index_hint_originals.empty())
        {
            QueryTreeNodePtr index_hint_arg;
            if (index_hint_originals.size() == 1)
            {
                index_hint_arg = std::move(index_hint_originals[0]);
            }
            else
            {
                auto original_or = std::make_shared<FunctionNode>("or");
                original_or->getArguments().getNodes() = std::move(index_hint_originals);
                original_or->resolveAsFunction(or_function_resolver);
                index_hint_arg = std::move(original_or);
            }

            auto index_hint_resolver = FunctionFactory::instance().get("indexHint", context);
            auto index_hint_fn = std::make_shared<FunctionNode>("indexHint");
            index_hint_fn->getArguments().getNodes().push_back(std::move(index_hint_arg));
            index_hint_fn->resolveAsFunction(index_hint_resolver);
            index_hint_node = std::move(index_hint_fn);
        }

        /// Flatten the slot list back into the OR's argument list.
        QueryTreeNodes flattened;
        for (auto & slot : slots)
            for (auto & elem : slot)
                flattened.push_back(std::move(elem));

        /// OR must have at least two arguments.
        if (flattened.size() == 1)
            flattened.push_back(std::make_shared<ConstantNode>(static_cast<UInt8>(0)));

        function_node->getArguments().getNodes() = std::move(flattened);
        function_node->resolveAsFunction(or_function_resolver);

        if (!index_hint_node)
            return;

        /// Clone the optimized OR node
        auto optimized_or_clone = node->clone();

        /// Create AND of the optimized OR with indexHint:
        /// optimized_or AND indexHint(original_like_chain)
        auto and_node = std::make_shared<FunctionNode>("and");
        and_node->getArguments().getNodes().push_back(std::move(optimized_or_clone));
        and_node->getArguments().getNodes().push_back(std::move(index_hint_node));
        and_node->resolveAsFunction(and_function_resolver);

        /// Mark the AND node as processed to prevent re-visiting
        processed_nodes.insert(and_node.get());

        /// Also mark the cloned OR inside AND as processed
        if (auto * or_child = and_node->getArguments().getNodes()[0]->as<FunctionNode>())
            processed_nodes.insert(or_child);

        /// Replace the original node with the AND expression
        node = std::move(and_node);
    }
private:
    const FunctionOverloadResolverPtr or_function_resolver;
    const FunctionOverloadResolverPtr and_function_resolver;
    std::unordered_set<const FunctionNode *> processed_nodes;
};

}

void ConvertOrLikeChainPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    auto or_function_resolver = createInternalFunctionOrOverloadResolver();
    auto and_function_resolver = createInternalFunctionAndOverloadResolver();

    ConvertOrLikeChainVisitor visitor(std::move(or_function_resolver), std::move(and_function_resolver), std::move(context));
    visitor.visit(query_tree_node);
}

}
