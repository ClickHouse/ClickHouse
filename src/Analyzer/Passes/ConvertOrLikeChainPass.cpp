#include <Analyzer/Passes/ConvertOrLikeChainPass.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config.h"

#include <Common/likePatternToRegexp.h>

#include <Core/Field.h>

#include <DataTypes/DataTypesNumber.h>

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
    extern const SettingsBool reject_expensive_hyperscan_regexps;
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

    Array getRegexps() const
    {
        Array result;
        for (const auto & p : patterns)
            result.push_back(p.regexp);
        return result;
    }

    /// Build a combined regexp pattern using alternation: (pattern1)|(pattern2)|...
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
    /// `getCombinedRegexp` would build (the `match` fallback when Vectorscan is unavailable
    /// or disallowed). The alternation adds `(`, `)` and `|` overhead per branch, so the
    /// final regexp can be substantially larger than the sum of raw pattern sizes. Pre-checking
    /// the combined size lets the rewrite back off to the original `OR LIKE` chain instead of
    /// emitting a `match` regexp that could blow up RE2 compile limits.
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
    bool hasExpensiveRegexp() const
    {
        SlowWithHyperscanChecker checker;
        for (const auto & p : patterns)
            if (checker.isSlow(p.regexp))
                return true;
        return false;
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

            auto it = key_to_index.find(like_first_argument);
            size_t idx;
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
        const size_t min_patterns_for_rewrite = context->getSettingsRef()[Setting::optimize_or_like_chain_min_patterns];

        /// `indexHint(X) AND expr` restricts index analysis to granules that may satisfy `X`. To stay
        /// correct, `X` must be a *superset* of the rows the outer OR can match — otherwise we'd
        /// prune granules whose only matching rows come from branches missing in `X`. We therefore
        /// wrap with `indexHint(<original full OR chain>)` only when every OR branch is a
        /// LIKE/ILIKE/match (no `URL = 'foo'`-style branches that the index might match independently
        /// of the LIKE chain), and include originals for *all* per-key groups — both rewritten and
        /// the ones we keep as-is due to hyperscan size limits.
        const bool is_pure_like_chain = per_key_data.size() == slots.size();

        bool any_rewrite = false;
        QueryTreeNodes index_hint_originals;

        for (auto & key_data : per_key_data)
        {
            auto & info = key_data.info;
            QueryTreeNodes & slot = slots[key_data.slot_index];

            std::shared_ptr<FunctionNode> match_function;

            /// Skip rewriting groups with too few patterns — `optimize_or_like_chain_min_patterns` is
            /// calibrated so the rewrite is only applied where it is consistently beneficial (see the
            /// setting's documentation). We still let the loop reach the indexHint-originals collection
            /// below, then fall through to the "keep originals" branch, so the chain stays as written.
            const bool too_few_patterns = info.patterns.size() < min_patterns_for_rewrite;

            if (!too_few_patterns && info.canUseMultiSearchAny())
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
            else if (!too_few_patterns && info.fitsHyperscanLimits(max_hyperscan_regexp_length, max_hyperscan_regexp_total_length))
            {
                if (allow_hyperscan && !(reject_expensive_hyperscan_regexps && info.hasExpensiveRegexp()))
                {
                    /// Use `multiMatchAny` for non-substring patterns. It is significantly faster than
                    /// `match` with alternation because it can leverage Vectorscan/Hyperscan when available
                    /// and falls back to RE2 alternation otherwise.
                    /// `multiMatchAny` enforces `max_hyperscan_regexp_length`,
                    /// `max_hyperscan_regexp_total_length` and `reject_expensive_hyperscan_regexps`
                    /// at execution time, so we pre-check those guards; that way the rewrite cannot
                    /// turn a previously-working query into a `BAD_ARGUMENTS` /
                    /// `HYPERSCAN_CANNOT_SCAN_TEXT` failure.
                    match_function = std::make_shared<FunctionNode>("multiMatchAny");
                    match_function->getArguments().getNodes().push_back(key_data.key);
                    match_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(Field{info.getRegexps()}));
                    auto resolver = FunctionFactory::instance().get("multiMatchAny", context);
                    match_function->resolveAsFunction(resolver);
                }
                else if (info.combinedRegexpFitsHyperscanLimits(max_hyperscan_regexp_length, max_hyperscan_regexp_total_length))
                {
                    /// Fall back to `match` with combined alternation when Hyperscan is disabled or the
                    /// patterns would be rejected as expensive. `combinedRegexpFitsHyperscanLimits`
                    /// accounts for the `(`, `)` and `|` overhead added by `getCombinedRegexp` so
                    /// `max_hyperscan_regexp_total_length` is a strict upper bound on the emitted
                    /// regexp, and we cannot blow up RE2 compile limits here.
                    match_function = std::make_shared<FunctionNode>("match");
                    match_function->getArguments().getNodes().push_back(key_data.key);
                    match_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(Field{info.getCombinedRegexp()}));
                    auto resolver = FunctionFactory::instance().get("match", context);
                    match_function->resolveAsFunction(resolver);
                }
            }

            /// Collect originals for the indexHint payload. Done for *all* keys (including kept-as-is)
            /// so the hint represents the full original chain — see comment above.
            if (is_pure_like_chain)
                for (const auto & original : key_data.originals)
                    index_hint_originals.push_back(original->clone());

            if (!match_function)
            {
                /// Patterns exceed the configured hyperscan size limits. We cannot emit
                /// `multiMatchAny` (would throw at runtime), and emitting a single combined `match`
                /// would build an unbounded regexp that can blow up RE2 compile limits. Keep the
                /// original `OR LIKE` branches so the query remains executable.
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
