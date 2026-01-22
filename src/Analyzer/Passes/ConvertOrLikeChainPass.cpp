#include <Analyzer/Passes/ConvertOrLikeChainPass.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <Common/likePatternToRegexp.h>

#include <Core/Field.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>
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
    extern const SettingsBool optimize_or_like_chain;
}

namespace
{

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

        QueryTreeNodes unique_elems;
        /// Store clones of original LIKE/ILIKE/match expressions for indexHint
        QueryTreeNodes original_like_exprs;

        QueryTreeNodePtrWithHashMap<PatternInfo> node_to_patterns;
        std::vector<QueryTreeNodePtr> pattern_keys;  /// To preserve order of first occurrence
        /// Track positions in unique_elems where we'll insert the match functions
        std::vector<size_t> match_function_positions;

        for (auto & argument : function_node->getArguments())
        {
            unique_elems.push_back(argument);

            auto * argument_function = argument->as<FunctionNode>();
            if (!argument_function)
                continue;

            const bool is_like  = argument_function->getFunctionName() == "like";
            const bool is_ilike = argument_function->getFunctionName() == "ilike";
            const bool is_match = argument_function->getFunctionName() == "match";

            /// Not {i}like or match -> bail out.
            if (!is_like && !is_ilike && !is_match)
                continue;

            const auto & like_arguments = argument_function->getArguments().getNodes();
            if (like_arguments.size() != 2)
                continue;

            const auto & like_first_argument = like_arguments[0];
            const auto * pattern = like_arguments[1]->as<ConstantNode>();
            if (!pattern || !isString(pattern->getResultType()))
                continue;

            const String & pattern_str = pattern->getValue().safeGet<String>();

            PatternData data;
            data.is_case_insensitive = is_ilike;
            data.is_substring = false;

            if (is_match)
            {
                /// match() already has a regexp pattern - use as is
                data.regexp = pattern_str;
                data.is_substring = false;
                /// Check if match pattern starts with (?i) for case insensitivity
                if (data.regexp.starts_with("(?i)"))
                    data.is_case_insensitive = true;
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

            /// Clone the original expression for indexHint before removing it
            original_like_exprs.push_back(argument->clone());

            unique_elems.pop_back();

            auto it = node_to_patterns.find(like_first_argument);
            if (it == node_to_patterns.end())
            {
                it = node_to_patterns.insert({like_first_argument, PatternInfo{}}).first;
                pattern_keys.push_back(like_first_argument);
                /// Remember the position where we'll insert the function later
                match_function_positions.push_back(unique_elems.size());
                /// Add placeholder that will be replaced
                unique_elems.push_back(nullptr);
            }

            it->second.patterns.push_back(std::move(data));
        }

        /// If no patterns were optimized, nothing to do
        if (original_like_exprs.empty())
            return;

        /// Cache context for later use
        auto context = getContext();

        /// Create indexHint with the original LIKE/ILIKE/match expressions for index analysis FIRST
        /// (before modifying the original node)
        QueryTreeNodePtr index_hint_arg;
        if (original_like_exprs.size() == 1)
        {
            index_hint_arg = std::move(original_like_exprs[0]);
        }
        else
        {
            /// Create OR of all original expressions
            auto original_or = std::make_shared<FunctionNode>("or");
            original_or->getArguments().getNodes() = std::move(original_like_exprs);
            original_or->resolveAsFunction(or_function_resolver);
            index_hint_arg = std::move(original_or);
        }

        auto index_hint_resolver = FunctionFactory::instance().get("indexHint", context);
        auto index_hint_node = std::make_shared<FunctionNode>("indexHint");
        index_hint_node->getArguments().getNodes().push_back(std::move(index_hint_arg));
        index_hint_node->resolveAsFunction(index_hint_resolver);

        /// Now create the appropriate function nodes and fill in the placeholders
        for (size_t i = 0; i < pattern_keys.size(); ++i)
        {
            const auto & key = pattern_keys[i];
            auto & info = node_to_patterns.at(key);
            size_t pos = match_function_positions[i];

            std::shared_ptr<FunctionNode> match_function;

            if (info.canUseMultiSearchAny())
            {
                /// Use multiSearchAny or multiSearchAnyCaseInsensitive for pure substring patterns
                String func_name = info.needsCaseInsensitive() ? "multiSearchAnyCaseInsensitive" : "multiSearchAny";
                match_function = std::make_shared<FunctionNode>(func_name);
                match_function->getArguments().getNodes().push_back(key);
                match_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(Field{info.getSubstrings()}));
                auto resolver = FunctionFactory::instance().get(func_name, context);
                match_function->resolveAsFunction(resolver);
            }
            else
            {
                /// Use match() with combined regexp pattern using alternation
                match_function = std::make_shared<FunctionNode>("match");
                match_function->getArguments().getNodes().push_back(key);
                match_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(Field{info.getCombinedRegexp()}));
                auto resolver = FunctionFactory::instance().get("match", context);
                match_function->resolveAsFunction(resolver);
            }

            unique_elems[pos] = std::move(match_function);
        }

        /// OR must have at least two arguments.
        if (unique_elems.size() == 1)
            unique_elems.push_back(std::make_shared<ConstantNode>(static_cast<UInt8>(0)));

        function_node->getArguments().getNodes() = std::move(unique_elems);
        function_node->resolveAsFunction(or_function_resolver);

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
