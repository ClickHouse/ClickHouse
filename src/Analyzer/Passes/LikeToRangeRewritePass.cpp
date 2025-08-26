#include <Analyzer/Passes/LikeToRangeRewritePass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_like_to_range;
}

namespace
{

class LikeToRangeRewriteVisitor : public InDepthQueryTreeVisitorWithContext<LikeToRangeRewriteVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<LikeToRangeRewriteVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node) 
    {
        if (!getSettings()[Setting::optimize_rewrite_like_to_range])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        const String & function_name = function_node->getFunctionName();
        const bool is_like = (function_name == "like");
        // todo: ilike
        if (!is_like)
            return;

        auto & args = function_node->getArguments().getNodes();
        if (args.size() != 2)
            return;

        auto * pattern_constant = args[1]->as<ConstantNode>();
        if (!pattern_constant || !isString(pattern_constant->getResultType()))
            return;

        /// Extract prefix and check if it's suitable for rewrite
        auto pattern = pattern_constant->getValue().safeGet<String>();
        auto [prefix, is_perfect] = extractFixedPrefixFromLikePattern(pattern, true);

        if (!is_perfect || prefix.empty())
            return;

        /// Replace the node with the AND condition
        auto and_function = std::make_shared<FunctionNode>("and");
        auto ge_function = std::make_shared<FunctionNode>("greaterOrEquals");
        auto column_node = args[0]; // The column being compared

        auto ge_resolver = FunctionFactory::instance().get("greaterOrEquals", getContext());
        auto lt_resolver = FunctionFactory::instance().get("less", getContext());
        auto and_resolver = FunctionFactory::instance().get("and", getContext());

        /// Set new function arguments
        auto prefix_constant = std::make_shared<ConstantNode>(Field(prefix));
        ge_function->getArguments().getNodes() = {column_node, prefix_constant};
        ge_function->resolveAsFunction(ge_resolver);

        String right_bound = firstStringThatIsGreaterThanAllStringsWithPrefix(prefix);
        if (right_bound.empty()) 
        {
            and_function->getArguments().getNodes() = {ge_function};
        }
        else
        {
            /// Set new function arguments
            auto right_bound_constant = std::make_shared<ConstantNode>(right_bound);
            auto lt_function = std::make_shared<FunctionNode>("less");
            lt_function->getArguments().getNodes() = {column_node, right_bound_constant};
            lt_function->resolveAsFunction(lt_resolver);

            and_function->getArguments().getNodes() = {ge_function, lt_function};
        }
        and_function->resolveAsFunction(and_resolver);

        /// Replpace the original LIKE node
        // todo: check and_function type is "and"
        node = std::move(and_function);
    }
};

}

void LikeToRangeRewritePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context) 
{
    LikeToRangeRewriteVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}

