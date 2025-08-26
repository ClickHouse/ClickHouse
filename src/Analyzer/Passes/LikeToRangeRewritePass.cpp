#include <memory>
#include <Analyzer/IQueryTreeNode.h>
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
        const bool is_not_like = (function_name == "notLike");
        // todo: ilike
        if (!(is_like || is_not_like))
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

        String right_bound = firstStringThatIsGreaterThanAllStringsWithPrefix(prefix);
        const bool no_right_bound = right_bound.empty();

        /// Create new node
        QueryTreeNodePtr new_node = nullptr;
        auto column_node = args[0]; // The column being compared
        if (is_like)
        {
            auto left = comparison(column_node, "greaterOrEquals", prefix);
            new_node = no_right_bound 
                ? left 
                : operation(left, "and", comparison(column_node, "less", right_bound));
        } 
        else if (is_not_like)
        {
            auto left = comparison(column_node, "less", prefix);
            new_node = no_right_bound
                ? left
                : operation(left, "or", comparison(column_node, "greaterOrEquals", right_bound));
        }
        else
            chassert(false, "shouldn't be here");

        /// Replpace the original LIKE node
        chassert(new_node != nullptr, "should have been created");
        node = std::move(new_node);
    }
private:
    FunctionNodePtr comparison(const QueryTreeNodePtr left, const String & compare, const String & right)
    {
        auto right_constant = std::make_shared<ConstantNode>(right);
        return operation(left, compare, right_constant);
    }

    FunctionNodePtr operation(const QueryTreeNodePtr left, const String & op, const QueryTreeNodePtr right)
    {
        auto op_function = std::make_shared<FunctionNode>(op);
        auto op_resolver = FunctionFactory::instance().get(op, getContext());
        op_function->getArguments().getNodes() = {left, right};
        op_function->resolveAsFunction(op_resolver);
        return op_function;
    }
};

}

void LikeToRangeRewritePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context) 
{
    LikeToRangeRewriteVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}

