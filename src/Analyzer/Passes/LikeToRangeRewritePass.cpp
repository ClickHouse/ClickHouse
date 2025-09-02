#include <memory>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Passes/LikeToRangeRewritePass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <boost/algorithm/string.hpp>
#include <Core/Settings.h>
#include <Common/StringUtils.h>
#include <Functions/FunctionFactory.h>

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
        const bool is_ilike = (function_name == "ilike");
        const bool is_not_ilike = (function_name == "notILike");
        if (!(is_like || is_not_like || is_ilike || is_not_ilike))
            return;

        auto & args = function_node->getArguments().getNodes();
        if (args.size() != 2)
            return;

        /// Do not rewrite attributes of low cardinality
        auto col_type = args[0]->getResultType();
        if (WhichDataType(col_type).isLowCardinality())
            return;

        /// Extract prefix and check if it's suitable for rewrite
        auto * pattern_constant = args[1]->as<ConstantNode>();
        if (!pattern_constant || !isString(pattern_constant->getResultType()))
            return;

        auto pattern = pattern_constant->getValue().safeGet<String>();
        auto [prefix, is_perfect] = extractFixedPrefixFromLikePattern(pattern, true);

        if (!is_perfect || prefix.empty())
            return;

        /// Create range bounds
        /// Determine if we need case conversion
        const bool case_insensitive = (is_ilike || is_not_ilike);
        String comparison_prefix = case_insensitive
            ? boost::algorithm::to_lower_copy(prefix)
            : prefix;
        String right_bound = firstStringThatIsGreaterThanAllStringsWithPrefix(comparison_prefix);
        const bool no_right_bound = right_bound.empty();

        auto prefix_constant = std::make_shared<ConstantNode>(std::move(comparison_prefix));
        ConstantNodePtr right_bound_constant = nullptr;
        if (!no_right_bound)
            right_bound_constant = std::make_shared<ConstantNode>(std::move(right_bound));

        /// Create range expression
        FunctionNodePtr new_node = nullptr;
        auto column_node = case_insensitive ? operation("lower", args[0]) : args[0]; /// The column being compared
        chassert(column_node != nullptr, "Column node should be non-null");

        if (is_like || is_ilike)
        {
            auto left = operation("greaterOrEquals", column_node, prefix_constant);
            new_node = no_right_bound ? left : operation(
                "and", left, operation("less", column_node, right_bound_constant));
        }
        else if (is_not_like || is_not_ilike)
        {
            auto left = operation("less", column_node, prefix_constant);
            new_node = no_right_bound ? left : operation(
                "or", left, operation("greaterOrEquals", column_node, right_bound_constant));
        }
        else
            chassert(false, "shouldn't be here");

        /// Replpace the original LIKE node
        chassert(new_node != nullptr, "should have been created");
        node = std::move(new_node);
    }
private:
    template<typename... Args>
    requires (std::is_convertible_v<Args, QueryTreeNodePtr> && ...) && (sizeof...(Args) >= 1)
    FunctionNodePtr operation(const String & op, Args&&... operands)
    {
        auto op_function = std::make_shared<FunctionNode>(op);
        auto op_resolver = FunctionFactory::instance().get(op, getContext());
        op_function->getArguments().getNodes() = {std::forward<Args>(operands)...};
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

