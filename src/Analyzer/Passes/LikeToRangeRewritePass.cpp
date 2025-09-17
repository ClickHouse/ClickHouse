#include <Analyzer/Passes/LikeToRangeRewritePass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Core/Settings.h>
#include <Common/StringUtils.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>

#include <boost/algorithm/string.hpp>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_like_to_range;
}

namespace
{

/** Rewrite LIKE expressions with perfect prefix to range expressions.
  * For example, `col LIKE 'ClickHouse%'` is rewritten as `col >= ClickHouse AND col < ClickHousf`.
  * The scope of the rewrite:
  *     - `col LIKE      'Prefix%'`: as `col >= 'Prefix' AND col <  'Prefy'`
  *     - `col NOT LIKE  'Prefix%'`: as `col <  'Prefix' OR  col >= 'Prefy'`
  *     - `col ILIKE     'Prefix%'`: as `lower(col) >= 'prefix' AND lower(col) <  'prefy'`
  *     - `col NOT ILIKE 'Prefix%'`: as `lower(col) <  'prefix' OR  lower(col) >= 'prefy'`
  * Type requirement on the left operand, `col`:
  *     - Only resolved column of type `String` or `FixedString`
  *     - `LowCardinality(String)` is unsupported due to non-order-preserving encoding
  */
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
        if (!(is_like || is_not_like))
            return;

        auto & args = function_node->getArguments().getNodes();
        if (args.size() != 2)
            return;

        /// Do not rewrite for non-column, non-string left hand side
        if (args[0]->getNodeType() != QueryTreeNodeType::COLUMN || !isStringOrFixedString(args[0]->getResultType()))
            return;

        /// Extract affix (prefix or suffix) and check if suitable for rewrite
        auto * pattern_constant = args[1]->as<ConstantNode>();
        if (!pattern_constant || !isString(pattern_constant->getResultType()))
            return;

        auto pattern = pattern_constant->getValue().safeGet<String>();
        const bool is_suffix = pattern.starts_with("%");

        /// Do not rewrite for FixedString LIKE suffix, which requires triming trailing null chars
        if (is_suffix && isFixedString(args[0]->getResultType()))
            return;

        /// Only rewrite for perfect prefix or suffix
        /// Suffix is prefix in reverse
        if (is_suffix)
            std::reverse(pattern.begin(), pattern.end());

        auto [affix, is_perfect] = extractFixedPrefixFromLikePattern(pattern, true);
        if (!is_perfect || affix.empty())
            return;

        if (is_suffix)
            std::reverse(affix.begin(), affix.end());

        auto affix_constant = std::make_shared<ConstantNode>(std::move(affix));

        auto column_node = args[0]; /// The column being compared
        chassert(column_node != nullptr, "Column node should be non-null");

        /// Create startsWith/endsWith function
        FunctionNodePtr new_node = operation(is_suffix ? "endsWith" : "startsWith", column_node, affix_constant);
        if (is_not_like)
            new_node = operation("not", new_node);

        /// Replpace the original LIKE node
        chassert(new_node != nullptr, "should have been created");
        chassert(new_node->getResultType()->getTypeId() == node->getResultType()->getTypeId(), "Rewrite should preserve type");
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

