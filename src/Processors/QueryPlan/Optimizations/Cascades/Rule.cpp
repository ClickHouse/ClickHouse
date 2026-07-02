#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>

namespace DB
{

std::vector<GroupExpressionPtr> IOptimizationRule::apply(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto new_expressions = applyImpl(expression, required_properties, memo);
    /// Note: Statistics are now stored in Group, not in GroupExpression, so no need to copy them
    expression->setApplied(*this, required_properties);
    return new_expressions;
}

}
