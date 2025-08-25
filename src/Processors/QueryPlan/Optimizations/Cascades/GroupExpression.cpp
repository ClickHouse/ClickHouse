#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

String GroupExpression::getName() const
{
    return node.step->getSerializationName();
}

String GroupExpression::getDescription() const
{
    return node.step->getSerializationName() + " " + node.step->getStepDescription();
}

bool GroupExpression::isApplied(const IOptimizationRule & rule) const
{
    return applied_rules.contains(rule.getName());
}

void GroupExpression::setApplied(const IOptimizationRule & rule)
{
    applied_rules.insert(rule.getName());
}

}
