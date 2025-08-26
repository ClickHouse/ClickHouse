#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

String GroupExpression::getName() const
{
    if (plan_step)
        return plan_step->getSerializationName();
    if (original_node && original_node->step)
        return original_node->step->getSerializationName();
    return {};
}

String GroupExpression::getDescription() const
{
    String description;
    if (plan_step)
        description = plan_step->getStepDescription();
    if (original_node && original_node->step)
        description = original_node->step->getStepDescription();
    if (description.empty())
        return getName();
    return getName() + " " + description;
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
