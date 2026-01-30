#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <IO/Operators.h>

namespace DB
{

String GroupExpression::getName() const
{
    if (plan_step)
        return plan_step->getSerializationName();
//    if (original_node && original_node->step)
//        return original_node->step->getSerializationName();
    return {};
}

String GroupExpression::getDescription() const
{
    String description;
    if (plan_step)
        description = plan_step->getStepDescription();
//    if (original_node && original_node->step)
//        description = original_node->step->getStepDescription();
    if (description.empty())
        return getName();
    return getName() + " " + description;
}

IQueryPlanStep * GroupExpression::getQueryPlanStep() const
{
//    if (plan_step)
        return plan_step.get();
//    if (original_node)
//        return original_node->step.get();
//    return nullptr;
}

bool GroupExpression::isApplied(const IOptimizationRule & rule, const ExpressionProperties & required_properties) const
{
    String full_name = rule.getName() + '@' + required_properties.dump();
    return applied_rules.contains(full_name);
}

void GroupExpression::setApplied(const IOptimizationRule & rule, const ExpressionProperties & required_properties)
{
    String full_name = rule.getName() + '@' + required_properties.dump();
    applied_rules.insert(full_name);
}

void GroupExpression::dump(WriteBuffer & out) const
{
    properties.dump(out);
    out << " ";
    for (const auto & enforcer_step : property_enforcer_steps)
        out << enforcer_step->getName() << " ";

    out << "'" << getDescription() << "' inputs:";
    for (const auto & input : inputs)
        out << " #" << input.group_id;
    if (statistics.has_value())
        out << " rows: " << statistics->estimated_row_count;
    if (cost.has_value())
        out << " cost: " << cost->subtree_cost;
}

String GroupExpression::dump() const
{
    WriteBufferFromOwnString out;
    dump(out);
    return out.str();
}

}
