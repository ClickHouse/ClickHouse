#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <IO/Operators.h>
#include <boost/functional/hash.hpp>

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

const IQueryPlanStep * GroupExpression::getQueryPlanStep() const
{
    return plan_step.get();
}

bool GroupExpression::isApplied(const IOptimizationRule & rule, const ExpressionProperties & required_properties) const
{
    return applied_rules.contains({&rule, required_properties});
}

void GroupExpression::setApplied(const IOptimizationRule & rule, const ExpressionProperties & required_properties)
{
    applied_rules.insert({&rule, required_properties});
}

void GroupExpression::dump(WriteBuffer & out, const CostConfig & cost_config) const
{
    properties.dump(out);
    out << " '" << getDescription() << "'";
    if (strategy)
        out << " [" << strategy->getName() << "]";
    out << " inputs:";
    for (const auto & input : inputs)
        out << " #" << input.group_id;
    if (cost.has_value())
        out << " cost: " << cost->subtree_cost.total(cost_config);
}

String GroupExpression::dump(const CostConfig & cost_config) const
{
    WriteBufferFromOwnString out;
    dump(out, cost_config);
    return out.str();
}

size_t GroupExpression::fingerprint() const
{
    size_t h = std::hash<String>()(getDescription());
    if (strategy)
        boost::hash_combine(h, std::hash<String>()(strategy->getName()));
    boost::hash_combine(h, ExpressionPropertiesHash()(properties));
    for (const auto & input : inputs)
    {
        boost::hash_combine(h, input.group_id);
        boost::hash_combine(h, ExpressionPropertiesHash()(input.required_properties));
    }
    return h;
}

}
