#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <IO/Operators.h>
#include <boost/functional/hash.hpp>
#include <algorithm>

namespace DB
{

String GroupExpression::getName() const
{
    if (plan_step)
        return plan_step->getSerializationName();
    return {};
}

String GroupExpression::getDescription() const
{
    String description;
    if (plan_step)
        description = plan_step->getStepDescription();
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
    if (isEnforcer())
    {
        out << " [enforcer for";
        for (const auto & enforced_properties : enforced_for)
            out << " " << enforced_properties.dump();
        out << "]";
    }
    out << " inputs:";
    for (const auto & input : inputs)
        out << " #" << input.group_id;
    if (cost.has_value())
        out << " cost: " << cost->subtree_cost.total(cost_config);
}

bool GroupExpression::answers(const ExpressionProperties & required_properties) const
{
    if (!required_properties.isSatisfiedBy(properties))
        return false;

    return !isEnforcer()
        || properties == required_properties
        || std::find(enforced_for.begin(), enforced_for.end(), required_properties) != enforced_for.end();
}

String GroupExpression::dump(const CostConfig & cost_config) const
{
    WriteBufferFromOwnString out;
    dump(out, cost_config);
    return out.str();
}

/// Equality and the fingerprint hash must cover the same identity components, in this order:
/// properties, inputs (group and required properties), step name, step description, strategy
/// name. `getName` and `getDescription` build their strings, so equality compares the cheap
/// components first and materializes the strings one at a time.
bool GroupExpression::structurallyEqualTo(const GroupExpression & other) const
{
    if (!(properties == other.properties))
        return false;

    if (inputs.size() != other.inputs.size())
        return false;
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        if (inputs[i].group_id != other.inputs[i].group_id)
            return false;
        if (!(inputs[i].required_properties == other.inputs[i].required_properties))
            return false;
    }

    if (getName() != other.getName() || getDescription() != other.getDescription())
        return false;

    /// Strategies are per-type singletons (`strategySingleton`), so equal pointers mean the
    /// same strategy type.
    return strategy == other.strategy;
}

size_t GroupExpression::fingerprint() const
{
    size_t hash_value = ExpressionPropertiesHash()(properties);
    for (const auto & input : inputs)
    {
        boost::hash_combine(hash_value, input.group_id);
        boost::hash_combine(hash_value, ExpressionPropertiesHash()(input.required_properties));
    }
    boost::hash_combine(hash_value, std::hash<String>()(getName()));
    boost::hash_combine(hash_value, std::hash<String>()(getDescription()));
    if (strategy)
        boost::hash_combine(hash_value, std::hash<String>()(strategy->getName()));
    return hash_value;
}

}
