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
    if (enforcer_axis != EnforcerAxis::None)
        out << " [enforcer:" << (enforcer_axis == EnforcerAxis::Sorting ? "Sorting" : "Distribution") << "]";
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

/// The one list of components that define an expression's structural identity; equality and
/// the fingerprint hash must not diverge from it.
struct GroupExpressionIdentityView
{
    String name;
    String description;
    String strategy_name;
    const ExpressionProperties & properties;
    const std::vector<GroupExpression::Input> & inputs;
};

static GroupExpressionIdentityView identityView(const GroupExpression & expression)
{
    return {
        expression.getName(),
        expression.getDescription(),
        expression.strategy ? expression.strategy->getName() : String{},
        expression.properties,
        expression.inputs};
}

bool GroupExpression::structurallyEqualTo(const GroupExpression & other) const
{
    const auto left = identityView(*this);
    const auto right = identityView(other);

    if (left.name != right.name || left.description != right.description || left.strategy_name != right.strategy_name)
        return false;

    if (!(left.properties == right.properties))
        return false;

    if (left.inputs.size() != right.inputs.size())
        return false;
    for (size_t i = 0; i < left.inputs.size(); ++i)
    {
        if (left.inputs[i].group_id != right.inputs[i].group_id)
            return false;
        if (!(left.inputs[i].required_properties == right.inputs[i].required_properties))
            return false;
    }
    return true;
}

size_t GroupExpression::fingerprint() const
{
    const auto view = identityView(*this);

    size_t hash_value = std::hash<String>()(view.name);
    boost::hash_combine(hash_value, std::hash<String>()(view.description));
    boost::hash_combine(hash_value, std::hash<String>()(view.strategy_name));
    boost::hash_combine(hash_value, ExpressionPropertiesHash()(view.properties));
    for (const auto & input : view.inputs)
    {
        boost::hash_combine(hash_value, input.group_id);
        boost::hash_combine(hash_value, ExpressionPropertiesHash()(input.required_properties));
    }
    return hash_value;
}

}
