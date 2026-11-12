#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

bool Group::addLogicalExpression(GroupExpressionPtr group_expression)
{
    /// Drop only a structurally-equal duplicate; a mere fingerprint hash collision keeps both.
    auto & same_fingerprint = logical_expressions_by_fingerprint[group_expression->fingerprint()];
    for (const auto * existing : same_fingerprint)
    {
        if (existing->structurallyEqualTo(*group_expression))
            return false;
    }

    group_expression->group_id = group_id;
    same_fingerprint.push_back(group_expression.get());
    logical_expressions.push_back(std::move(group_expression));
    return true;
}

GroupExpressionPtr Group::addPhysicalExpression(GroupExpressionPtr group_expression)
{
    /// Drop only a structurally-equal duplicate; a mere fingerprint hash collision keeps both.
    auto & same_fingerprint = physical_expressions_by_fingerprint[group_expression->fingerprint()];
    for (const auto & existing : same_fingerprint)
    {
        if (existing->structurallyEqualTo(*group_expression))
            return existing;
    }

    group_expression->group_id = group_id;
    same_fingerprint.push_back(group_expression);
    physical_expressions.push_back(group_expression);
    return group_expression;
}

bool Group::isOptimizedFor(const ExpressionProperties & required_properties) const
{
    return optimized_properties.contains(required_properties);
}

void Group::setOptimizedFor(const ExpressionProperties & required_properties)
{
    optimized_properties.insert(required_properties);
}

bool Group::isEnforcedFor(const ExpressionProperties & required_properties) const
{
    return enforced_properties.contains(required_properties);
}

void Group::setEnforcedFor(const ExpressionProperties & required_properties)
{
    enforced_properties.insert(required_properties);
}

bool Group::isFullyDoneFor(const ExpressionProperties & required_properties) const
{
    return fully_done_properties.contains(required_properties);
}

void Group::setFullyDoneFor(const ExpressionProperties & required_properties)
{
    fully_done_properties.insert(required_properties);
}

void Group::updateBestImplementation(GroupExpressionPtr expression, const CostConfig & cost_config)
{
    UInt64 key = distributionKey(expression->properties.distribution);
    auto & bucket = best_implementations[key];

    /// An expression that is cheaper and has at least the same properties replaces another one.
    /// An enforcer does not: it answers only some required properties (see `GroupExpression::answers`),
    /// so an expression that it would replace can still be the only answer to other ones.
    /// Only the matching distribution-shape bucket needs checking - `isSatisfiedBy` requires
    /// exact match on (node_count, is_replicated).
    for (auto best_it = bucket.begin(); best_it != bucket.end();)
    {
        /// The expression got its cost again.
        if (*best_it == expression)
            return;

        if (!(*best_it)->isEnforcer() &&
            expression->properties.isSatisfiedBy((*best_it)->properties) &&
            (*best_it)->cost->subtree_cost.total(cost_config) <= expression->cost->subtree_cost.total(cost_config))
        {
            /// There is already a cheaper implementation that satisfies the same properties
            return;
        }

        if (!expression->isEnforcer() &&
            (*best_it)->properties.isSatisfiedBy(expression->properties) &&
            (*best_it)->cost->subtree_cost.total(cost_config) > expression->cost->subtree_cost.total(cost_config))
        {
            /// Keep the bucket order: when alternatives cost the same, the first one wins.
            /// A cheaper swap-with-the-last removal would reorder them and change plans.
            best_it = bucket.erase(best_it);
        }
        else
        {
            ++best_it;
        }
    }

    bucket.push_back(std::move(expression));
}

ExpressionWithCost Group::getBestImplementation(
    const ExpressionProperties & required_properties,
    const CostConfig & cost_config,
    const std::unordered_set<GroupExpression *> & excluded) const
{
    UInt64 key = distributionKey(required_properties.distribution);
    auto it = best_implementations.find(key);
    if (it == best_implementations.end())
        return {};

    GroupExpressionPtr found_best;
    for (const auto & expression : it->second)
    {
        if (excluded.contains(expression.get()) || !expression->answers(required_properties))
            continue;

        if (!found_best || found_best->cost->subtree_cost.total(cost_config) > expression->cost->subtree_cost.total(cost_config))
            found_best = expression;
    }

    if (!found_best)
        return {};

    return {found_best, *found_best->cost};
}

void Group::dump(WriteBuffer & out, const CostConfig & cost_config, String indent) const
{
    if (statistics.has_value())
    {
        out << indent << "Statistics: rows: " << statistics->estimated_row_count << "\n";
    }

    out << indent << "Logical:\n";
    for (const auto & expression : logical_expressions)
    {
        out << indent << indent;
        expression->dump(out, cost_config);
        out << "\n";
    }

    out << indent << "Physical:\n";
    for (const auto & expression : physical_expressions)
    {
        out << indent << indent;
        expression->dump(out, cost_config);
        out << "\n";
    }

    for (const auto & [_, bucket] : best_implementations)
    {
        for (const auto & best : bucket)
        {
            out << indent << "Best for " << best->properties.dump() << ":\n"
                << indent << indent
                << "Cost: local(" << best->cost->cost.dump(cost_config) << ")"
                << " subtree(" << best->cost->subtree_cost.dump(cost_config) << ")"
                << " : " << best->getDescription();
            /// The inputs with their required properties let the reader follow the winning
            /// chain down the memo (which bucket of each child group this plan was built from).
            for (const auto & input : best->inputs)
                out << " <- #" << input.group_id << " " << input.required_properties.dump();
            out << "\n";
        }
    }
}

String Group::dump(const CostConfig & cost_config) const
{
    WriteBufferFromOwnString out;
    dump(out, cost_config);
    return out.str();
}

}
