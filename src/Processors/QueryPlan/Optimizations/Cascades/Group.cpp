#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

//#include <iostream>

namespace DB
{

void Group::addLogicalExpression(GroupExpressionPtr group_expression)
{
    group_expression->group_id = group_id;
    logical_expressions.push_back(std::move(group_expression));
}

void Group::addPhysicalExpression(GroupExpressionPtr group_expression)
{
    group_expression->group_id = group_id;

    if (!physical_fingerprints.insert(group_expression->fingerprint()).second)
        return;

    physical_expressions.push_back(std::move(group_expression));
}

bool Group::isOptimizedFor(const ExpressionProperties & required_properties) const
{
    return optimized_properties.contains(required_properties.dump());
}

void Group::setOptimizedFor(const ExpressionProperties & required_properties)
{
    optimized_properties.insert(required_properties.dump());
}

bool Group::isFullyDoneFor(const ExpressionProperties & required_properties) const
{
    return fully_done_properties.contains(required_properties.dump());
}

void Group::setFullyDoneFor(const ExpressionProperties & required_properties)
{
    fully_done_properties.insert(required_properties.dump());
}

void Group::updateBestImplementation(GroupExpressionPtr expression, const CostConfig & cost_config)
{
    /// Remove all known best expressions with higher cost and properties satisfied by the new expression
    for (auto best_it = best_implementations.begin(); best_it != best_implementations.end();)
    {
        if (expression->properties.isSatisfiedBy((*best_it)->properties) &&
            (*best_it)->cost->subtree_cost.weighted_total(cost_config) <= expression->cost->subtree_cost.weighted_total(cost_config))
        {
            /// There is already a cheaper implementation that satisfies the same properties
            return;
        }

        if ((*best_it)->properties.isSatisfiedBy(expression->properties) &&
            (*best_it)->cost->subtree_cost.weighted_total(cost_config) > expression->cost->subtree_cost.weighted_total(cost_config))
        {
            best_it = best_implementations.erase(best_it);
        }
        else
        {
            ++best_it;
        }
    }

    best_implementations.insert(expression);
}

ExpressionWithCost Group::getBestImplementation(const ExpressionProperties & required_properties, const CostConfig & cost_config) const
{
    GroupExpressionPtr found_best;
    for (const auto & expression : best_implementations)
    {
        if (required_properties.isSatisfiedBy(expression->properties) &&
            (!found_best || found_best->cost->subtree_cost.weighted_total(cost_config) > expression->cost->subtree_cost.weighted_total(cost_config)))
        {
            found_best = expression;
        }
    }

//    std::cerr
//        << "Get Best " << required_properties.dump() << " from group #" << group_id << "\n"
//        << dump()
//        << "\nFound:\n"
//        << (found_best ? found_best->dump() : String())
//        << "\n\n\n";

    if (!found_best)
        return {};

    return {found_best, *found_best->cost};
}

Float64 Group::getBestCostForProperties(const ExpressionProperties & required_properties, const CostConfig & cost_config) const
{
    auto best = getBestImplementation(required_properties, cost_config);
    if (!best.expression)
        return std::numeric_limits<Float64>::infinity();
    return best.cost.subtree_cost.weighted_total(cost_config);
}

void Group::dump(WriteBuffer & out, String indent) const
{
    if (statistics.has_value())
    {
        out << indent << "Statistics: rows: " << statistics->estimated_row_count << "\n";
    }

    out << indent << "Logical:\n";
    for (const auto & expression : logical_expressions)
    {
        out << indent << indent;
        expression->dump(out);
        out << "\n";
    }

    out << indent << "Physical:\n";
    for (const auto & expression : physical_expressions)
    {
        out << indent << indent;
        expression->dump(out);
        out << "\n";
    }

    for (const auto & best : best_implementations)
    {
        out << indent << "Best for " << best->properties.dump() << ":\n"
            << indent << indent
            << "Cost: " << best->cost->cost.total() << " (subtree: " << best->cost->subtree_cost.total() << ") : "
            << best->getDescription() << "\n";
    }
}

String Group::dump() const
{
    WriteBufferFromOwnString out;
    dump(out);
    return out.str();
}

}
