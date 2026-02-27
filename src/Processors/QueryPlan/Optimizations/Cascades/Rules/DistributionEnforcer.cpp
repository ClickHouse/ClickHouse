#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/ShuffleExchangeStep.h>
#include <Processors/QueryPlan/BroadcastExchangeStep.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Processors/QueryPlan/ScatterExchangeStep.h>
#include <Common/Exception.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Produces self-referential enforcer expressions that bridge distribution gaps.
/// Each enforcer expression lives in the same group as the source expression; its
/// single input points back to the same group with normalized properties (node_count,
/// is_replicated, and sorting where needed) as the requirement — this lets the optimizer
/// pick the cheapest source with that distribution shape.  The optimizer recursively
/// satisfies this self-referential input through the normal task mechanism, enabling
/// natural enforcer composition (e.g. Sort + Gather compose into Strategy A without
/// bundling steps).
class DistributionEnforcer : public IOptimizationRule
{
public:
    String getName() const override { return "DistributionEnforcer"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 1000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool DistributionEnforcer::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return !ExpressionProperties::isDistributionSatisfiedBy(required_properties.distribution, expression->properties.distribution);
}

std::vector<GroupExpressionPtr> DistributionEnforcer::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto & input_header = expression->getQueryPlanStep()->getOutputHeader();
    std::vector<GroupExpressionPtr> result;

    if (required_properties.distribution.columns.empty())
    {
        if (required_properties.distribution.is_replicated)
        {
            /// BroadcastExchangeStep only supports single-source input (1->N).
            /// The input always requires {1 node}; the optimizer will recursively
            /// create a GatherExchange to satisfy this when the source has multiple nodes.
            ExpressionProperties input_required;
            input_required.distribution.node_count = 1;

            auto enforcer_expr = std::make_shared<GroupExpression>(
                std::make_unique<BroadcastExchangeStep>(
                    input_header,
                    required_properties.distribution.node_count));
            enforcer_expr->group_id = expression->group_id;
            enforcer_expr->inputs.push_back({.group_id = expression->group_id, .required_properties = input_required});
            enforcer_expr->properties.distribution = required_properties.distribution;

            enforcer_expr->setApplied(*this, required_properties);
            memo.getGroup(expression->group_id)->addPhysicalExpression(enforcer_expr);
            result.push_back(enforcer_expr);
        }
        else if (required_properties.distribution.node_count == 1
                 && expression->properties.distribution.node_count > 1
                 && !expression->properties.distribution.is_replicated)
        {
            /// Regular gather: N nodes -> 1 node, sorting NOT preserved.
            {
                ExpressionProperties input_required;
                input_required.distribution.node_count = expression->properties.distribution.node_count;
                input_required.distribution.is_replicated = expression->properties.distribution.is_replicated;

                auto enforcer_expr = std::make_shared<GroupExpression>(
                    std::make_unique<GatherExchangeStep>(
                        input_header,
                        expression->properties.distribution.node_count));
                enforcer_expr->group_id = expression->group_id;
                enforcer_expr->inputs.push_back({.group_id = expression->group_id, .required_properties = input_required});
                enforcer_expr->properties.distribution = required_properties.distribution;
                /// Sorting is destroyed by a regular gather.

                enforcer_expr->setApplied(*this, required_properties);
                memo.getGroup(expression->group_id)->addPhysicalExpression(enforcer_expr);
                result.push_back(enforcer_expr);
            }

            /// Sorted-merge gather: N nodes -> 1 node, sorting PRESERVED.
            /// Only produced when the source expression already has sorting, so that
            /// the composition SortOnEachNode -> SortedGather yields Strategy B.
            if (!expression->properties.sorting.empty())
            {
                ExpressionProperties input_required;
                input_required.distribution.node_count = expression->properties.distribution.node_count;
                input_required.distribution.is_replicated = expression->properties.distribution.is_replicated;
                input_required.sorting = expression->properties.sorting;
                input_required.sort_limit = expression->properties.sort_limit;

                auto enforcer_expr = std::make_shared<GroupExpression>(
                    std::make_unique<GatherExchangeStep>(
                        input_header,
                        expression->properties.distribution.node_count,
                        expression->properties.sorting));
                enforcer_expr->group_id = expression->group_id;
                enforcer_expr->inputs.push_back({.group_id = expression->group_id, .required_properties = input_required});
                enforcer_expr->properties.distribution = required_properties.distribution;
                enforcer_expr->properties.sorting = expression->properties.sorting;

                enforcer_expr->setApplied(*this, required_properties);
                memo.getGroup(expression->group_id)->addPhysicalExpression(enforcer_expr);
                result.push_back(enforcer_expr);
            }
        }
    }
    else
    {
        if (required_properties.distribution.is_replicated)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot enforce replicated distribution with specific columns");

        Names shuffle_columns;
        for (const auto & distribution_column : required_properties.distribution.columns)
        {
            /// TODO: take the column that is present in the expression and is equivalent to required distribution column
            shuffle_columns.push_back(*distribution_column.begin());
        }

        QueryPlanStepPtr exchange_step =
            (expression->properties.distribution.node_count == 1)
            ? QueryPlanStepPtr(std::make_unique<ScatterExchangeStep>(
                input_header,
                std::move(shuffle_columns),
                required_properties.distribution.node_count))
            : QueryPlanStepPtr(std::make_unique<ShuffleExchangeStep>(
                input_header,
                std::move(shuffle_columns),
                expression->properties.distribution.node_count,
                required_properties.distribution.node_count));

        ExpressionProperties input_required;
        input_required.distribution.node_count = expression->properties.distribution.node_count;
        input_required.distribution.is_replicated = expression->properties.distribution.is_replicated;

        auto enforcer_expr = std::make_shared<GroupExpression>(std::move(exchange_step));
        enforcer_expr->group_id = expression->group_id;
        enforcer_expr->inputs.push_back({.group_id = expression->group_id, .required_properties = input_required});
        enforcer_expr->properties.distribution = required_properties.distribution;
        /// Shuffle/scatter destroys sorting.

        enforcer_expr->setApplied(*this, required_properties);
        memo.getGroup(expression->group_id)->addPhysicalExpression(enforcer_expr);
        result.push_back(enforcer_expr);
    }

    return result;
}

OptimizationRulePtr createDistributionEnforcer() { return std::make_shared<DistributionEnforcer>(); }

}
