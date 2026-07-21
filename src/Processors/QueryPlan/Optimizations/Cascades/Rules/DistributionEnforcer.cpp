#include <DataTypes/DataTypeFactory.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Common/logger_useful.h>
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
/// is_replicated, and sorting where needed) as the requirement - this lets the optimizer
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

    class EnforcerEnumerator;

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

/// Emits the exchange enforcers that can bridge one distribution gap, one method per
/// exchange kind. addEnforcer centralizes the construction of the self-referential
/// enforcer expression: it lives in the source group, its single input points back to
/// the same group with a relaxed requirement, the output distribution is the required
/// one, and the expression carries the Distribution enforcer axis that the
/// cycle-avoidance rules key on.
class DistributionEnforcer::EnforcerEnumerator
{
public:
    EnforcerEnumerator(
        const DistributionEnforcer & rule_,
        GroupExpressionPtr expression_,
        const ExpressionProperties & required_properties_,
        Memo & memo_,
        std::vector<GroupExpressionPtr> & result_)
        : rule(rule_)
        , expression(std::move(expression_))
        , input_header(expression->getQueryPlanStep()->getOutputHeader())
        , required_properties(required_properties_)
        , memo(memo_)
        , result(result_)
    {
    }

    void addBroadcast();
    void addRoundRobinScatter();
    void addGather();
    void addSortedGather();
    void addKeyedShuffle();

private:
    void addEnforcer(QueryPlanStepPtr step, ExpressionProperties input_required, SortDescription output_sorting = {});

    const DistributionEnforcer & rule;
    GroupExpressionPtr expression;
    const SharedHeader & input_header;
    const ExpressionProperties & required_properties;
    Memo & memo;
    std::vector<GroupExpressionPtr> & result;
};

bool DistributionEnforcer::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return !ExpressionProperties::isDistributionSatisfiedBy(required_properties.distribution, expression->properties.distribution);
}

void DistributionEnforcer::EnforcerEnumerator::addEnforcer(QueryPlanStepPtr step, ExpressionProperties input_required, SortDescription output_sorting)
{
    ExpressionProperties output_properties;
    output_properties.distribution = required_properties.distribution;
    output_properties.sorting = std::move(output_sorting);
    auto enforcer_expr = makeEnforcerExpression(
        expression, std::move(step), std::move(input_required), std::move(output_properties), EnforcerAxis::Distribution);

    rule.addPhysicalToMemo(enforcer_expr, required_properties, memo, result);
}

/// BroadcastExchangeStep only supports single-source input (1->N).
/// The input always requires {1 node}; the optimizer will recursively
/// create a GatherExchange to satisfy this when the source has multiple nodes.
void DistributionEnforcer::EnforcerEnumerator::addBroadcast()
{
    ExpressionProperties input_required;
    input_required.distribution.node_count = 1;

    addEnforcer(
        std::make_unique<BroadcastExchangeStep>(input_header, required_properties.distribution.node_count),
        std::move(input_required));
}

/// Column-less scatter: rows go round-robin, any node may get any row. Like the
/// broadcast, the input always requires {1 node}; a multi-node source composes
/// through a gather.
void DistributionEnforcer::EnforcerEnumerator::addRoundRobinScatter()
{
    ExpressionProperties input_required;
    input_required.distribution.node_count = 1;

    addEnforcer(
        std::make_unique<ScatterExchangeStep>(input_header, Names{}, required_properties.distribution.node_count),
        std::move(input_required));
}

/// Regular gather: N nodes -> 1 node, sorting NOT preserved.
void DistributionEnforcer::EnforcerEnumerator::addGather()
{
    ExpressionProperties input_required;
    input_required.distribution.node_count = expression->properties.distribution.node_count;
    input_required.distribution.is_replicated = expression->properties.distribution.is_replicated;

    /// Sorting is destroyed by a regular gather.
    addEnforcer(
        std::make_unique<GatherExchangeStep>(input_header, expression->properties.distribution.node_count),
        std::move(input_required));
}

/// Sorted-merge gather: N nodes -> 1 node, sorting PRESERVED.
/// Only produced when the source expression already has sorting, so that
/// the composition SortOnEachNode -> SortedGather yields Strategy B.
void DistributionEnforcer::EnforcerEnumerator::addSortedGather()
{
    ExpressionProperties input_required;
    input_required.distribution.node_count = expression->properties.distribution.node_count;
    input_required.distribution.is_replicated = expression->properties.distribution.is_replicated;
    input_required.sorting = expression->properties.sorting;

    addEnforcer(
        std::make_unique<GatherExchangeStep>(
            input_header,
            expression->properties.distribution.node_count,
            expression->properties.sorting),
        std::move(input_required),
        expression->properties.sorting);
}

/// Keyed shuffle: repartition by the required columns, as a 1->N scatter when the source
/// is on one node and an N->M shuffle otherwise.
void DistributionEnforcer::EnforcerEnumerator::addKeyedShuffle()
{
    if (required_properties.distribution.is_replicated)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot enforce replicated distribution with specific columns");

    /// Shuffling a replicated source hashes every node's full copy, multiplying the output by
    /// the replica count. Do not emit this invalid plan; the keyed requirement is satisfied
    /// from a non-replicated implementation of the same group instead.
    if (expression->properties.distribution.is_replicated && expression->properties.distribution.node_count > 1)
    {
        LOG_TEST(getLogger("DistributionEnforcer"), "No shuffle from replicated '{}': it would multiply rows by the replica count",
            expression->getName());
        return;
    }

    Names shuffle_columns;
    for (const auto & distribution_column : required_properties.distribution.columns)
    {
        /// Pick the equivalent name that is present in the input header: the shuffle step
        /// resolves the column by name, so a dropped equivalent would fail at execution.
        String chosen_column;
        for (const auto & equivalent_name : distribution_column)
        {
            if (input_header->has(equivalent_name))
            {
                chosen_column = equivalent_name;
                break;
            }
        }
        /// None of the equivalents survive in the input: this shuffle cannot be built.
        if (chosen_column.empty())
        {
            LOG_TEST(getLogger("DistributionEnforcer"), "No shuffle for '{}': no equivalent of a required distribution column is in the input header",
                expression->getName());
            return;
        }
        shuffle_columns.push_back(chosen_column);
    }

    /// Cast keys before hashing when the requirement pins hash types (mismatched
    /// join key types), so both sides of the join hash into the same buckets.
    DataTypes hash_cast_types;
    if (!required_properties.distribution.hash_type_names.empty())
    {
        const auto & type_factory = DataTypeFactory::instance();
        for (const auto & type_name : required_properties.distribution.hash_type_names)
            hash_cast_types.push_back(type_factory.get(type_name));
    }

    QueryPlanStepPtr exchange_step =
        (expression->properties.distribution.node_count == 1)
        ? QueryPlanStepPtr(std::make_unique<ScatterExchangeStep>(
            input_header,
            std::move(shuffle_columns),
            required_properties.distribution.node_count,
            std::move(hash_cast_types)))
        : QueryPlanStepPtr(std::make_unique<ShuffleExchangeStep>(
            input_header,
            std::move(shuffle_columns),
            expression->properties.distribution.node_count,
            required_properties.distribution.node_count,
            std::move(hash_cast_types)));

    ExpressionProperties input_required;
    input_required.distribution.node_count = expression->properties.distribution.node_count;
    input_required.distribution.is_replicated = expression->properties.distribution.is_replicated;

    /// Shuffle/scatter destroys sorting.
    addEnforcer(std::move(exchange_step), std::move(input_required));
}

std::vector<GroupExpressionPtr> DistributionEnforcer::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    std::vector<GroupExpressionPtr> result;
    EnforcerEnumerator enforcers(*this, expression, required_properties, memo, result);

    if (required_properties.distribution.columns.empty())
    {
        if (required_properties.distribution.is_replicated)
        {
            enforcers.addBroadcast();
        }
        else if (required_properties.distribution.node_count > 1)
        {
            enforcers.addRoundRobinScatter();
        }
        else if (required_properties.distribution.node_count == 1
                 && expression->properties.distribution.node_count > 1
                 && !expression->properties.distribution.is_replicated)
        {
            enforcers.addGather();

            if (!expression->properties.sorting.empty())
                enforcers.addSortedGather();
        }
    }
    else
    {
        enforcers.addKeyedShuffle();
    }

    return result;
}

OptimizationRulePtr createDistributionEnforcer();
OptimizationRulePtr createDistributionEnforcer() { return std::make_shared<DistributionEnforcer>(); }

}
