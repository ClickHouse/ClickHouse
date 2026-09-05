#include <AggregateFunctions/IAggregateFunction.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// The per-bucket selection scans every cell anyway; past this many candidate rows per bucket
/// the saved materialization no longer pays for the selection scan.
constexpr size_t max_bucket_top_k = 65536;

/// Traces an output column of a pass-through expression to the input column it forwards:
/// aliases are unwrapped, anything computed disqualifies.
const String * traceThroughExpression(const ExpressionStep & expression, const String & name)
{
    const auto * node = expression.getExpression().tryFindInOutputs(name);
    if (!node)
        return nullptr;
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    if (node->type != ActionsDAG::ActionType::INPUT)
        return nullptr;
    return &node->result_name;
}

}

size_t tryPushBucketTopKIntoAggregation(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings &)
{
    /// The shape: Limit over Sorting (with a pushed-down limit, by one plain column) over zero
    /// or more pass-through expressions over a final aggregation, and the sort column is the
    /// aggregation's lone-`count()` output. `HAVING`, `WITH TOTALS`, `LIMIT BY` and windows sit
    /// between the aggregation and the sorting as their own steps and break the adjacency.
    const auto * limit = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit || parent_node->children.size() != 1)
        return 0;

    /// The per-bucket selection permanently drops the other groups, so it cannot serve
    /// `LIMIT WITH TIES` (the output size is not known in advance) or an exact
    /// `rows_before_limit_at_least` (`alwaysReadTillEnd`): the counter runs downstream of the
    /// conversion and would report the kept rows instead of the full group count.
    if (limit->withTies() || limit->alwaysReadTillEnd())
        return 0;

    QueryPlan::Node * sorting_node = parent_node->children.front();
    const auto * sorting = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting || sorting_node->children.size() != 1)
        return 0;

    const size_t n = sorting->getLimit();
    if (n == 0 || n > max_bucket_top_k)
        return 0;

    const auto & description = sorting->getSortDescription();
    if (description.size() != 1 || description.front().with_fill)
        return 0;

    String column = description.front().column_name;
    const bool ascending = description.front().direction > 0;

    QueryPlan::Node * node = sorting_node->children.front();
    while (const auto * expression = typeid_cast<ExpressionStep *>(node->step.get()))
    {
        /// An `arrayJoin` between the aggregation and the sort changes row multiplicity per group,
        /// and over an empty array it produces no row at all. The best n groups of a bucket are then
        /// no longer a superset of the rows the limit needs, and the groups pruned inside the bucket
        /// can be precisely the ones that would have survived. The sibling gates in
        /// `optimizeGroupByTopK` and in the planner reject the same shape.
        if (expression->getExpression().hasArrayJoin())
            return 0;

        const String * traced = traceThroughExpression(*expression, column);
        if (!traced || node->children.size() != 1)
            return 0;
        column = *traced;
        node = node->children.front();
    }

    auto * aggregating = typeid_cast<AggregatingStep *>(node->step.get());
    if (!aggregating)
        return 0;

    const auto & params = aggregating->getParams();
    if (!aggregating->isFinal() || aggregating->isGroupingSets() || params.overflow_row || params.keys_size == 0)
        return 0;

    /// The selection reads the count straight from the aggregate state and handles no
    /// null-key cell, so nullable and low-cardinality keys stay on the ordinary conversion.
    const auto & header = node->step->getOutputHeader();
    for (const auto & key : params.keys)
    {
        const auto & type = header->getByName(key).type;
        if (type->isNullable() || type->isLowCardinalityNullable() || typeid_cast<const DataTypeLowCardinality *>(type.get()))
            return 0;
    }

    for (size_t i = 0; i < params.aggregates.size(); ++i)
    {
        const auto & aggregate = params.aggregates[i];
        if (aggregate.column_name != column)
            continue;
        if (aggregate.function->getName() != "count" || !aggregate.argument_names.empty() || !aggregate.parameters.empty())
            return 0;

        aggregating->enableBucketTopK(n, ascending, i);
        return 0;
    }

    return 0;
}

}
