#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/Block.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Adds another way to compute a `uniqExact(x)` without `GROUP BY` (so also a `count(DISTINCT x)`):
/// the count of the rows of an aggregation by `x`.
///
///     Aggregating(no keys, uniqExact(x))   =>   Aggregating(no keys, count(x))
///                     |                                       |
///                   input                           Aggregating(keys = x)
///                                                             |
///                                                           input
///
/// The other aggregation rules plan the two new steps. The plans of the original step stay in the
/// group, and the costs select among all of them.
///
/// For a `Nullable` argument the aggregation by `x` has a row for NULL. `count(x)` does not count it,
/// as `uniqExact` does not count NULL.
class DistinctAggregationSplit : public IOptimizationRule
{
public:
    [[nodiscard]] String getName() const override { return "DistinctAggregationSplit"; }
    [[nodiscard]] bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    [[nodiscard]] Promise getPromise() const override { return 2500; }
    [[nodiscard]] bool isTransformation() const override { return true; }

protected:
    GroupExpressions applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

/// The types for which `uniqExact` keeps the value itself, so it finds the same distinct values as an
/// aggregation by the value. For strings, decimals and other wide types it keeps a 128-bit hash: two
/// values with one hash would be one value for it and two groups for the aggregation.
/// Floats are not here: an aggregation in the order of its key takes `0.` and `-0.` as one value.
static bool distinctValuesAreAggregationKeys(const DataTypePtr & type)
{
    const WhichDataType which(removeNullable(removeLowCardinality(type)));
    return which.isNativeInteger() || which.isInt128() || which.isUInt128() || which.isInt256() || which.isUInt256()
        || which.isDate() || which.isDate32() || which.isDateTime()
        || which.isEnum() || which.isUUID() || which.isIPv4();
}

/// A final aggregation over `input_header` with the settings of `source_step`. It has no sort
/// descriptions and no bucket order: the rule does not apply to a step that has them.
static std::unique_ptr<AggregatingStep> makeAggregation(
    const AggregatingStep & source_step, const SharedHeader & input_header, const Names & keys, const AggregateDescriptions & aggregates)
{
    auto params = source_step.getParams().cloneWithKeysAndAggregates(keys, aggregates);
    /// The key of the source step is for the size of its own hash table.
    params.stats_collecting_params.setKey(0);

    return std::make_unique<AggregatingStep>(
        input_header,
        std::move(params),
        GroupingSetsParamsList{},
        /*final_=*/true,
        source_step.getMaxBlockSize(),
        source_step.getMaxBlockSizeForAggregationInOrder(),
        source_step.getMergeThreads(),
        source_step.getTemporaryDataMergeThreads(),
        /*storage_has_evenly_distributed_read_=*/false,
        source_step.isGroupByUseNulls(),
        SortDescription{},
        SortDescription{},
        /*should_produce_results_in_order_of_bucket_number_=*/false,
        source_step.usingMemoryBoundMerging(),
        /*explicit_sorting_required_for_aggregation_in_order_=*/false);
}

bool DistinctAggregationSplit::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & memo) const
{
    /// On one node the original step gives the same result with less work.
    if (memo.getContext().cluster_node_count <= 1)
        return false;

    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    if (!agg_step || expression->strategy != nullptr || expression->inputs.size() != 1)
        return false;

    /// `canUseRadixUniqExact` is true for a final single `uniqExact` of one argument without keys.
    if (!agg_step->canUseRadixUniqExact() || agg_step->explicitSortingRequired() || agg_step->hasUniqExactInOrder()
        || agg_step->shouldProduceResultsInBucketOrder())
        return false;

    /// The limit would apply to the distinct values, which `uniqExact` does not limit.
    if (agg_step->getParams().max_rows_to_group_by != 0)
        return false;

    const auto & argument = agg_step->getParams().aggregates.front().argument_names.front();
    return distinctValuesAreAggregationKeys(agg_step->getInputHeaders().front()->getByName(argument).type);
}

GroupExpressions DistinctAggregationSplit::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    const auto & agg_step = *typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    const auto & distinct_aggregate = agg_step.getParams().aggregates.front();
    const auto & argument = distinct_aggregate.argument_names.front();

    auto distinct_values_step = makeAggregation(agg_step, agg_step.getInputHeaders().front(), {argument}, {});
    distinct_values_step->setStepDescription("Distinct values");

    /// `count(x)` for a `Nullable` argument, so that the NULL row is not counted.
    const auto & values_type = distinct_values_step->getOutputHeader()->getByName(argument).type;
    const bool has_null_row = removeLowCardinality(values_type)->isNullable();
    AggregateDescription count;
    AggregateFunctionProperties properties;
    count.function = AggregateFunctionFactory::instance().get(
        "count", NullsAction::EMPTY, has_null_row ? DataTypes{values_type} : DataTypes{}, Array{}, properties);
    if (has_null_row)
        count.argument_names = {argument};
    count.column_name = distinct_aggregate.column_name;

    auto count_step = makeAggregation(agg_step, distinct_values_step->getOutputHeader(), {}, {count});
    count_step->setStepDescription("Count of distinct values");

    /// Both steps give one `UInt64` column with the name of the source aggregate.
    if (!blocksHaveEqualStructure(*count_step->getOutputHeader(), *agg_step.getOutputHeader()))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "The count of distinct values has the columns '{}', and the aggregation that it computes has '{}'",
            count_step->getOutputHeader()->dumpStructure(), agg_step.getOutputHeader()->dumpStructure());

    /// The aggregation by the argument goes to a new group, the count to the group of the original step.
    auto distinct_values_expression = std::make_shared<GroupExpression>(std::move(distinct_values_step));
    auto count_expression = addTwoStageSplit(memo, expression, std::move(distinct_values_expression), std::move(count_step), {});
    if (!count_expression)
        return {};
    return {count_expression};
}

OptimizationRulePtr createDistinctAggregationSplit() { return std::make_shared<DistinctAggregationSplit>(); }

}
