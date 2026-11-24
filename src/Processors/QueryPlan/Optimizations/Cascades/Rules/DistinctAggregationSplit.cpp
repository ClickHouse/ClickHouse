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
#include <algorithm>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Adds another way to compute a `uniqExact(x)` without `GROUP BY` (so also a `count(DISTINCT x)`):
/// the count of the rows of an aggregation by `x`. That aggregation also computes a state of each
/// other aggregate of the step for each value of `x`, and the step above it merges these states.
///
///     Aggregating(no keys, uniqExact(x), sum(y))   =>   Aggregating(no keys, count(x), sumMerge(s))
///                         |                                               |
///                       input                           Aggregating(keys = x, sumState(y) AS s)
///                                                                         |
///                                                                       input
///
/// The other aggregation rules plan the two new steps. The plans of the original step stay in the
/// group, and the costs select among all of them.
///
/// For a `Nullable` argument the aggregation by `x` has a row for NULL. `count(x)` does not count it,
/// as `uniqExact` does not count NULL. The states of this row are merged as those of all other rows.
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

/// The other aggregates that the aggregation by the distinct values can compute as states: `count`,
/// `sum`, `min`, `max`, `avg` and `sumCount` (the analyzer makes it from a `sum` and an `avg` of one
/// argument), also with `-If`. The merge of their states is the state of all the rows, however the
/// rows are grouped.
static bool canAggregateAsStates(const IAggregateFunction & function)
{
    String name = function.getName();
    if (name.ends_with("If"))
        name.resize(name.size() - 2);
    if (name != "count" && name != "sum" && name != "min" && name != "max" && name != "avg" && name != "sumCount")
        return false;

    /// A state of a fixed size that owns no other memory (so not the `min` of a `String`): the
    /// estimate of the row width takes this size for a column of states.
    if (!function.hasTrivialDestructor() || function.allocatesMemoryInArena())
        return false;

    /// With a condition that is only NULL (`sumIf(y, NULL)`) the factory gives a `-State` form that
    /// returns no state.
    return std::ranges::none_of(function.getArgumentTypes(), [](const DataTypePtr & type) { return type->onlyNull(); });
}

/// The `uniqExact` of `step` that the rule computes in another way, or nullptr if the rule does not apply to `step`.
static const AggregateDescription * findDistinctAggregateToSplit(const AggregatingStep & step, const Memo & memo)
{
    /// On one node the original step gives the same result with less work.
    if (!memo.getContext().cascades_distinct_aggregation_split || memo.getContext().cluster_node_count <= 1)
        return nullptr;

    /// A final aggregation of rows without keys, with no contract on the order of its work or of its result.
    const auto & params = step.getParams();
    if (!step.isFinal() || !params.keys.empty() || step.isGroupingSets() || step.inOrder() || step.isMergingSkipped()
        || params.only_merge || params.overflow_row || step.explicitSortingRequired() || step.hasUniqExactInOrder()
        || step.shouldProduceResultsInBucketOrder())
        return nullptr;

    /// The limit would apply to the distinct values, which `uniqExact` does not limit.
    if (params.max_rows_to_group_by != 0)
        return nullptr;

    const AggregateDescription * distinct_aggregate = nullptr;
    for (const auto & aggregate : params.aggregates)
    {
        const bool is_uniq_exact_of_one_argument = aggregate.function->getName() == "uniqExact"
            && aggregate.parameters.empty() && aggregate.argument_names.size() == 1;
        if (is_uniq_exact_of_one_argument && !distinct_aggregate)
            distinct_aggregate = &aggregate;
        else if (!canAggregateAsStates(*aggregate.function))
            return nullptr;
    }
    if (!distinct_aggregate)
        return nullptr;

    /// The aggregation by the argument has the states of the other aggregates under their column names.
    const auto & argument = distinct_aggregate->argument_names.front();
    for (const auto & aggregate : params.aggregates)
        if (aggregate.column_name == argument)
            return nullptr;

    const auto & argument_type = step.getInputHeaders().front()->getByName(argument).type;
    return distinctValuesAreAggregationKeys(argument_type) ? distinct_aggregate : nullptr;
}

/// `aggregate` with a combinator added to its function, over the columns `argument_names` of `header`.
static AggregateDescription withCombinator(
    const AggregateDescription & aggregate, const String & combinator, const Block & header, const Names & argument_names)
{
    DataTypes argument_types;
    for (const auto & argument_name : argument_names)
        argument_types.push_back(header.getByName(argument_name).type);

    AggregateDescription result;
    AggregateFunctionProperties properties;
    result.function = AggregateFunctionFactory::instance().get(
        aggregate.function->getName() + combinator, NullsAction::EMPTY, argument_types, aggregate.parameters, properties);
    result.parameters = aggregate.parameters;
    result.argument_names = argument_names;
    result.column_name = aggregate.column_name;
    return result;
}

/// The count of the rows of the aggregation by `argument`, under the column name of `distinct_aggregate`.
/// For a `Nullable` argument it is `count(argument)`, so that the NULL row is not counted.
static AggregateDescription makeCountOfDistinctValues(
    const AggregateDescription & distinct_aggregate, const String & argument, const DataTypePtr & argument_type)
{
    const bool has_null_row = removeLowCardinality(argument_type)->isNullable();
    AggregateDescription count;
    AggregateFunctionProperties properties;
    count.function = AggregateFunctionFactory::instance().get(
        "count", NullsAction::EMPTY, has_null_row ? DataTypes{argument_type} : DataTypes{}, Array{}, properties);
    if (has_null_row)
        count.argument_names = {argument};
    count.column_name = distinct_aggregate.column_name;
    return count;
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
    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    return agg_step && expression->strategy == nullptr && expression->inputs.size() == 1
        && findDistinctAggregateToSplit(*agg_step, memo) != nullptr;
}

GroupExpressions DistinctAggregationSplit::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    const auto & agg_step = *typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    const auto & aggregates = agg_step.getParams().aggregates;
    const auto & distinct_aggregate = *findDistinctAggregateToSplit(agg_step, memo);
    const auto & argument = distinct_aggregate.argument_names.front();
    const auto & input_header = agg_step.getInputHeaders().front();

    AggregateDescriptions states;
    for (const auto & aggregate : aggregates)
        if (&aggregate != &distinct_aggregate)
            states.push_back(withCombinator(aggregate, "State", *input_header, aggregate.argument_names));

    auto distinct_values_step = makeAggregation(agg_step, input_header, {argument}, states);
    distinct_values_step->setStepDescription("Distinct values");
    const auto & distinct_values_header = distinct_values_step->getOutputHeader();

    /// The aggregates of the source step in their order, each under its column name.
    AggregateDescriptions aggregates_of_distinct_values;
    for (const auto & aggregate : aggregates)
    {
        if (&aggregate == &distinct_aggregate)
            aggregates_of_distinct_values.push_back(
                makeCountOfDistinctValues(distinct_aggregate, argument, distinct_values_header->getByName(argument).type));
        else
            aggregates_of_distinct_values.push_back(withCombinator(aggregate, "Merge", *distinct_values_header, {aggregate.column_name}));
    }

    auto count_step = makeAggregation(agg_step, distinct_values_header, {}, aggregates_of_distinct_values);
    count_step->setStepDescription("Count of distinct values");

    if (!blocksHaveEqualStructure(*count_step->getOutputHeader(), *agg_step.getOutputHeader()))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "The aggregation over the distinct values has the columns '{}', and the aggregation that it computes has '{}'",
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
