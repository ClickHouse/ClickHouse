#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsUtils.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/SaveSubqueryResultToBufferStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <fmt/format.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace
{

ColumnStatsProvenance provenance(ColumnStatsOrigin origin, UInt16 transformations = 0)
{
    return ColumnStatsProvenance{.origin = origin, .transformations = transformations};
}

SharedHeader makeHeader(const String & column_name = "k")
{
    const auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, column_name)}));
}

ColumnStats measuredColumnStats()
{
    return ColumnStats{
        .num_distinct_values = 50,
        .ndv_provenance = provenance(ColumnStatsOrigin::MeasuredStatistics),
        .avg_bytes = 8,
        .min_value = Field(UInt64{10}),
        .max_value = Field(UInt64{59}),
        .null_fraction = 0.0,
        .range_provenance = provenance(ColumnStatsOrigin::MeasuredStatistics)};
}

RelationStats inputRelationStats(bool rows_exact = true)
{
    RelationStats result;
    result.estimated_rows = 100;
    result.column_stats.emplace("k", measuredColumnStats());
    result.rows_exact = rows_exact;
    return result;
}

ActionsDAG makeFilterDAG(const SharedHeader & header)
{
    ActionsDAG dag(header->getColumnsWithTypeAndName());
    const auto filter_type = std::make_shared<DataTypeUInt8>();
    const auto & predicate
        = dag.addColumn(filter_type->createColumnConstWithDefaultValue(1), filter_type, "__filter");
    dag.addOrReplaceInOutputs(predicate);
    return dag;
}

SortDescription sortByKey()
{
    SortDescription result;
    result.emplace_back("k");
    return result;
}

std::unique_ptr<AggregatingStep>
makeAggregatingStep(const SharedHeader & header, Aggregator::Params params, GroupingSetsParamsList grouping_sets_params = {})
{
    return std::make_unique<AggregatingStep>(
        header,
        std::move(params),
        std::move(grouping_sets_params),
        /*final_=*/true,
        /*max_block_size_=*/65536,
        /*aggregation_in_order_max_block_bytes_=*/0,
        /*merge_threads_=*/1,
        /*temporary_data_merge_threads_=*/1,
        /*storage_has_evenly_distributed_read_=*/false,
        /*group_by_use_nulls_=*/false,
        /*sort_description_for_merging_=*/SortDescription{},
        /*group_by_sort_description_=*/SortDescription{},
        /*should_produce_results_in_order_of_bucket_number_=*/false,
        /*memory_bound_merging_of_aggregation_results_enabled_=*/false,
        /*explicit_sorting_required_for_aggregation_in_order_=*/false);
}

std::unique_ptr<AggregatingStep> makeAggregationStep(
    const SharedHeader & header,
    bool grouping_sets,
    bool overflow_row)
{
    Aggregator::Params params(
        Names{"k"},
        AggregateDescriptions{},
        overflow_row,
        /*max_threads_=*/1,
        /*max_block_size_=*/65536,
        /*min_hit_rate_to_use_consecutive_keys_optimization_=*/0.5f,
        /*serialize_string_with_zero_byte_=*/false,
        /*enable_packed_string_keys_=*/true);

    GroupingSetsParamsList grouping_sets_params;
    if (grouping_sets)
        grouping_sets_params.emplace_back(Names{"k"}, Names{});

    return makeAggregatingStep(header, std::move(params), std::move(grouping_sets_params));
}

std::unique_ptr<AggregatingStep> makeAggregationStepWithGroupLimit(
    const SharedHeader & header,
    size_t max_rows_to_group_by,
    OverflowMode overflow_mode)
{
    Aggregator::Params params(
        Names{"k"},
        AggregateDescriptions{},
        /*overflow_row_=*/false,
        max_rows_to_group_by,
        overflow_mode,
        /*group_by_two_level_threshold_=*/0,
        /*group_by_two_level_threshold_bytes_=*/0,
        /*max_bytes_before_external_group_by_=*/0,
        /*min_bytes_before_external_group_by_=*/0,
        /*empty_result_for_aggregation_by_empty_set_=*/false,
        /*tmp_data_scope_=*/{},
        /*max_threads_=*/1,
        /*min_free_disk_space_=*/0,
        /*compile_aggregate_expressions_=*/false,
        /*min_count_to_compile_aggregate_expression_=*/0,
        /*max_block_size_=*/65536,
        /*enable_prefetch_=*/false,
        /*only_merge_=*/false,
        /*optimize_group_by_constant_keys_=*/false,
        /*min_hit_rate_to_use_consecutive_keys_optimization_=*/0.5f,
        StatsCollectingParams{},
        /*enable_producing_buckets_out_of_order_in_aggregation_=*/true,
        /*serialize_string_with_zero_byte_=*/false,
        /*enable_parallel_single_level_merge_=*/false,
        /*enable_packed_string_keys_=*/true,
        /*enable_adaptive_aggregator_=*/false,
        /*adaptive_aggregator_freeze_threshold_=*/0,
        /*adaptive_aggregator_freeze_threshold_bytes_=*/0,
        /*adaptive_aggregator_disable_thaw_=*/false,
        /*use_aggregation_memory_tracker_=*/true);

    return makeAggregatingStep(header, std::move(params));
}

JoinExpressionActions makeJoinExpressionActions(const Block & left_header, const Block & right_header)
{
    JoinExpressionActions expression_actions(left_header, right_header);
    auto actions_dag = expression_actions.getActionsDAG();
    for (const auto * input : actions_dag->getInputs())
        actions_dag->getOutputs().push_back(input);
    return expression_actions;
}

std::unique_ptr<JoinStepLogical> makeLogicalJoinStep(JoinKind kind, JoinStrictness strictness)
{
    auto left_header = makeHeader("l");
    auto right_header = makeHeader("r");
    QueryPlanSerializationSettings settings;
    return std::make_unique<JoinStepLogical>(
        left_header,
        right_header,
        JoinOperator(kind, strictness),
        makeJoinExpressionActions(*left_header, *right_header),
        ActionsDAG::NodeRawConstPtrs{},
        JoinSettings(settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION),
        SortingStep::Settings(settings));
}

std::unique_ptr<JoinStepLogical>
makeOptimizedJoinStep(JoinKind kind, JoinStrictness strictness, String stats_column, UInt64 estimated_rows = 100)
{
    auto step = makeLogicalJoinStep(kind, strictness);
    step->setOptimized(estimated_rows, {{std::move(stats_column), measuredColumnStats()}});
    return step;
}

/// Stands in for `ReadFromSystemOne`, which lives in a module the tests do not link and which the estimator
/// therefore recognizes by its name.
class TestSystemOneStep final : public ISourceStep
{
public:
    explicit TestSystemOneStep(SharedHeader header)
        : ISourceStep(std::move(header))
    {
    }

    String getName() const override { return "ReadFromSystemOne"; }
    QueryPlanStepPtr clone() const override { return std::make_unique<TestSystemOneStep>(getOutputHeader()); }
    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override { }
};

/// A source that counts how often the estimator visits it. The estimator asks every source it does not know by type
/// for its name (to recognize `ReadFromSystemOne`), so each call of `getName` is a visit.
class CountingSourceStep final : public ISourceStep
{
public:
    CountingSourceStep(SharedHeader header, size_t & visits_)
        : ISourceStep(std::move(header))
        , visits(visits_)
    {
    }

    String getName() const override
    {
        ++visits;
        return "CountingSource";
    }
    QueryPlanStepPtr clone() const override { return std::make_unique<CountingSourceStep>(getOutputHeader(), visits); }
    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override { }

private:
    size_t & visits;
};

class PreservingTestStep final : public ITransformingStep
{
public:
    explicit PreservingTestStep(SharedHeader header)
        : ITransformingStep(
            header,
            header,
            Traits{
                {.returns_single_stream = false, .preserves_number_of_streams = true, .preserves_sorting = true},
                {.preserves_number_of_rows = true}})
    {
    }

    String getName() const override { return "PreservingTest"; }
    QueryPlanStepPtr clone() const override { return std::make_unique<PreservingTestStep>(getOutputHeader()); }
    void transformPipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override { }

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }
};

void expectUnsupportedUnaryStepStopsBeforeChild(std::unique_ptr<IQueryPlanStep> step, const SharedHeader & child_header)
{
    size_t child_visits = 0;
    QueryPlan::Node child;
    child.step = std::make_unique<CountingSourceStep>(child_header, child_visits);
    QueryPlan::Node root;
    root.step = std::move(step);
    root.children = {&child};

    const auto result = estimateReadRowsCount(root);
    EXPECT_FALSE(result.estimated_rows.has_value());
    EXPECT_TRUE(result.column_stats.empty());
    EXPECT_EQ(child_visits, 0);
}
}

TEST(ColumnStatsProvenance, StableDiagnosticString)
{
    EXPECT_EQ(provenance(ColumnStatsOrigin::Unknown).toString(), "unknown");
    EXPECT_EQ(provenance(ColumnStatsOrigin::MeasuredStatistics).toString(), "measured-statistics");
    EXPECT_EQ(provenance(ColumnStatsOrigin::SyntheticFallback).toString(), "synthetic-fallback");
    EXPECT_EQ(provenance(ColumnStatsOrigin::SyntheticOverride).toString(), "synthetic-override");

    constexpr UInt16 all_transformations = RowSubset | EstimatedRowCountClamp | NDVBoundExpression | PartialStatisticsCoverage | Unmodeled;
    EXPECT_EQ(
        provenance(ColumnStatsOrigin::MeasuredStatistics, all_transformations).toString(),
        "measured-statistics[row-subset,estimated-row-clamp,ndv-bound-expression,partial-statistics-coverage,unmodeled]");
}

TEST(ColumnStatsProvenance, GuaranteesKeptByEachTransformation)
{
    struct Case
    {
        std::optional<ColumnStatsTransformation> transformation;
        bool exact_distinct_count;
        bool distinct_count_upper_bound;
        bool exact_range;
        bool range_superset;
        bool representative_range;
    };
    /// The table in `ColumnStatsProvenance.h`.
    const std::vector<Case> cases{
        {std::nullopt, true, true, true, true, true},
        {RowSubset, false, true, false, true, false},
        {NDVBoundExpression, false, true, false, false, false},
        {PartialStatisticsCoverage, false, false, false, false, false},
        {Unmodeled, false, false, false, false, false},
    };
    for (const auto & test_case : cases)
    {
        auto stats = measuredColumnStats();
        if (test_case.transformation)
            stats.addTransformation(*test_case.transformation);
        SCOPED_TRACE(stats.ndv_provenance.toString());
        EXPECT_EQ(stats.distinctCount(DistinctCountGuarantee::Exact).has_value(), test_case.exact_distinct_count);
        EXPECT_EQ(stats.distinctCount(DistinctCountGuarantee::UpperBound).has_value(), test_case.distinct_count_upper_bound);
        EXPECT_EQ(stats.valueRange(ValueRangeGuarantee::Exact).has_value(), test_case.exact_range);
        EXPECT_EQ(stats.valueRange(ValueRangeGuarantee::Superset).has_value(), test_case.range_superset);
        EXPECT_EQ(stats.valueRange(ValueRangeGuarantee::Representative).has_value(), test_case.representative_range);
    }

    auto clamped = measuredColumnStats();
    clamped.ndv_provenance.add(EstimatedRowCountClamp);
    EXPECT_FALSE(clamped.distinctCount(DistinctCountGuarantee::Exact).has_value());
    EXPECT_FALSE(clamped.distinctCount(DistinctCountGuarantee::UpperBound).has_value());
    EXPECT_TRUE(clamped.valueRange(ValueRangeGuarantee::Exact).has_value());
}

TEST(ColumnStatsProvenance, GuaranteesRequireTheirOrigins)
{
    for (const auto origin : {ColumnStatsOrigin::Unknown, ColumnStatsOrigin::SyntheticFallback, ColumnStatsOrigin::SyntheticOverride})
    {
        SCOPED_TRACE(provenance(origin).toString());
        auto stats = measuredColumnStats();
        stats.ndv_provenance.origin = origin;
        stats.range_provenance.origin = origin;
        const bool is_trusted_distinct_count = origin == ColumnStatsOrigin::SyntheticOverride;
        EXPECT_EQ(stats.distinctCount(DistinctCountGuarantee::Exact).has_value(), is_trusted_distinct_count);
        EXPECT_EQ(stats.distinctCount(DistinctCountGuarantee::UpperBound).has_value(), is_trusted_distinct_count);
        EXPECT_FALSE(stats.valueRange(ValueRangeGuarantee::Superset).has_value());
    }
}

TEST(ColumnStatsProvenance, ValueRangeComesWithTheNullFractionOfTheSameRows)
{
    auto stats = measuredColumnStats();
    stats.null_fraction = 0.25;
    const auto exact = stats.valueRange(ValueRangeGuarantee::Exact);
    ASSERT_TRUE(exact.has_value());
    EXPECT_EQ(exact->min, Field(UInt64{10}));
    EXPECT_EQ(exact->max, Field(UInt64{59}));
    EXPECT_EQ(exact->null_fraction, 0.25);

    /// Dropping rows can change the NULL fraction arbitrarily.
    stats.addTransformation(RowSubset);
    const auto superset = stats.valueRange(ValueRangeGuarantee::Superset);
    ASSERT_TRUE(superset.has_value());
    EXPECT_FALSE(superset->null_fraction.has_value());
}

TEST(ColumnStatsProvenance, SyntheticPlannerInputsCarryOverrideProvenance)
{
    /// A hint labels every fact it gives that has a provenance: the distinct count of `k` and the ranges of `k` and `r`.
    /// A width has no provenance, and a fact that the hint does not give stays `Unknown`.
    const auto hinted = parseTableStatsHint(
        R"({"t":{"cardinality":100,"distinct_keys":{"k":25},"key_ranges":{"k":[0,99],"r":[5,10]},"column_bytes":{"w":8}}})", "t");
    ASSERT_EQ(hinted.source, RowEstimateSource::Hint);
    ASSERT_EQ(hinted.column_stats.size(), 3);
    const auto & k = hinted.column_stats.at("k");
    EXPECT_EQ(k.ndv_provenance.origin, ColumnStatsOrigin::SyntheticOverride);
    EXPECT_EQ(k.range_provenance.origin, ColumnStatsOrigin::SyntheticOverride);
    EXPECT_EQ(hinted.column_stats.at("r").ndv_provenance.origin, ColumnStatsOrigin::Unknown);
    EXPECT_EQ(hinted.column_stats.at("r").range_provenance.origin, ColumnStatsOrigin::SyntheticOverride);
    EXPECT_EQ(hinted.column_stats.at("w").ndv_provenance.origin, ColumnStatsOrigin::Unknown);
    EXPECT_EQ(hinted.column_stats.at("w").range_provenance.origin, ColumnStatsOrigin::Unknown);

    /// The hinted distinct count is trusted as if measured. The hinted range gives no guarantee, but stays for the
    /// readers that do not consult provenance.
    EXPECT_EQ(k.distinctCount(DistinctCountGuarantee::Exact), std::optional<UInt64>(25));
    EXPECT_FALSE(k.valueRange(ValueRangeGuarantee::Superset).has_value());
    EXPECT_EQ(k.min_value, Field(Float64{0}));
    EXPECT_EQ(k.max_value, Field(Float64{99}));

    const auto randomized = getRandomizedStats(42, 0, "t", *makeHeader());
    ASSERT_EQ(randomized.source, RowEstimateSource::Randomized);
    ASSERT_EQ(randomized.column_stats.size(), 1);
    EXPECT_EQ(randomized.column_stats.at("k").ndv_provenance.origin, ColumnStatsOrigin::SyntheticOverride);
}

TEST(ColumnStatsProvenance, ExpressionLineageRecordsValueChangingFunctionsAndClearsTheirRanges)
{
    tryRegisterFunctions();
    const auto header = makeHeader();
    ActionsDAG dag(header->getColumnsWithTypeAndName());
    const auto & input = *dag.getInputs().front();
    dag.addOrReplaceInOutputs(dag.addAlias(input, "alias"));

    auto materialize = FunctionFactory::instance().get("materialize", getContext().context);
    auto round = FunctionFactory::instance().get("roundToExp2", getContext().context);
    auto to_nullable = FunctionFactory::instance().get("toNullable", getContext().context);
    const auto & preserved = dag.addFunction(materialize, {&input}, "preserved");
    const auto & nullable = dag.addFunction(to_nullable, {&input}, "nullable");
    const auto & casted = dag.addCast(input, std::make_shared<DataTypeUInt32>(), "casted", getContext().context);
    const auto & collapsed = dag.addFunction(round, {&input}, "collapsed");
    const auto & chained = dag.addFunction(materialize, {&collapsed}, "chained");
    dag.addOrReplaceInOutputs(preserved);
    dag.addOrReplaceInOutputs(nullable);
    dag.addOrReplaceInOutputs(casted);
    dag.addOrReplaceInOutputs(collapsed);
    dag.addOrReplaceInOutputs(chained);

    ExpressionStep step(header, std::move(dag));
    auto input_stats = inputRelationStats();
    input_stats.column_stats.at("k").null_fraction = 0.25;
    auto result = estimateUnaryStepStats(step, std::move(input_stats));
    ASSERT_TRUE(result.has_value());
    const auto & stats = result->column_stats;
    EXPECT_TRUE(result->rows_exact);

    /// Value-preserving outputs keep every guarantee.
    for (const auto * name : {"k", "alias", "preserved", "nullable"})
    {
        SCOPED_TRACE(name);
        EXPECT_EQ(stats.at(name).ndv_provenance.transformations, 0);
        EXPECT_EQ(stats.at(name).range_provenance.transformations, 0);
        const auto range = stats.at(name).valueRange(ValueRangeGuarantee::Exact);
        ASSERT_TRUE(range.has_value());
        EXPECT_EQ(range->null_fraction, 0.25);
    }

    /// Value-changing outputs keep the distinct count of their input as an upper bound, but the measured range does not
    /// describe their values, so it is cleared, also for readers that do not consult the provenance. The NULL fraction
    /// stays with the provenance that disqualifies it.
    for (const auto * name : {"casted", "collapsed", "chained"})
    {
        SCOPED_TRACE(name);
        EXPECT_TRUE(stats.at(name).ndv_provenance.has(NDVBoundExpression));
        EXPECT_TRUE(stats.at(name).range_provenance.has(NDVBoundExpression));
        EXPECT_EQ(stats.at(name).range_provenance.origin, ColumnStatsOrigin::MeasuredStatistics);
        EXPECT_EQ(stats.at(name).num_distinct_values, 50);
        EXPECT_FALSE(stats.at(name).min_value.has_value());
        EXPECT_FALSE(stats.at(name).max_value.has_value());
        EXPECT_EQ(stats.at(name).null_fraction, 0.25);
        EXPECT_TRUE(stats.at(name).distinctCount(DistinctCountGuarantee::UpperBound).has_value());
        EXPECT_FALSE(stats.at(name).valueRange(ValueRangeGuarantee::Superset).has_value());
    }
}

TEST(ColumnStatsProvenance, KeepOnlyEnclosingValueRangeClearsRangesWithoutSupersetGuarantee)
{
    /// Each transformation alone, with the measured origin.
    for (const auto [transformations, enclosing] : std::initializer_list<std::pair<UInt16, bool>>{
             {0, true},
             {RowSubset, true},
             {EstimatedRowCountClamp, false},
             {NDVBoundExpression, false},
             {PartialStatisticsCoverage, false},
             {Unmodeled, false},
             {RowSubset | Unmodeled, false}})
    {
        SCOPED_TRACE(provenance(ColumnStatsOrigin::MeasuredStatistics, transformations).toString());
        auto stats = measuredColumnStats();
        stats.null_fraction = 0.25;
        stats.range_provenance.transformations = transformations;
        keepOnlyEnclosingValueRange(stats);
        EXPECT_EQ(stats.min_value.has_value(), enclosing);
        EXPECT_EQ(stats.max_value.has_value(), enclosing);
        /// Everything else is untouched: the NULL fraction, the provenance and the distinct count.
        EXPECT_EQ(stats.null_fraction, 0.25);
        EXPECT_EQ(stats.range_provenance.transformations, transformations);
        EXPECT_EQ(stats.range_provenance.origin, ColumnStatsOrigin::MeasuredStatistics);
        EXPECT_EQ(stats.num_distinct_values, 50);
    }

    /// A range that was not measured, e.g. a test-only statistics hint, is not enclosing either.
    for (const auto origin : {ColumnStatsOrigin::Unknown, ColumnStatsOrigin::SyntheticFallback, ColumnStatsOrigin::SyntheticOverride})
    {
        SCOPED_TRACE(provenance(origin).toString());
        auto stats = measuredColumnStats();
        stats.range_provenance.origin = origin;
        keepOnlyEnclosingValueRange(stats);
        EXPECT_FALSE(stats.min_value.has_value());
        EXPECT_FALSE(stats.max_value.has_value());
        EXPECT_EQ(stats.range_provenance.origin, origin);
    }

    /// The distinct-count provenance does not matter for the range.
    auto stats = measuredColumnStats();
    stats.ndv_provenance.add(Unmodeled);
    keepOnlyEnclosingValueRange(stats);
    EXPECT_TRUE(stats.min_value.has_value());

    /// Without a range there is nothing to clear.
    ColumnStats without_range{.num_distinct_values = 50, .ndv_provenance = provenance(ColumnStatsOrigin::MeasuredStatistics)};
    keepOnlyEnclosingValueRange(without_range);
    EXPECT_FALSE(without_range.min_value.has_value());
    EXPECT_EQ(without_range.num_distinct_values, 50);
}

namespace
{

/// A header with the column `passthrough` and the array column `arr`.
SharedHeader makeArrayJoinHeader(const String & passthrough)
{
    const auto element_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(element_type);
    return std::make_shared<const Block>(Block(
        {ColumnWithTypeAndName(element_type->createColumn(), element_type, passthrough),
         ColumnWithTypeAndName(array_type->createColumn(), array_type, "arr")}));
}

/// The `arrayJoin(arr) AS arr` form of ARRAY JOIN, which keeps `passthrough`.
std::unique_ptr<ExpressionStep> makeArrayJoinExpressionStep(const String & passthrough)
{
    const auto header = makeArrayJoinHeader(passthrough);
    ActionsDAG dag(header->getColumnsWithTypeAndName());
    const auto & array_join = dag.addArrayJoin(*dag.getInputs().back(), "arr");
    dag.addOrReplaceInOutputs(array_join);
    return std::make_unique<ExpressionStep>(header, std::move(dag));
}

}

TEST(ColumnStatsProvenance, ArrayJoinMultipliesRowsAndRecordsRowSubset)
{
    const auto header = makeArrayJoinHeader("k");
    std::vector<std::unique_ptr<IQueryPlanStep>> steps;
    steps.push_back(makeArrayJoinExpressionStep("k"));
    for (const bool is_left : {false, true})
        steps.push_back(std::make_unique<ArrayJoinStep>(
            header,
            ArrayJoin{.columns = {"arr"}, .is_left = is_left},
            /*is_unaligned_=*/false,
            /*max_block_size_=*/65536,
            /*enable_lazy_columns_replication_=*/false));

    for (const auto & step : steps)
    {
        SCOPED_TRACE(step->getName());
        auto input_stats = inputRelationStats();
        input_stats.column_stats.emplace("arr", measuredColumnStats());
        const auto result = estimateUnaryStepStats(*step, std::move(input_stats));
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->estimated_rows, 100 * 100);
        EXPECT_FALSE(result->rows_exact);

        /// The statistics of the array do not describe its elements.
        EXPECT_FALSE(result->column_stats.contains("arr"));

        /// Repeating rows keeps the values of the other columns, but not their distribution.
        ASSERT_TRUE(result->column_stats.contains("k"));
        const auto & stats = result->column_stats.at("k");
        EXPECT_EQ(stats.num_distinct_values, 50);
        EXPECT_TRUE(stats.ndv_provenance.has(RowSubset));
        EXPECT_TRUE(stats.range_provenance.has(RowSubset));
        EXPECT_TRUE(stats.distinctCount(DistinctCountGuarantee::UpperBound).has_value());
        EXPECT_FALSE(stats.distinctCount(DistinctCountGuarantee::Exact).has_value());
        EXPECT_TRUE(stats.valueRange(ValueRangeGuarantee::Superset).has_value());
        EXPECT_FALSE(stats.valueRange(ValueRangeGuarantee::Exact).has_value());
        EXPECT_FALSE(stats.valueRange(ValueRangeGuarantee::Representative).has_value());
    }
}

TEST(ColumnStatsProvenance, ArrayJoinOfExactlyEmptyInputIsExactlyEmpty)
{
    const auto step = makeArrayJoinExpressionStep("k");

    auto empty_input = inputRelationStats();
    empty_input.estimated_rows = 0;
    const auto empty = estimateUnaryStepStats(*step, std::move(empty_input));
    ASSERT_TRUE(empty.has_value());
    EXPECT_EQ(empty->estimated_rows, 0);
    EXPECT_TRUE(empty->rows_exact);

    auto estimated_empty_input = inputRelationStats(/*rows_exact=*/false);
    estimated_empty_input.estimated_rows = 0;
    const auto estimated_empty = estimateUnaryStepStats(*step, std::move(estimated_empty_input));
    ASSERT_TRUE(estimated_empty.has_value());
    EXPECT_EQ(estimated_empty->estimated_rows, 0);
    EXPECT_FALSE(estimated_empty->rows_exact);

    auto unknown_input = inputRelationStats(/*rows_exact=*/false);
    unknown_input.estimated_rows.reset();
    const auto unknown = estimateUnaryStepStats(*step, std::move(unknown_input));
    ASSERT_TRUE(unknown.has_value());
    EXPECT_FALSE(unknown->estimated_rows.has_value());
    EXPECT_FALSE(unknown->rows_exact);
}

TEST(ColumnStatsProvenance, FilterAboveArrayJoinIsNotPushedBelowIt)
{
    /// A filter above an array join leaves its row count unknown unless the input is empty. Pushed below, the filter
    /// would make the estimate of the sub-join unknown (see `JoinEstimateUnderPlanTimeFilterIsUnknown`), so an empty
    /// sub-join shows that the filter stays above.
    for (const UInt64 join_rows : {UInt64{0}, UInt64{100}})
    {
        SCOPED_TRACE(join_rows);
        QueryPlan::Node join_node;
        join_node.step = makeOptimizedJoinStep(JoinKind::Inner, JoinStrictness::All, "l", join_rows);

        QueryPlan::Node array_join_node;
        array_join_node.step = makeArrayJoinExpressionStep("l");
        array_join_node.children = {&join_node};

        const auto header = makeHeader("l");
        QueryPlan::Node filter_node;
        filter_node.step = std::make_unique<FilterStep>(header, makeFilterDAG(header), "__filter", true);
        filter_node.children = {&array_join_node};

        const auto stats = estimateReadRowsCount(filter_node);
        if (join_rows == 0)
            EXPECT_EQ(stats.estimated_rows, 0);
        else
            EXPECT_FALSE(stats.estimated_rows.has_value());
        EXPECT_FALSE(stats.rows_exact);
        ASSERT_TRUE(stats.column_stats.contains("l"));
        const auto & ndv_provenance = stats.column_stats.at("l").ndv_provenance;
        EXPECT_TRUE(ndv_provenance.has(Unmodeled));
        EXPECT_TRUE(ndv_provenance.has(RowSubset));
    }
}

TEST(ColumnStatsProvenance, DistinctFillingAndUnionAreUnsupportedBoundaries)
{
    const auto header = makeHeader();

    expectUnsupportedUnaryStepStopsBeforeChild(
        std::make_unique<DistinctStep>(header, DistinctStep::Settings{}, 0, Names{"k"}, false), header);

    auto fill_sort = sortByKey();
    fill_sort.front().with_fill = true;
    expectUnsupportedUnaryStepStopsBeforeChild(std::make_unique<FillingStep>(header, std::move(fill_sort), nullptr, false), header);

    size_t left_visits = 0;
    size_t right_visits = 0;
    QueryPlan::Node left;
    left.step = std::make_unique<CountingSourceStep>(header, left_visits);
    QueryPlan::Node right;
    right.step = std::make_unique<CountingSourceStep>(header, right_visits);
    QueryPlan::Node union_node;
    union_node.step = std::make_unique<UnionStep>(SharedHeaders{header, header});
    union_node.children = {&left, &right};

    const auto union_result = estimateReadRowsCount(union_node);
    EXPECT_FALSE(union_result.estimated_rows.has_value());
    EXPECT_TRUE(union_result.column_stats.empty());
    EXPECT_EQ(left_visits, 0);
    EXPECT_EQ(right_visits, 0);
}

TEST(ColumnStatsProvenance, TransformationsAppendToBothFactsIdempotently)
{
    RelationStats stats;
    stats.column_stats
        = {{"measured",
            ColumnStats{
                .num_distinct_values = 2000,
                .ndv_provenance = provenance(ColumnStatsOrigin::MeasuredStatistics),
                .min_value = Field(UInt64{10}),
                .max_value = Field(UInt64{20}),
                .range_provenance = provenance(ColumnStatsOrigin::MeasuredStatistics)}},
           {"synthetic",
            ColumnStats{
                .num_distinct_values = 2000, .ndv_provenance = provenance(ColumnStatsOrigin::SyntheticFallback), .range_provenance = {}}}};
    stats.addTransformation(RowSubset);
    stats.addTransformation(RowSubset);

    const auto & measured = stats.column_stats.at("measured");
    const auto & synthetic = stats.column_stats.at("synthetic");
    EXPECT_EQ(measured.ndv_provenance.transformations, RowSubset);
    EXPECT_EQ(measured.range_provenance.transformations, RowSubset);
    EXPECT_EQ(measured.ndv_provenance.origin, ColumnStatsOrigin::MeasuredStatistics);
    EXPECT_EQ(synthetic.ndv_provenance.origin, ColumnStatsOrigin::SyntheticFallback);
    EXPECT_EQ(measured.num_distinct_values, 2000);
    EXPECT_EQ(synthetic.num_distinct_values, 2000);
}

TEST(ColumnStatsProvenance, GroupingKeyPreservesDistinctCountAndRange)
{
    const ColumnStats input{
        .num_distinct_values = 2000,
        .ndv_provenance = provenance(ColumnStatsOrigin::MeasuredStatistics, RowSubset),
        .avg_bytes = 8,
        .min_value = Field(UInt64{10}),
        .max_value = Field(UInt64{20}),
        .null_fraction = 0.25,
        .range_provenance = provenance(ColumnStatsOrigin::MeasuredStatistics, RowSubset)};

    RelationStats input_relation;
    input_relation.estimated_rows = 1000;

    const auto estimated = makeGroupingKeyStats(input, input_relation);
    EXPECT_EQ(estimated.num_distinct_values, 1000);
    EXPECT_EQ(estimated.ndv_provenance.origin, ColumnStatsOrigin::MeasuredStatistics);
    EXPECT_TRUE(estimated.ndv_provenance.has(RowSubset));
    EXPECT_TRUE(estimated.ndv_provenance.has(EstimatedRowCountClamp));
    EXPECT_EQ(estimated.min_value, input.min_value);
    EXPECT_EQ(estimated.max_value, input.max_value);
    EXPECT_EQ(estimated.range_provenance.transformations, input.range_provenance.transformations);
    EXPECT_FALSE(estimated.null_fraction.has_value());

    /// A clamp to an exact row count cannot make the distinct count too small.
    input_relation.rows_exact = true;
    const auto exact = makeGroupingKeyStats(input, input_relation);
    EXPECT_EQ(exact.num_distinct_values, 1000);
    EXPECT_EQ(exact.ndv_provenance.transformations, input.ndv_provenance.transformations);
}

TEST(ColumnStatsProvenance, JoinCardinalityClampInvalidatesDistinctCountUpperBound)
{
    auto clamped_stats = measuredColumnStats();
    clamped_stats.num_distinct_values = 1000;
    auto retained_stats = measuredColumnStats();

    auto left = std::make_shared<DPJoinEntry>(
        0, 1000, std::unordered_map<String, ColumnStats>{{"clamped", clamped_stats}, {"retained", retained_stats}});
    auto right = std::make_shared<DPJoinEntry>(1, 1000);
    DPJoinEntry joined(left, right, 0, 1, 100, JoinOperator{});

    const auto & clamped = joined.column_stats.at("clamped");
    EXPECT_EQ(clamped.num_distinct_values, 100);
    EXPECT_TRUE(clamped.ndv_provenance.has(EstimatedRowCountClamp));
    EXPECT_FALSE(clamped.distinctCount(DistinctCountGuarantee::UpperBound).has_value());
    EXPECT_FALSE(clamped.range_provenance.has(EstimatedRowCountClamp));

    const auto & retained = joined.column_stats.at("retained");
    EXPECT_EQ(retained.num_distinct_values, 50);
    EXPECT_FALSE(retained.ndv_provenance.has(EstimatedRowCountClamp));
    EXPECT_TRUE(retained.distinctCount(DistinctCountGuarantee::UpperBound).has_value());
}

TEST(ColumnStatsProvenance, FilterAddsRowSubsetAndMakesRowsInexact)
{
    const auto header = makeHeader();
    FilterStep step(header, makeFilterDAG(header), "__filter", true);

    auto result = estimateUnaryStepStats(step, inputRelationStats());
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->estimated_rows.has_value());
    EXPECT_EQ(*result->estimated_rows, 100);
    EXPECT_FALSE(result->rows_exact);

    const auto & stats = result->column_stats.at("k");
    EXPECT_TRUE(stats.ndv_provenance.has(RowSubset));
    EXPECT_TRUE(stats.range_provenance.has(RowSubset));
    EXPECT_EQ(stats.min_value, Field(UInt64{10}));
    EXPECT_EQ(stats.max_value, Field(UInt64{59}));
}

TEST(ColumnStatsProvenance, LimitRecordsRowSubsetAndTracksExactRows)
{
    const auto header = makeHeader();
    LimitStep exact_limit(header, 10, 5);
    auto exact = estimateUnaryStepStats(exact_limit, inputRelationStats());
    ASSERT_TRUE(exact.has_value());
    ASSERT_TRUE(exact->estimated_rows.has_value());
    EXPECT_EQ(*exact->estimated_rows, 10);
    EXPECT_TRUE(exact->rows_exact);
    EXPECT_TRUE(exact->column_stats.at("k").ndv_provenance.has(RowSubset));
    EXPECT_TRUE(exact->column_stats.at("k").range_provenance.has(RowSubset));
    EXPECT_EQ(exact->column_stats.at("k").null_fraction, 0.0);

    LimitStep non_truncating_limit(header, 100, 0);
    auto non_truncated = estimateUnaryStepStats(non_truncating_limit, inputRelationStats());
    ASSERT_TRUE(non_truncated.has_value());
    EXPECT_TRUE(non_truncated->rows_exact);
    EXPECT_FALSE(non_truncated->column_stats.at("k").ndv_provenance.has(RowSubset));
    EXPECT_FALSE(non_truncated->column_stats.at("k").range_provenance.has(RowSubset));

    LimitStep with_ties(header, 10, 0, false, true, sortByKey());
    auto tied = estimateUnaryStepStats(with_ties, inputRelationStats());
    ASSERT_TRUE(tied.has_value());
    EXPECT_FALSE(tied->rows_exact);

    auto estimated_input = inputRelationStats(false);
    LimitStep estimated_limit(header, 10, 0);
    auto estimated = estimateUnaryStepStats(estimated_limit, std::move(estimated_input));
    ASSERT_TRUE(estimated.has_value());
    EXPECT_FALSE(estimated->rows_exact);
    EXPECT_TRUE(estimated->column_stats.at("k").ndv_provenance.has(RowSubset));

    auto unknown_input = inputRelationStats();
    unknown_input.estimated_rows.reset();
    auto unknown = estimateUnaryStepStats(estimated_limit, std::move(unknown_input));
    ASSERT_TRUE(unknown.has_value());
    EXPECT_EQ(unknown->estimated_rows, 10);
    EXPECT_FALSE(unknown->rows_exact);
    EXPECT_TRUE(unknown->column_stats.at("k").ndv_provenance.has(RowSubset));
}

TEST(ColumnStatsProvenance, SortingLimitRecordsRowSubsetAndPreservesExactness)
{
    const auto header = makeHeader();
    SortingStep limited_sort(
        header,
        sortByKey(),
        10,
        SortingStep::Settings(8192));
    auto limited = estimateUnaryStepStats(limited_sort, inputRelationStats());
    ASSERT_TRUE(limited.has_value());
    ASSERT_TRUE(limited->estimated_rows.has_value());
    EXPECT_EQ(*limited->estimated_rows, 10);
    EXPECT_TRUE(limited->rows_exact);
    EXPECT_TRUE(limited->column_stats.at("k").ndv_provenance.has(RowSubset));
    EXPECT_TRUE(limited->column_stats.at("k").range_provenance.has(RowSubset));

    SortingStep non_truncating_sort(header, sortByKey(), 100, SortingStep::Settings(8192));
    auto non_truncated = estimateUnaryStepStats(non_truncating_sort, inputRelationStats());
    ASSERT_TRUE(non_truncated.has_value());
    EXPECT_TRUE(non_truncated->rows_exact);
    EXPECT_FALSE(non_truncated->column_stats.at("k").ndv_provenance.has(RowSubset));
    EXPECT_FALSE(non_truncated->column_stats.at("k").range_provenance.has(RowSubset));

    SortingStep unlimited_sort(
        header,
        sortByKey(),
        0,
        SortingStep::Settings(8192));
    auto unlimited = estimateUnaryStepStats(unlimited_sort, inputRelationStats());
    ASSERT_TRUE(unlimited.has_value());
    EXPECT_TRUE(unlimited->rows_exact);
    EXPECT_FALSE(unlimited->column_stats.at("k").ndv_provenance.has(RowSubset));

    auto unknown_input = inputRelationStats();
    unknown_input.estimated_rows.reset();
    auto unknown = estimateUnaryStepStats(limited_sort, std::move(unknown_input));
    ASSERT_TRUE(unknown.has_value());
    EXPECT_EQ(unknown->estimated_rows, 10);
    EXPECT_FALSE(unknown->rows_exact);
    EXPECT_TRUE(unknown->column_stats.at("k").ndv_provenance.has(RowSubset));
}

TEST(ColumnStatsProvenance, NonThrowingSortLimitsRecordRowSubset)
{
    const auto header = makeHeader();
    auto settings_with_limits = [](SizeLimits size_limits)
    {
        SortingStep::Settings settings(8192);
        settings.size_limits = size_limits;
        return settings;
    };

    /// Without the size limits, neither sort drops a row of the exact 100-row input.
    for (const UInt64 limit : {UInt64{0}, UInt64{100}})
    {
        for (const auto & size_limits : {SizeLimits(10, 0, OverflowMode::BREAK), SizeLimits(0, 64, OverflowMode::BREAK)})
        {
            SCOPED_TRACE(fmt::format("limit {}, max_rows {}, max_bytes {}", limit, size_limits.max_rows, size_limits.max_bytes));
            SortingStep sort(header, sortByKey(), limit, settings_with_limits(size_limits));
            auto stats = estimateUnaryStepStats(sort, inputRelationStats());
            ASSERT_TRUE(stats.has_value());
            EXPECT_EQ(stats->estimated_rows, 100);
            EXPECT_FALSE(stats->rows_exact);
            const auto & column = stats->column_stats.at("k");
            EXPECT_TRUE(column.ndv_provenance.has(RowSubset));
            EXPECT_TRUE(column.range_provenance.has(RowSubset));
            EXPECT_FALSE(column.valueRange(ValueRangeGuarantee::Representative).has_value());
            EXPECT_TRUE(column.valueRange(ValueRangeGuarantee::Superset).has_value());
        }

        SCOPED_TRACE(fmt::format("limit {}, throwing", limit));
        SortingStep throwing_sort(header, sortByKey(), limit, settings_with_limits(SizeLimits(10, 64, OverflowMode::THROW)));
        auto throwing = estimateUnaryStepStats(throwing_sort, inputRelationStats());
        ASSERT_TRUE(throwing.has_value());
        EXPECT_TRUE(throwing->rows_exact);
        EXPECT_FALSE(throwing->column_stats.at("k").ndv_provenance.has(RowSubset));
        EXPECT_TRUE(throwing->column_stats.at("k").valueRange(ValueRangeGuarantee::Representative).has_value());
    }
}

TEST(ColumnStatsProvenance, AggregationPreservesPlainKeysAndRejectsGroupingSetsAndOverflow)
{
    const auto header = makeHeader();

    auto plain_step = makeAggregationStep(header, false, false);
    auto plain = estimateUnaryStepStats(*plain_step, inputRelationStats());
    ASSERT_TRUE(plain.has_value());
    ASSERT_TRUE(plain->estimated_rows.has_value());
    EXPECT_EQ(*plain->estimated_rows, 50);
    EXPECT_FALSE(plain->rows_exact);
    EXPECT_FALSE(plain->column_stats.at("k").ndv_provenance.has(Unmodeled));
    const auto plain_range = plain->column_stats.at("k").valueRange(ValueRangeGuarantee::Representative);
    ASSERT_TRUE(plain_range.has_value());
    EXPECT_EQ(plain_range->min, Field(UInt64{10}));
    EXPECT_EQ(plain_range->max, Field(UInt64{59}));

    /// Grouping sets and the overflow row add keys the input does not have, so the output keys get no range at all.
    auto grouping_sets_step = makeAggregationStep(header, true, false);
    auto grouping_sets = estimateUnaryStepStats(*grouping_sets_step, inputRelationStats());
    ASSERT_TRUE(grouping_sets.has_value());
    EXPECT_TRUE(grouping_sets->column_stats.at("k").ndv_provenance.has(Unmodeled));
    EXPECT_TRUE(grouping_sets->column_stats.at("k").range_provenance.has(Unmodeled));
    EXPECT_FALSE(grouping_sets->column_stats.at("k").min_value.has_value());
    EXPECT_FALSE(grouping_sets->column_stats.at("k").max_value.has_value());
    EXPECT_EQ(grouping_sets->column_stats.at("k").num_distinct_values, 50);

    auto overflow_step = makeAggregationStep(header, false, true);
    auto overflow = estimateUnaryStepStats(*overflow_step, inputRelationStats());
    ASSERT_TRUE(overflow.has_value());
    EXPECT_TRUE(overflow->column_stats.at("k").ndv_provenance.has(Unmodeled));
    EXPECT_TRUE(overflow->column_stats.at("k").range_provenance.has(Unmodeled));
    EXPECT_FALSE(overflow->column_stats.at("k").min_value.has_value());
    EXPECT_FALSE(overflow->column_stats.at("k").max_value.has_value());
}

/// Limits, sorts with a limit and grouping keys used to come without a value range. They keep one only while it still
/// encloses the produced values, which depends on what the steps below recorded.
TEST(ColumnStatsProvenance, LimitSortLimitAndGroupingKeepOnlyEnclosingRanges)
{
    const auto header = makeHeader();

    /// A filter keeps an enclosing range (`RowSubset`), a generic row-preserving step disqualifies it (`Unmodeled`), and a
    /// range that was not measured (a statistics hint) was never enclosing.
    auto filtered_input = [&]
    {
        FilterStep filter(header, makeFilterDAG(header), "__filter", true);
        return *estimateUnaryStepStats(filter, inputRelationStats());
    };
    auto unmodeled_input = [&]
    {
        PreservingTestStep preserving(header);
        return *estimateUnaryStepStats(preserving, inputRelationStats());
    };
    auto hinted_input = [&]
    {
        auto result = inputRelationStats();
        result.column_stats.at("k").range_provenance.origin = ColumnStatsOrigin::SyntheticOverride;
        return result;
    };

    auto expect_range = [](const std::optional<RelationStats> & result, bool kept, UInt16 recorded)
    {
        ASSERT_TRUE(result.has_value());
        const auto & stats = result->column_stats.at("k");
        EXPECT_EQ(stats.min_value.has_value(), kept);
        EXPECT_EQ(stats.max_value.has_value(), kept);
        /// Clearing the range does not change what is recorded about it, or the distinct count.
        EXPECT_EQ(stats.range_provenance.transformations, recorded);
        EXPECT_EQ(stats.num_distinct_values, 50);
    };

    /// The limit cannot drop a row of the exact 100-row input, so it records nothing itself.
    LimitStep limit(header, 100, 0);
    {
        SCOPED_TRACE("limit");
        expect_range(estimateUnaryStepStats(limit, filtered_input()), true, RowSubset);
        expect_range(estimateUnaryStepStats(limit, unmodeled_input()), false, Unmodeled);
        expect_range(estimateUnaryStepStats(limit, hinted_input()), false, 0);
        /// The NULL fraction stays with its provenance.
        EXPECT_EQ(estimateUnaryStepStats(limit, unmodeled_input())->column_stats.at("k").null_fraction, 0.0);
    }

    SortingStep limited_sort(header, sortByKey(), 100, SortingStep::Settings(8192));
    SortingStep unlimited_sort(header, sortByKey(), 0, SortingStep::Settings(8192));
    {
        SCOPED_TRACE("sort");
        expect_range(estimateUnaryStepStats(limited_sort, filtered_input()), true, RowSubset);
        expect_range(estimateUnaryStepStats(limited_sort, unmodeled_input()), false, Unmodeled);
        expect_range(estimateUnaryStepStats(limited_sort, hinted_input()), false, 0);
        /// A sort without a limit never cleared ranges and still does not: the facts stay for the provenance to judge.
        expect_range(estimateUnaryStepStats(unlimited_sort, unmodeled_input()), true, Unmodeled);
    }

    const auto grouping = makeAggregationStep(header, false, false);
    {
        SCOPED_TRACE("grouping");
        const auto filtered = estimateUnaryStepStats(*grouping, filtered_input());
        expect_range(filtered, true, RowSubset);
        /// The range of a grouping key over a filtered input encloses the keys without being representative of them.
        EXPECT_TRUE(filtered->column_stats.at("k").valueRange(ValueRangeGuarantee::Superset).has_value());
        EXPECT_FALSE(filtered->column_stats.at("k").valueRange(ValueRangeGuarantee::Representative).has_value());
        expect_range(estimateUnaryStepStats(*grouping, unmodeled_input()), false, Unmodeled);
        expect_range(estimateUnaryStepStats(*grouping, hinted_input()), false, 0);
    }
}

TEST(ColumnStatsProvenance, AggregationWithGroupByLimitRecordsRowSubset)
{
    const auto header = makeHeader();
    for (const auto mode : {OverflowMode::ANY, OverflowMode::BREAK})
    {
        SCOPED_TRACE(static_cast<UInt64>(mode));
        const auto step = makeAggregationStepWithGroupLimit(header, 1, mode);
        const auto stats = estimateUnaryStepStats(*step, inputRelationStats());
        ASSERT_TRUE(stats.has_value());
        EXPECT_TRUE(stats->column_stats.at("k").ndv_provenance.has(RowSubset));
        EXPECT_TRUE(stats->column_stats.at("k").range_provenance.has(RowSubset));
    }

    const auto throwing_step = makeAggregationStepWithGroupLimit(header, 1, OverflowMode::THROW);
    const auto throwing = estimateUnaryStepStats(*throwing_step, inputRelationStats());
    ASSERT_TRUE(throwing.has_value());
    EXPECT_FALSE(throwing->column_stats.at("k").ndv_provenance.has(RowSubset));
    EXPECT_FALSE(throwing->column_stats.at("k").range_provenance.has(RowSubset));

    const auto unlimited_step = makeAggregationStepWithGroupLimit(header, 0, OverflowMode::ANY);
    const auto unlimited = estimateUnaryStepStats(*unlimited_step, inputRelationStats());
    ASSERT_TRUE(unlimited.has_value());
    EXPECT_FALSE(unlimited->column_stats.at("k").ndv_provenance.has(RowSubset));
    EXPECT_FALSE(unlimited->column_stats.at("k").range_provenance.has(RowSubset));
}

TEST(ColumnStatsProvenance, JoinEstimatesArePropagated)
{
    QueryPlan::Node node;
    node.step = makeOptimizedJoinStep(JoinKind::Inner, JoinStrictness::All, "l");

    const auto propagated_stats = estimateReadRowsCount(node);
    ASSERT_TRUE(propagated_stats.estimated_rows.has_value());
    EXPECT_EQ(*propagated_stats.estimated_rows, 100);
    ASSERT_TRUE(propagated_stats.column_stats.contains("l"));
    EXPECT_TRUE(propagated_stats.column_stats.at("l").ndv_provenance.has(Unmodeled));
    EXPECT_TRUE(propagated_stats.column_stats.at("l").range_provenance.has(Unmodeled));
    /// The relation name is a chain that already shows the sub-joins' estimates.
    EXPECT_TRUE(propagated_stats.composite);
}

TEST(ColumnStatsProvenance, JoinEstimateUnderPlanTimeFilterIsUnknown)
{
    QueryPlan::Node join_node;
    join_node.step = makeOptimizedJoinStep(JoinKind::Inner, JoinStrictness::All, "l");

    const auto header = makeHeader("l");
    QueryPlan::Node filter_node;
    filter_node.step = std::make_unique<FilterStep>(header, makeFilterDAG(header), "__filter", true);
    filter_node.children = {&join_node};

    const auto stats = estimateReadRowsCount(filter_node);
    /// The sub-join's estimate does not account for the filter above it, so the row count is unknown.
    EXPECT_FALSE(stats.estimated_rows.has_value());
    EXPECT_TRUE(stats.imprecise_estimate);
    EXPECT_FALSE(stats.rows_exact);
    EXPECT_TRUE(stats.composite);
    ASSERT_TRUE(stats.column_stats.contains("l"));
    EXPECT_TRUE(stats.column_stats.at("l").ndv_provenance.has(Unmodeled));
    EXPECT_TRUE(stats.column_stats.at("l").ndv_provenance.has(RowSubset));
    EXPECT_TRUE(stats.column_stats.at("l").range_provenance.has(RowSubset));
}

TEST(ColumnStatsProvenance, JoinEstimatesOfEveryKindAreUnmodeled)
{
    const std::vector join_cases{
        std::tuple{JoinKind::Inner, JoinStrictness::All, String{"l"}},
        std::tuple{JoinKind::Left, JoinStrictness::All, String{"l"}},
        std::tuple{JoinKind::Full, JoinStrictness::All, String{"l"}},
        std::tuple{JoinKind::Paste, JoinStrictness::All, String{"l"}},
        std::tuple{JoinKind::Left, JoinStrictness::Semi, String{"l"}},
        std::tuple{JoinKind::Right, JoinStrictness::Semi, String{"r"}},
        std::tuple{JoinKind::Left, JoinStrictness::Anti, String{"l"}},
        std::tuple{JoinKind::Right, JoinStrictness::Anti, String{"r"}}};
    for (const auto & [kind, strictness, column_name] : join_cases)
    {
        SCOPED_TRACE(static_cast<UInt64>(kind));
        SCOPED_TRACE(static_cast<UInt64>(strictness));
        QueryPlan::Node node;
        node.step = makeOptimizedJoinStep(kind, strictness, column_name);

        const auto stats = estimateReadRowsCount(node);
        ASSERT_TRUE(stats.estimated_rows.has_value());
        EXPECT_FALSE(stats.rows_exact);
        ASSERT_TRUE(stats.column_stats.contains(column_name));
        EXPECT_TRUE(stats.column_stats.at(column_name).ndv_provenance.has(Unmodeled));
        EXPECT_TRUE(stats.column_stats.at(column_name).range_provenance.has(Unmodeled));
    }
}

TEST(ColumnStatsProvenance, PassThroughStepsKeepFactsAndProvenance)
{
    const auto header = makeHeader();
    CommonSubplanStep common_subplan(header);
    ExtremesStep extremes(header);
    SaveSubqueryResultToBufferStep save_to_buffer(header, {"k"}, {});

    for (const auto * step : std::initializer_list<const IQueryPlanStep *>{&common_subplan, &extremes, &save_to_buffer})
    {
        SCOPED_TRACE(step->getName());
        const auto result = estimateUnaryStepStats(*step, inputRelationStats());
        ASSERT_TRUE(result.has_value());
        EXPECT_TRUE(result->rows_exact);
        ASSERT_EQ(result->estimated_rows, 100);

        const auto & stats = result->column_stats.at("k");
        EXPECT_EQ(stats.ndv_provenance.transformations, 0);
        EXPECT_EQ(stats.range_provenance.transformations, 0);
        EXPECT_TRUE(stats.valueRange(ValueRangeGuarantee::Representative).has_value());
    }
}

TEST(ColumnStatsProvenance, CommonSubplanReferenceSeesThroughCommonSubplanStep)
{
    const auto header = makeHeader();
    QueryPlan::Node source;
    source.step = std::make_unique<TestSystemOneStep>(header);
    QueryPlan::Node common_subplan;
    common_subplan.step = std::make_unique<CommonSubplanStep>(header);
    common_subplan.children = {&source};
    QueryPlan::Node reference;
    reference.step = std::make_unique<CommonSubplanReferenceStep>(header, &common_subplan, ColumnIdentifiers{"k"});

    const auto stats = estimateReadRowsCount(reference);
    ASSERT_EQ(stats.estimated_rows, 1);
    EXPECT_TRUE(stats.rows_exact);
    EXPECT_TRUE(stats.column_stats.empty());
}

TEST(ColumnStatsProvenance, GenericPreservingTransformFailsClosedWithoutErasingFacts)
{
    PreservingTestStep step(makeHeader());
    auto result = estimateUnaryStepStats(step, inputRelationStats());
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->rows_exact);

    const auto & stats = result->column_stats.at("k");
    EXPECT_TRUE(stats.ndv_provenance.has(Unmodeled));
    EXPECT_TRUE(stats.range_provenance.has(Unmodeled));
    EXPECT_EQ(stats.num_distinct_values, 50);
    EXPECT_EQ(stats.min_value, Field(UInt64{10}));
    EXPECT_EQ(stats.max_value, Field(UInt64{59}));
}

TEST(ColumnStatsProvenance, SystemOneRowsAreExact)
{
    QueryPlan::Node node;
    node.step = std::make_unique<TestSystemOneStep>(makeHeader());
    const auto stats = estimateReadRowsCount(node);
    ASSERT_TRUE(stats.estimated_rows.has_value());
    EXPECT_EQ(*stats.estimated_rows, 1);
    EXPECT_TRUE(stats.rows_exact);
}
