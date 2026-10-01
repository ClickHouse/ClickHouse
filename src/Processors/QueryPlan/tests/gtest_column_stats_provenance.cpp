#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

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
        .ndv_provenance = provenance(ColumnStatsOrigin::PartStatistics),
        .avg_bytes = 8,
        .min_value = Field(UInt64{10}),
        .max_value = Field(UInt64{59}),
        .range_provenance = provenance(ColumnStatsOrigin::PartStatistics),
        .null_fraction = 0.0};
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

std::unique_ptr<AggregatingStep> makeAggregationStep(
    const SharedHeader & header,
    bool grouping_sets,
    bool overflow_row)
{
    Aggregator::Params params(
        Names{"k"},
        AggregateDescriptions{},
        overflow_row,
        1,
        65536,
        0.5f,
        false,
        true);

    GroupingSetsParamsList grouping_sets_params;
    if (grouping_sets)
        grouping_sets_params.emplace_back(Names{"k"}, Names{});

    return std::make_unique<AggregatingStep>(
        header,
        std::move(params),
        std::move(grouping_sets_params),
        true,
        65536,
        0,
        1,
        1,
        false,
        false,
        SortDescription{},
        SortDescription{},
        false,
        false,
        false);
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

std::unique_ptr<JoinStepLogical> makeOptimizedJoinStep(JoinKind kind, JoinStrictness strictness, String stats_column)
{
    auto step = makeLogicalJoinStep(kind, strictness);
    step->setOptimized(100, {{std::move(stats_column), measuredColumnStats()}});
    return step;
}

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

class CountingSystemOneStep final : public ISourceStep
{
public:
    CountingSystemOneStep(SharedHeader header, size_t & visits_)
        : ISourceStep(std::move(header))
        , visits(visits_)
    {
    }

    String getName() const override
    {
        ++visits;
        return "ReadFromSystemOne";
    }
    QueryPlanStepPtr clone() const override { return std::make_unique<CountingSystemOneStep>(getOutputHeader(), visits); }
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
    EXPECT_EQ(provenance(ColumnStatsOrigin::PartStatistics).toString(), "part-statistics");
    EXPECT_EQ(provenance(ColumnStatsOrigin::SyntheticFallback).toString(), "synthetic-fallback");
    EXPECT_EQ(provenance(ColumnStatsOrigin::ExactRowCount).toString(), "exact-row-count");

    constexpr UInt16 all_transformations
        = RowSubset | NonUniformRowSubset | ExactRowCountClamp | EstimatedRowCountClamp | NDVBoundExpression
        | ValuePreservingExpression | PartialPartCoverage | Unsupported;
    EXPECT_EQ(
        provenance(ColumnStatsOrigin::PartStatistics, all_transformations).toString(),
        "part-statistics[row-subset,non-uniform-row-subset,exact-row-clamp,estimated-row-clamp,"
        "ndv-bound-expression,value-preserving-expression,partial-part-coverage,unsupported]");
}

TEST(ColumnStatsProvenance, ExpressionLineageAppendsTransformationsAndKeepsFacts)
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
    auto result = estimateUnaryStepStats(step, inputRelationStats());
    ASSERT_TRUE(result.has_value());
    const auto & stats = result->column_stats;

    EXPECT_TRUE(result->rows_exact);
    EXPECT_EQ(stats.at("k").ndv_provenance.transformations, 0);
    EXPECT_TRUE(stats.at("alias").ndv_provenance.has(ValuePreservingExpression));
    EXPECT_TRUE(stats.at("alias").range_provenance.has(ValuePreservingExpression));
    EXPECT_TRUE(stats.at("preserved").ndv_provenance.has(ValuePreservingExpression));
    EXPECT_TRUE(stats.at("preserved").range_provenance.has(ValuePreservingExpression));
    EXPECT_TRUE(stats.at("nullable").ndv_provenance.has(ValuePreservingExpression));
    EXPECT_TRUE(stats.at("nullable").range_provenance.has(ValuePreservingExpression));
    EXPECT_TRUE(stats.at("casted").ndv_provenance.has(NDVBoundExpression));
    EXPECT_TRUE(stats.at("casted").range_provenance.has(NDVBoundExpression));
    EXPECT_TRUE(stats.at("collapsed").ndv_provenance.has(NDVBoundExpression));
    EXPECT_TRUE(stats.at("collapsed").range_provenance.has(NDVBoundExpression));
    EXPECT_TRUE(stats.at("chained").ndv_provenance.has(NDVBoundExpression));
    EXPECT_EQ(stats.at("collapsed").num_distinct_values, 50);
    EXPECT_EQ(stats.at("collapsed").min_value, Field(UInt64{10}));
    EXPECT_EQ(stats.at("collapsed").max_value, Field(UInt64{59}));
}

TEST(ColumnStatsProvenance, ArrayJoinExpressionIsUnsupported)
{
    const auto element_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(element_type);
    const auto header = std::make_shared<const Block>(
        Block({ColumnWithTypeAndName(array_type->createColumn(), array_type, "k")}));
    ActionsDAG dag(header->getColumnsWithTypeAndName());
    const auto & array_join = dag.addArrayJoin(*dag.getInputs().front(), "k");
    dag.addOrReplaceInOutputs(array_join);

    auto step = std::make_unique<ExpressionStep>(header, std::move(dag));
    EXPECT_FALSE(estimateUnaryStepStats(*step, inputRelationStats()).has_value());

    size_t child_visits = 0;
    QueryPlan::Node child;
    child.step = std::make_unique<CountingSourceStep>(header, child_visits);
    QueryPlan::Node root;
    root.step = std::move(step);
    root.children.emplace_back(&child);
    const auto result = estimateReadRowsCount(root);
    EXPECT_FALSE(result.estimated_rows.has_value());
    EXPECT_TRUE(result.column_stats.empty());
    EXPECT_EQ(child_visits, 0);
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
    std::unordered_map<String, ColumnStats> stats{
        {"measured",
         ColumnStats{
             .num_distinct_values = 2000,
             .ndv_provenance = provenance(ColumnStatsOrigin::PartStatistics),
             .min_value = Field(UInt64{10}),
             .max_value = Field(UInt64{20}),
             .range_provenance = provenance(ColumnStatsOrigin::PartStatistics)}},
        {"synthetic",
         ColumnStats{
             .num_distinct_values = 2000,
             .ndv_provenance = provenance(ColumnStatsOrigin::SyntheticFallback),
             .range_provenance = {}}}};
    addTransformation(stats, RowSubset);
    addTransformation(stats, RowSubset);

    EXPECT_TRUE(stats.at("measured").ndv_provenance.has(RowSubset));
    EXPECT_TRUE(stats.at("measured").range_provenance.has(RowSubset));
    EXPECT_EQ(stats.at("measured").ndv_provenance.origin, ColumnStatsOrigin::PartStatistics);
    EXPECT_EQ(stats.at("synthetic").ndv_provenance.origin, ColumnStatsOrigin::SyntheticFallback);
    EXPECT_EQ(stats.at("measured").num_distinct_values, 2000);
    EXPECT_EQ(stats.at("synthetic").num_distinct_values, 2000);
}

TEST(ColumnStatsProvenance, GroupingKeyPreservesDistinctCountAndRange)
{
    const ColumnStats input{
        .num_distinct_values = 2000,
        .ndv_provenance = provenance(ColumnStatsOrigin::PartStatistics, RowSubset),
        .avg_bytes = 8,
        .min_value = Field(UInt64{10}),
        .max_value = Field(UInt64{20}),
        .range_provenance = provenance(ColumnStatsOrigin::PartStatistics, RowSubset),
        .null_fraction = 0.25};

    const auto estimated = makeGroupingKeyStats(input, 1000, /*rows_exact=*/false);
    EXPECT_EQ(estimated.num_distinct_values, 1000);
    EXPECT_EQ(estimated.ndv_provenance.origin, ColumnStatsOrigin::PartStatistics);
    EXPECT_TRUE(estimated.ndv_provenance.has(RowSubset));
    EXPECT_TRUE(estimated.ndv_provenance.has(EstimatedRowCountClamp));
    EXPECT_EQ(estimated.min_value, input.min_value);
    EXPECT_EQ(estimated.max_value, input.max_value);
    EXPECT_EQ(estimated.range_provenance.transformations, input.range_provenance.transformations);
    EXPECT_FALSE(estimated.null_fraction.has_value());

    const auto exact = makeGroupingKeyStats(input, 1000, /*rows_exact=*/true);
    EXPECT_TRUE(exact.ndv_provenance.has(ExactRowCountClamp));
    EXPECT_FALSE(exact.ndv_provenance.has(EstimatedRowCountClamp));
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

TEST(ColumnStatsProvenance, LimitRecordsNonUniformSubsetAndTracksExactRows)
{
    const auto header = makeHeader();
    LimitStep exact_limit(header, 10, 5);
    auto exact = estimateUnaryStepStats(exact_limit, inputRelationStats());
    ASSERT_TRUE(exact.has_value());
    ASSERT_TRUE(exact->estimated_rows.has_value());
    EXPECT_EQ(*exact->estimated_rows, 10);
    EXPECT_TRUE(exact->rows_exact);
    EXPECT_TRUE(exact->column_stats.at("k").ndv_provenance.has(NonUniformRowSubset));
    EXPECT_TRUE(exact->column_stats.at("k").range_provenance.has(NonUniformRowSubset));

    LimitStep non_truncating_limit(header, 100, 0);
    auto non_truncated = estimateUnaryStepStats(non_truncating_limit, inputRelationStats());
    ASSERT_TRUE(non_truncated.has_value());
    EXPECT_TRUE(non_truncated->rows_exact);
    EXPECT_FALSE(non_truncated->column_stats.at("k").ndv_provenance.has(NonUniformRowSubset));
    EXPECT_FALSE(non_truncated->column_stats.at("k").range_provenance.has(NonUniformRowSubset));

    LimitStep with_ties(header, 10, 0, false, true, sortByKey());
    auto tied = estimateUnaryStepStats(with_ties, inputRelationStats());
    ASSERT_TRUE(tied.has_value());
    EXPECT_FALSE(tied->rows_exact);

    auto estimated_input = inputRelationStats(false);
    LimitStep estimated_limit(header, 10, 0);
    auto estimated = estimateUnaryStepStats(estimated_limit, std::move(estimated_input));
    ASSERT_TRUE(estimated.has_value());
    EXPECT_FALSE(estimated->rows_exact);
    EXPECT_TRUE(estimated->column_stats.at("k").ndv_provenance.has(NonUniformRowSubset));
}

TEST(ColumnStatsProvenance, SortingLimitRecordsNonUniformSubsetAndPreservesExactness)
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
    EXPECT_TRUE(limited->column_stats.at("k").ndv_provenance.has(NonUniformRowSubset));
    EXPECT_TRUE(limited->column_stats.at("k").range_provenance.has(NonUniformRowSubset));

    SortingStep non_truncating_sort(header, sortByKey(), 100, SortingStep::Settings(8192));
    auto non_truncated = estimateUnaryStepStats(non_truncating_sort, inputRelationStats());
    ASSERT_TRUE(non_truncated.has_value());
    EXPECT_TRUE(non_truncated->rows_exact);
    EXPECT_FALSE(non_truncated->column_stats.at("k").ndv_provenance.has(NonUniformRowSubset));
    EXPECT_FALSE(non_truncated->column_stats.at("k").range_provenance.has(NonUniformRowSubset));

    SortingStep unlimited_sort(
        header,
        sortByKey(),
        0,
        SortingStep::Settings(8192));
    auto unlimited = estimateUnaryStepStats(unlimited_sort, inputRelationStats());
    ASSERT_TRUE(unlimited.has_value());
    EXPECT_TRUE(unlimited->rows_exact);
    EXPECT_FALSE(unlimited->column_stats.at("k").ndv_provenance.has(NonUniformRowSubset));
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
    EXPECT_FALSE(plain->column_stats.at("k").ndv_provenance.has(Unsupported));
    EXPECT_TRUE(isRepresentativeValueRange(plain->column_stats.at("k").range_provenance));
    EXPECT_EQ(plain->column_stats.at("k").min_value, Field(UInt64{10}));
    EXPECT_EQ(plain->column_stats.at("k").max_value, Field(UInt64{59}));

    auto grouping_sets_step = makeAggregationStep(header, true, false);
    auto grouping_sets = estimateUnaryStepStats(*grouping_sets_step, inputRelationStats());
    ASSERT_TRUE(grouping_sets.has_value());
    EXPECT_TRUE(grouping_sets->column_stats.at("k").ndv_provenance.has(Unsupported));
    EXPECT_TRUE(grouping_sets->column_stats.at("k").range_provenance.has(Unsupported));
    EXPECT_FALSE(isRepresentativeValueRange(grouping_sets->column_stats.at("k").range_provenance));

    auto overflow_step = makeAggregationStep(header, false, true);
    auto overflow = estimateUnaryStepStats(*overflow_step, inputRelationStats());
    ASSERT_TRUE(overflow.has_value());
    EXPECT_TRUE(overflow->column_stats.at("k").ndv_provenance.has(Unsupported));
    EXPECT_TRUE(overflow->column_stats.at("k").range_provenance.has(Unsupported));
    EXPECT_FALSE(isRepresentativeValueRange(overflow->column_stats.at("k").range_provenance));
}

TEST(ColumnStatsProvenance, JoinEstimatesArePropagatedOnlyOnRequest)
{
    QueryPlan::Node node;
    node.step = makeOptimizedJoinStep(JoinKind::Inner, JoinStrictness::All, "l");
    RelationStatsCache cache;

    const auto default_stats = estimateReadRowsCount(node, nullptr, {}, &cache);
    EXPECT_FALSE(default_stats.estimated_rows.has_value());
    EXPECT_TRUE(default_stats.column_stats.empty());

    const auto propagated_stats = estimateReadRowsCount(
        node,
        nullptr,
        RelationStatsOptions{.propagate_join_estimates = true},
        &cache);
    ASSERT_TRUE(propagated_stats.estimated_rows.has_value());
    EXPECT_EQ(*propagated_stats.estimated_rows, 100);
    ASSERT_TRUE(propagated_stats.column_stats.contains("l"));
    EXPECT_TRUE(propagated_stats.column_stats.at("l").ndv_provenance.has(Unsupported));
    EXPECT_TRUE(propagated_stats.column_stats.at("l").range_provenance.has(Unsupported));
}

TEST(ColumnStatsProvenance, JoinKindsRecordUnsupportedAndSemiAntiSubsets)
{
    auto estimate_join = [](JoinKind kind, JoinStrictness strictness, const String & stats_column)
    {
        QueryPlan::Node node;
        node.step = makeOptimizedJoinStep(kind, strictness, stats_column);
        return estimateReadRowsCount(
            node,
            nullptr,
            RelationStatsOptions{.propagate_join_estimates = true});
    };

    for (const auto kind : {JoinKind::Inner, JoinKind::Left})
    {
        SCOPED_TRACE(static_cast<UInt64>(kind));
        const auto stats = estimate_join(kind, JoinStrictness::All, "l");
        const auto & column = stats.column_stats.at("l");
        EXPECT_TRUE(column.ndv_provenance.has(Unsupported));
        EXPECT_TRUE(column.range_provenance.has(Unsupported));
        EXPECT_FALSE(column.ndv_provenance.has(RowSubset));
        EXPECT_FALSE(stats.rows_exact);
    }

    const std::vector semi_anti_cases{
        std::tuple{JoinKind::Left, JoinStrictness::Semi, String{"l"}},
        std::tuple{JoinKind::Right, JoinStrictness::Semi, String{"r"}},
        std::tuple{JoinKind::Left, JoinStrictness::Anti, String{"l"}},
        std::tuple{JoinKind::Right, JoinStrictness::Anti, String{"r"}}};
    for (const auto & [kind, strictness, column_name] : semi_anti_cases)
    {
        SCOPED_TRACE(static_cast<UInt64>(kind));
        SCOPED_TRACE(static_cast<UInt64>(strictness));
        const auto stats = estimate_join(kind, strictness, column_name);
        const auto & column = stats.column_stats.at(column_name);
        EXPECT_TRUE(column.ndv_provenance.has(Unsupported));
        EXPECT_TRUE(column.ndv_provenance.has(RowSubset));
        EXPECT_TRUE(column.range_provenance.has(Unsupported));
        EXPECT_TRUE(column.range_provenance.has(RowSubset));
    }
}

TEST(ColumnStatsProvenance, FullAndPasteJoinEstimatesFailClosed)
{
    for (const auto kind : {JoinKind::Full, JoinKind::Paste})
    {
        SCOPED_TRACE(static_cast<UInt64>(kind));
        QueryPlan::Node node;
        node.step = makeOptimizedJoinStep(kind, JoinStrictness::All, "l");

        const auto stats = estimateReadRowsCount(node, nullptr, RelationStatsOptions{.propagate_join_estimates = true});
        ASSERT_TRUE(stats.estimated_rows.has_value());
        ASSERT_TRUE(stats.column_stats.contains("l"));
        EXPECT_TRUE(stats.column_stats.at("l").ndv_provenance.has(Unsupported));
        EXPECT_TRUE(stats.column_stats.at("l").range_provenance.has(Unsupported));
        EXPECT_FALSE(stats.rows_exact);
    }
}

TEST(ColumnStatsProvenance, GenericPreservingTransformFailsClosedWithoutErasingFacts)
{
    PreservingTestStep step(makeHeader());
    auto result = estimateUnaryStepStats(step, inputRelationStats());
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->rows_exact);

    const auto & stats = result->column_stats.at("k");
    EXPECT_TRUE(stats.ndv_provenance.has(Unsupported));
    EXPECT_TRUE(stats.range_provenance.has(Unsupported));
    EXPECT_EQ(stats.num_distinct_values, 50);
    EXPECT_EQ(stats.min_value, Field(UInt64{10}));
    EXPECT_EQ(stats.max_value, Field(UInt64{59}));
}

TEST(ColumnStatsProvenance, RelationStatsCacheSharesOptionIndependentEntriesAndSupportsInvalidation)
{
    size_t visits = 0;
    QueryPlan::Node node;
    node.step = std::make_unique<CountingSourceStep>(makeHeader(), visits);
    RelationStatsCache cache;

    estimateReadRowsCount(node, nullptr, {}, &cache);
    estimateReadRowsCount(
        node,
        nullptr,
        RelationStatsOptions{.propagate_join_estimates = true},
        &cache);
    EXPECT_EQ(visits, 1);

    cache.invalidate(node);
    estimateReadRowsCount(node, nullptr, {}, &cache);
    EXPECT_EQ(visits, 2);
}

TEST(ColumnStatsProvenance, RelationStatsCacheRebindsWrappedSubtreeAndSeparatesFilters)
{
    const auto header = makeHeader();
    size_t visits = 0;
    QueryPlan::Node node;
    node.step = std::make_unique<CountingSystemOneStep>(header, visits);
    RelationStatsCache cache;

    const auto original_stats = estimateReadRowsCount(node, nullptr, {}, &cache);
    ASSERT_EQ(original_stats.estimated_rows, 1);
    EXPECT_EQ(visits, 1);

    QueryPlan::Nodes nodes;
    auto & moved_subtree = nodes.emplace_back(std::move(node));
    node = QueryPlan::Node{std::make_unique<LimitStep>(header, 0, 0), {&moved_subtree}};
    cache.rebindNode(node, moved_subtree);

    const auto wrapped_stats = estimateReadRowsCount(node, nullptr, {}, &cache);
    ASSERT_EQ(wrapped_stats.estimated_rows, 0);
    EXPECT_EQ(visits, 1);

    auto first_filter_dag = makeFilterDAG(header);
    const auto * first_filter = first_filter_dag.tryFindInOutputs("__filter");
    estimateReadRowsCount(moved_subtree, first_filter, {}, &cache);
    estimateReadRowsCount(moved_subtree, first_filter, {}, &cache);
    EXPECT_EQ(visits, 2);

    auto second_filter_dag = makeFilterDAG(header);
    const auto * second_filter = second_filter_dag.tryFindInOutputs("__filter");
    estimateReadRowsCount(moved_subtree, second_filter, {}, &cache);
    EXPECT_EQ(visits, 3);
}

TEST(ColumnStatsProvenance, PredicatesEstablishOnlyNamedGuarantees)
{
    const auto exact = ColumnStatsProvenance{.origin = ColumnStatsOrigin::PartStatistics};
    EXPECT_TRUE(isExactDistinctCount(exact));
    EXPECT_TRUE(isDistinctCountUpperBound(exact));
    EXPECT_TRUE(isExactValueRange(exact));
    EXPECT_TRUE(isValueRangeSuperset(exact));
    EXPECT_TRUE(isRepresentativeValueRange(exact));

    auto row_subset = exact;
    row_subset.add(RowSubset);
    EXPECT_FALSE(isExactDistinctCount(row_subset));
    EXPECT_TRUE(isDistinctCountUpperBound(row_subset));
    EXPECT_FALSE(isExactValueRange(row_subset));
    EXPECT_TRUE(isValueRangeSuperset(row_subset));
    EXPECT_FALSE(isRepresentativeValueRange(row_subset));

    auto non_uniform_subset = exact;
    non_uniform_subset.add(NonUniformRowSubset);
    EXPECT_FALSE(isExactValueRange(non_uniform_subset));
    EXPECT_TRUE(isValueRangeSuperset(non_uniform_subset));
    EXPECT_FALSE(isRepresentativeValueRange(non_uniform_subset));

    auto partial = exact;
    partial.add(PartialPartCoverage);
    EXPECT_FALSE(isDistinctCountUpperBound(partial));
    EXPECT_FALSE(isExactValueRange(partial));
    EXPECT_FALSE(isValueRangeSuperset(partial));
    EXPECT_FALSE(isRepresentativeValueRange(partial));

    auto estimated_clamp = exact;
    estimated_clamp.add(EstimatedRowCountClamp);
    EXPECT_FALSE(isDistinctCountUpperBound(estimated_clamp));

    auto expression = exact;
    expression.add(NDVBoundExpression);
    EXPECT_FALSE(isExactValueRange(expression));
    EXPECT_FALSE(isValueRangeSuperset(expression));
    EXPECT_FALSE(isRepresentativeValueRange(expression));

    const auto exact_rows = ColumnStatsProvenance{.origin = ColumnStatsOrigin::ExactRowCount};
    EXPECT_FALSE(isExactDistinctCount(exact_rows));
    EXPECT_TRUE(isDistinctCountUpperBound(exact_rows));
    const auto synthetic = ColumnStatsProvenance{.origin = ColumnStatsOrigin::SyntheticFallback};
    EXPECT_FALSE(isDistinctCountUpperBound(synthetic));
}

TEST(ColumnStatsProvenance, RelationStatsCacheDoesNotPersistAcrossMutableJoins)
{
    QueryPlan::Node join_node;
    auto join_step = makeLogicalJoinStep(JoinKind::Inner, JoinStrictness::All);
    auto * mutable_join_step = join_step.get();
    const auto join_header = join_step->getOutputHeader();
    join_node.step = std::move(join_step);

    QueryPlan::Node root;
    root.step = std::make_unique<PreservingTestStep>(join_header);
    root.children = {&join_node};

    RelationStatsCache cache;
    const auto options = RelationStatsOptions{.propagate_join_estimates = true};
    EXPECT_FALSE(estimateReadRowsCount(root, nullptr, options, &cache).estimated_rows.has_value());

    mutable_join_step->setOptimized(100, {{"l", measuredColumnStats()}});
    cache.invalidate(join_node);
    const auto stats = estimateReadRowsCount(root, nullptr, options, &cache);
    ASSERT_EQ(stats.estimated_rows, 100);
    EXPECT_TRUE(stats.column_stats.at("l").ndv_provenance.has(Unsupported));
}

TEST(ColumnStatsProvenance, RelationStatsTraversalIsIterative)
{
    constexpr size_t depth = 5000;
    const auto header = makeHeader();
    QueryPlan::Nodes nodes;

    auto & leaf = nodes.emplace_back();
    leaf.step = std::make_unique<TestSystemOneStep>(header);
    QueryPlan::Node * root = &leaf;
    for (size_t i = 0; i < depth; ++i)
    {
        auto & parent = nodes.emplace_back();
        parent.step = std::make_unique<PreservingTestStep>(header);
        parent.children = {root};
        root = &parent;
    }

    const auto stats = estimateReadRowsCount(*root);
    ASSERT_TRUE(stats.estimated_rows.has_value());
    EXPECT_EQ(*stats.estimated_rows, 1);
    EXPECT_TRUE(stats.rows_exact);
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
