#include <gtest/gtest.h>

#include <filesystem>
#include <functional>

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/ProtocolDefines.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StepDigestCounters.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StepIdentity.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/StepIdentity.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageSnapshot.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

/// The Cascades cross-group identity compares the content of a step (its wire encoding plus the
/// audited non-wire fields), not its name and description like the within-group
/// `structurallyEqualTo`. It fails closed: a step type that has not opted in compares by pointer.

namespace
{

SharedHeader makeHeader()
{
    ColumnWithTypeAndName column;
    column.name = "x";
    column.type = std::make_shared<DataTypeUInt64>();
    column.column = column.type->createColumn();
    return std::make_shared<const Block>(Block{{column}});
}

/// Input `x` plus a constant output named `c`, so two DAGs differing only in `constant` produce
/// the same output header and differ only in the serialized DAG payload.
ActionsDAG makeDag(UInt64 constant)
{
    auto type = std::make_shared<DataTypeUInt64>();
    ActionsDAG dag(NamesAndTypesList{{"x", type}});
    dag.getOutputs().push_back(&dag.addColumn(type->createColumnConst(1, Field(constant)), type, "c"));
    return dag;
}

GroupExpressionPtr exprWithExpressionStep(const SharedHeader & header, UInt64 constant)
{
    return std::make_shared<GroupExpression>(std::make_unique<ExpressionStep>(header, makeDag(constant)));
}

/// Input `x` plus a constant UInt8 filter column `f`, so two DAGs differing only in `constant`
/// produce the same output header and differ only in the serialized DAG payload.
ActionsDAG makeFilterDag(UInt64 constant)
{
    auto type = std::make_shared<DataTypeUInt64>();
    auto filter_type = std::make_shared<DataTypeUInt8>();
    ActionsDAG dag(NamesAndTypesList{{"x", type}});
    dag.getOutputs().push_back(&dag.addColumn(filter_type->createColumnConst(1, Field(static_cast<UInt8>(constant))), filter_type, "f"));
    return dag;
}

SharedHeader makeAggregatedHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

/// Merge-only `Aggregator::Params` constructor - the same one `MergingAggregatedStep::deserialize`
/// uses, so `only_merge` is always true.
Aggregator::Params makeMergingAggregatedParams(size_t max_threads, size_t params_max_block_size = 65536)
{
    return Aggregator::Params(
        Names{"k"},
        AggregateDescriptions{},
        /*overflow_row=*/false,
        max_threads,
        params_max_block_size,
        /*min_hit_rate_to_use_consecutive_keys_optimization=*/0.5f,
        /*serialize_string_with_zero_byte=*/false,
        /*enable_packed_string_keys=*/true);
}

std::unique_ptr<MergingAggregatedStep> makeMergingAggregatedStep(
    size_t max_threads, size_t memory_efficient_merge_threads, bool final = true, size_t bucket_top_k = 0)
{
    auto params = makeMergingAggregatedParams(max_threads);
    /// `bucket_top_k` has no constructor argument on the merge-only `Params` overload; a Cascades
    /// rule that only copies `AggregatingStep`'s full `Params` (see `TwoStageAggregationTransformation`)
    /// can still carry it, so mutate the public field directly to reproduce that shape here.
    params.bucket_top_k = bucket_top_k;

    return std::make_unique<MergingAggregatedStep>(
        makeAggregatedHeader(),
        std::move(params),
        GroupingSetsParamsList{},
        final,
        /*memory_efficient_aggregation=*/false,
        memory_efficient_merge_threads,
        /*should_produce_results_in_order_of_bucket_number=*/false,
        /*max_block_size=*/65536,
        /*memory_bound_merging_max_block_bytes=*/0,
        /*memory_bound_merging_of_aggregation_results_enabled=*/false);
}

/// Merge-only `Aggregator::Params` again - `AggregatingStep` only stores them, and with no aggregate
/// functions the output header is the key list for both `final` values, so a test that flips `final`
/// really does compare two steps that differ in nothing else.
std::unique_ptr<AggregatingStep> makeAggregatingStep(
    bool final, size_t merge_threads = 4, UInt64 limit_hint = 0, size_t params_max_block_size = 65536)
{
    auto step = std::make_unique<AggregatingStep>(
        makeAggregatedHeader(),
        makeMergingAggregatedParams(/*max_threads=*/4, params_max_block_size),
        GroupingSetsParamsList{},
        final,
        /*max_block_size=*/65536,
        /*aggregation_in_order_max_block_bytes=*/0,
        merge_threads,
        /*temporary_data_merge_threads=*/4,
        /*storage_has_evenly_distributed_read=*/false,
        /*group_by_use_nulls=*/false,
        /*sort_description_for_merging=*/SortDescription{},
        /*group_by_sort_description=*/SortDescription{},
        /*should_produce_results_in_order_of_bucket_number=*/false,
        /*memory_bound_merging_of_aggregation_results_enabled=*/false,
        /*explicit_sorting_required_for_aggregation_in_order=*/false,
        /*enable_sharding_aggregator=*/false);

    if (limit_hint)
        step->setLimitHint(limit_hint);

    return step;
}

/// `overflow_row`, `max_rows_to_group_by` and `group_by_overflow_mode` are `const` members of
/// `Aggregator::Params`, so a pin that varies one of them has to go through the full ctor. Every
/// other value matches `makeMergingAggregatedParams`, and both sides of such a pin are built here,
/// so the two ctors are never compared against each other.
struct AggregationLimits
{
    bool overflow_row = false;
    size_t max_rows_to_group_by = 0;
    OverflowMode group_by_overflow_mode = OverflowMode::THROW;
};

Aggregator::Params makeLimitedAggregatedParams(const AggregationLimits & limits)
{
    return Aggregator::Params(
        Names{"k"},
        AggregateDescriptions{},
        limits.overflow_row,
        limits.max_rows_to_group_by,
        limits.group_by_overflow_mode,
        /*group_by_two_level_threshold_=*/0,
        /*group_by_two_level_threshold_bytes_=*/0,
        /*max_bytes_before_external_group_by_=*/0,
        /*empty_result_for_aggregation_by_empty_set_=*/false,
        /*tmp_data_scope_=*/nullptr,
        /*max_threads_=*/4,
        /*min_free_disk_space_=*/0,
        /*compile_aggregate_expressions_=*/false,
        /*min_count_to_compile_aggregate_expression_=*/0,
        /*max_block_size_=*/65536,
        /*enable_prefetch_=*/false,
        /*only_merge_=*/true,
        /*optimize_group_by_constant_keys_=*/false,
        /*min_hit_rate_to_use_consecutive_keys_optimization_=*/0.5f,
        StatsCollectingParams{},
        /*enable_producing_buckets_out_of_order_in_aggregation_=*/true,
        /*serialize_string_with_zero_byte_=*/false,
        /*enable_parallel_single_level_merge_=*/false,
        /*enable_packed_string_keys_=*/true,
        /*enable_adaptive_aggregator_=*/false,
        /*adaptive_aggregator_freeze_threshold_=*/0);
}

/// Knobs and flags a logical-audit pin has to vary on `AggregatingStep`. Everything not listed here
/// keeps the value `makeAggregatingStep` uses, so a pin really compares two steps that differ in one
/// field.
struct AggregatingVariant
{
    bool group_by_use_nulls = false;
    SortDescription group_by_sort_description = {};
    bool results_in_bucket_order = false;
    bool memory_bound_merging = false;
    std::optional<AggregationLimits> limits = {};
    std::function<void(Aggregator::Params &)> tweak_params = {};
};

std::unique_ptr<AggregatingStep> makeAggregatingVariant(const AggregatingVariant & variant)
{
    auto params = variant.limits ? makeLimitedAggregatedParams(*variant.limits) : makeMergingAggregatedParams(/*max_threads=*/4);
    if (variant.tweak_params)
        variant.tweak_params(params);

    return std::make_unique<AggregatingStep>(
        makeAggregatedHeader(),
        std::move(params),
        GroupingSetsParamsList{},
        /*final=*/true,
        /*max_block_size=*/65536,
        /*aggregation_in_order_max_block_bytes=*/0,
        /*merge_threads=*/4,
        /*temporary_data_merge_threads=*/4,
        /*storage_has_evenly_distributed_read=*/false,
        variant.group_by_use_nulls,
        /*sort_description_for_merging=*/SortDescription{},
        variant.group_by_sort_description,
        variant.results_in_bucket_order,
        variant.memory_bound_merging,
        /*explicit_sorting_required_for_aggregation_in_order=*/false,
        /*enable_sharding_aggregator=*/false);
}

/// The same for `MergingAggregatedStep`.
struct MergingAggregatedVariant
{
    bool memory_efficient_aggregation = false;
    SortDescription group_by_sort_description = {};
    bool results_in_bucket_order = false;
    bool memory_bound_merging = false;
    std::optional<AggregationLimits> limits = {};
    std::function<void(Aggregator::Params &)> tweak_params = {};
};

std::unique_ptr<MergingAggregatedStep> makeMergingAggregatedVariant(const MergingAggregatedVariant & variant)
{
    auto params = variant.limits ? makeLimitedAggregatedParams(*variant.limits) : makeMergingAggregatedParams(/*max_threads=*/4);
    if (variant.tweak_params)
        variant.tweak_params(params);

    auto step = std::make_unique<MergingAggregatedStep>(
        makeAggregatedHeader(),
        std::move(params),
        GroupingSetsParamsList{},
        /*final=*/true,
        variant.memory_efficient_aggregation,
        /*memory_efficient_merge_threads=*/4,
        variant.results_in_bucket_order,
        /*max_block_size=*/65536,
        /*memory_bound_merging_max_block_bytes=*/0,
        variant.memory_bound_merging);

    if (!variant.group_by_sort_description.empty())
        step->applyOrder(variant.group_by_sort_description);

    return step;
}

SortDescription makeSortDescription()
{
    SortDescription description;
    description.emplace_back(SortColumnDescription("x", "x"));
    return description;
}

/// The `Settings(size_t)` constructor leaves `temporary_files_buffer_size` at 0, which makes
/// `serializeSettings` throw on that `NonZeroUInt64` plan setting, so a sort that has to have an
/// identity must be built from the plan settings instead.
SortingStep::Settings makeSortSettings()
{
    QueryPlanSerializationSettings serialization_settings;
    return SortingStep::Settings(serialization_settings);
}

/// A bounded (top-N) full sort: `Type::Full` with `scatter_partitions == 0`, i.e. serializable, which
/// `supportsCascadesIdentity` requires.
std::unique_ptr<SortingStep> makeSortingStep(const SharedHeader & header)
{
    return std::make_unique<SortingStep>(header, makeSortDescription(), /*limit_=*/10, makeSortSettings());
}

/// The other serializable shape, `Type::FinishSorting`. Built through the `MergingSorted` ctor
/// because that is the only one that can set `always_read_till_end`, then converted - the same path
/// `applyOrder` takes when a distributed plan finds the input already sorted by a prefix.
std::unique_ptr<SortingStep> makeFinishSortingStep(
    const SharedHeader & header,
    bool always_read_till_end = false,
    SortDescription prefix = {},
    bool apply_virtual_row_conversions = false)
{
    auto step = std::make_unique<SortingStep>(
        header, makeSortDescription(), makeSortSettings(), /*limit_=*/10, always_read_till_end);
    step->convertToFinishSorting(std::move(prefix), /*use_buffering_=*/false, apply_virtual_row_conversions);
    return step;
}

/// A bounded partitioned full sort: still `Type::Full` with `scatter_partitions == 0`, so serializable.
std::unique_ptr<SortingStep> makePartitionedSortingStep(const SharedHeader & header, const SortDescription & partition_by)
{
    return std::make_unique<SortingStep>(header, makeSortDescription(), partition_by, /*limit_=*/10, makeSortSettings());
}

/// A cross join over two disjoint single-column relations. Every DAG node is an `INPUT`, so the step
/// serializes without any registered functions, and both inputs become outputs so that
/// `updateOutputHeader` has a non-empty header to build.
std::unique_ptr<JoinStepLogical> makeJoinStepLogical(bool with_correlated_column = false)
{
    auto type = std::make_shared<DataTypeUInt64>();
    auto left_header = std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "l")}));
    auto right_header = std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "r")}));

    JoinExpressionActions expression_actions(*left_header, *right_header);
    auto actions_dag = expression_actions.getActionsDAG();
    for (const auto * input : actions_dag->getInputs())
        actions_dag->getOutputs().push_back(input);

    if (with_correlated_column)
        actions_dag->addPlaceholder("correlated", type);

    QueryPlanSerializationSettings serialization_settings;
    return std::make_unique<JoinStepLogical>(
        left_header,
        right_header,
        JoinOperator(JoinKind::Cross),
        std::move(expression_actions),
        ActionsDAG::NodeRawConstPtrs{},
        JoinSettings(serialization_settings),
        makeSortSettings());
}

}

TEST(CascadesStepIdentity, ClonesOfSameStepAreEqual)
{
    auto header = makeHeader();
    ExpressionStep step(header, makeDag(1));

    auto a = std::make_shared<GroupExpression>(step.clone());
    auto b = std::make_shared<GroupExpression>(step.clone());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_TRUE(a->fullyEqualTo(*b));
}

TEST(CascadesStepIdentity, DifferentDagContentIsUnequal)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    auto b = exprWithExpressionStep(header, 2);

    /// Same input and output headers - only the constant inside the DAG differs.
    EXPECT_EQ(
        a->plan_step->getOutputHeader()->getNamesAndTypesList().toString(),
        b->plan_step->getOutputHeader()->getNamesAndTypesList().toString());
    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

TEST(CascadesStepIdentity, StepDescriptionIsExcluded)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<ExpressionStep>(header, makeDag(1));
    auto b_step = std::make_unique<ExpressionStep>(header, makeDag(1));
    b_step->setStepDescription("projection over a very different reason");

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_EQ(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_TRUE(a->fullyEqualTo(*b));
}

/// `prevent_input_removal` is not on the wire, but it blocks later input pruning, so the audit puts
/// it in the extras: the two steps below are not interchangeable.
TEST(CascadesStepIdentity, PreventInputRemovalIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<ExpressionStep>(header, makeDag(1));
    auto b_step = std::make_unique<ExpressionStep>(header, makeDag(1));
    b_step->setPreventInputRemoval();

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `Group` uses `enforced_property` to keep a self-referential enforcer from satisfying its own
/// input, so an enforcer and a plain expression over the same step must never be judged equal.
TEST(CascadesStepIdentity, EnforcedPropertyIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    auto b = exprWithExpressionStep(header, 1);
    ASSERT_TRUE(a->fullyEqualTo(*b));

    b->enforced_property = EnforcedProperty::Sorting;

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `description_suffix` is GroupExpression state set by rules (e.g. "(by col)"), not the step's
/// display description, and nothing guarantees it is free of meaning - so it is compared.
TEST(CascadesStepIdentity, DescriptionSuffixIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    auto b = exprWithExpressionStep(header, 1);
    ASSERT_TRUE(a->fullyEqualTo(*b));

    b->description_suffix = "(by k)";

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// The hash must be usable before any equality call, so that a memo can bucket expressions eagerly.
TEST(CascadesStepIdentity, IndependentlyBuiltStepsShareFingerprint)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 7);
    auto b = exprWithExpressionStep(header, 7);

    EXPECT_EQ(a->fullFingerprint(), b->fullFingerprint());
}

TEST(CascadesStepIdentity, StepWithoutOptInComparesByPointer)
{
    auto header = makeHeader();
    auto a = std::make_shared<GroupExpression>(std::make_unique<OffsetStep>(header, 10));
    auto b = std::make_shared<GroupExpression>(std::make_unique<OffsetStep>(header, 10));

    EXPECT_EQ(a->cachedStepFingerprint(), nullptr);
    /// Equal-looking but distinct instances are not interchangeable without a field audit.
    EXPECT_FALSE(a->fullyEqualTo(*b));

    // NOLINTNEXTLINE(performance-unnecessary-copy-initialization) - the shallow copy is the test subject
    GroupExpression shared_step_copy(*a);
    EXPECT_EQ(shared_step_copy.plan_step, a->plan_step);
    EXPECT_TRUE(a->fullyEqualTo(shared_step_copy));
    EXPECT_EQ(a->fullFingerprint(), shared_step_copy.fullFingerprint());
}

/// A rule may shallow-copy an expression and then replace its step. The inherited identity cache
/// must not be trusted for the new step.
TEST(CascadesStepIdentity, ReplacedStepInvalidatesInheritedIdentity)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    const auto fingerprint_a = a->fullFingerprint();

    auto b = std::make_shared<GroupExpression>(*a);
    ASSERT_EQ(b->fullFingerprint(), fingerprint_a);

    b->plan_step = std::make_shared<const ExpressionStep>(header, makeDag(2));

    EXPECT_NE(b->fullFingerprint(), fingerprint_a);
    EXPECT_FALSE(a->fullyEqualTo(*b));
    EXPECT_FALSE(b->fullyEqualTo(*a));
}

/// Moving a value between tags, or re-splitting two adjacent variable-length components, must
/// change the bytes - otherwise different field assignments could collide.
TEST(CascadesStepIdentity, ExtrasFramingIsInjective)
{
    SerializedSetsRegistry registry;
    auto dag = makeDag(1);

    WriteBufferFromOwnString dag_first;
    {
        StepDigestWriter extras(dag_first, registry);
        extras.addDAG(1, &dag);
        extras.addAbsent(2);
    }

    WriteBufferFromOwnString dag_second;
    {
        StepDigestWriter extras(dag_second, registry);
        extras.addAbsent(1);
        extras.addDAG(2, &dag);
    }

    EXPECT_NE(dag_first.str(), dag_second.str());

    WriteBufferFromOwnString split_left;
    {
        StepDigestWriter extras(split_left, registry);
        extras.addStrings(1, Names{"a", "b"});
        extras.addStrings(2, Names{});
    }

    WriteBufferFromOwnString split_right;
    {
        StepDigestWriter extras(split_right, registry);
        extras.addStrings(1, Names{"a"});
        extras.addStrings(2, Names{"b"});
    }

    EXPECT_NE(split_left.str(), split_right.str());
}

TEST(CascadesStepIdentity, CountersCountDigestPasses)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    auto b = exprWithExpressionStep(header, 1);

    StepDigestCounters counters;
    CurrentStepDigestCounters counters_scope(counters);

    a->fullFingerprint();
    EXPECT_EQ(counters.digests_written, 1u);
    EXPECT_GT(counters.digest_bytes_written, 0u);
    EXPECT_EQ(counters.digest_confirmations, 0u);

    EXPECT_TRUE(a->fullyEqualTo(*b));
    /// One pass fingerprints `b`, then both steps are re-digested to be compared byte for byte.
    EXPECT_EQ(counters.digest_confirmations, 2u);
    EXPECT_EQ(counters.digests_written, 4u);
}

/// FilterStep

TEST(CascadesStepIdentity, FilterStepClonesAreEqual)
{
    auto header = makeHeader();
    FilterStep step(header, makeFilterDag(1), "f", /*remove_filter_column_=*/false);

    auto a = std::make_shared<GroupExpression>(step.clone());
    auto b = std::make_shared<GroupExpression>(step.clone());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_TRUE(a->fullyEqualTo(*b));
}

/// `prevent_input_removal` is not on the wire, but it blocks later input pruning, so the audit puts
/// it in the extras: the two steps below are not interchangeable.
TEST(CascadesStepIdentity, FilterStepPreventInputRemovalIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<FilterStep>(header, makeFilterDag(1), "f", /*remove_filter_column_=*/false);
    auto b_step = std::make_unique<FilterStep>(header, makeFilterDag(1), "f", /*remove_filter_column_=*/false);
    b_step->setPreventInputRemoval();

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `condition` is not on the wire, but when set it makes `transformPipeline` write to the query
/// condition cache at runtime under that hash/text, so the audit puts it in the extras too.
TEST(CascadesStepIdentity, FilterStepConditionIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<FilterStep>(header, makeFilterDag(1), "f", /*remove_filter_column_=*/false);
    auto b_step = std::make_unique<FilterStep>(header, makeFilterDag(1), "f", /*remove_filter_column_=*/false);
    b_step->setConditionForQueryConditionCache(42, "x > 0");

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

TEST(CascadesStepIdentity, FilterStepDifferentDagContentIsUnequal)
{
    auto header = makeHeader();
    auto a = std::make_shared<GroupExpression>(
        std::make_unique<FilterStep>(header, makeFilterDag(1), "f", /*remove_filter_column_=*/false));
    auto b = std::make_shared<GroupExpression>(
        std::make_unique<FilterStep>(header, makeFilterDag(2), "f", /*remove_filter_column_=*/false));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// LimitStep

TEST(CascadesStepIdentity, LimitStepClonesAreEqual)
{
    auto header = makeHeader();
    LimitStep step(header, 10, 0);

    auto a = std::make_shared<GroupExpression>(step.clone());
    auto b = std::make_shared<GroupExpression>(step.clone());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_TRUE(a->fullyEqualTo(*b));
}

/// `is_shard_limit` is not on the wire, but `QueryPipeline::initRowsBeforeLimit` special-cases it
/// when computing the user-visible `rows_before_limit_at_least`, so the audit puts it in the extras.
TEST(CascadesStepIdentity, LimitStepIsShardLimitIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<LimitStep>(header, 10, 0);
    auto b_step = std::make_unique<LimitStep>(header, 10, 0);
    b_step->markAsShardLimit();

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

TEST(CascadesStepIdentity, LimitStepDifferentLimitIsUnequal)
{
    auto header = makeHeader();
    auto a = std::make_shared<GroupExpression>(std::make_unique<LimitStep>(header, 10, 0));
    auto b = std::make_shared<GroupExpression>(std::make_unique<LimitStep>(header, 20, 0));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// MergingAggregatedStep

TEST(CascadesStepIdentity, MergingAggregatedStepClonesAreEqual)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto step = makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4);

    auto a = std::make_shared<GroupExpression>(step->clone());
    auto b = std::make_shared<GroupExpression>(step->clone());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_TRUE(a->fullyEqualTo(*b));
}

/// `max_threads` is not on the wire - `deserialize` re-derives it from the session setting - but it
/// controls how far `transformPipeline` resizes the pipeline, so the audit puts it in the extras.
TEST(CascadesStepIdentity, MergingAggregatedStepMaxThreadsIsPartOfIdentity)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto a = std::make_shared<GroupExpression>(makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4));
    auto b = std::make_shared<GroupExpression>(makeMergingAggregatedStep(/*max_threads=*/8, /*memory_efficient_merge_threads=*/4));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

TEST(CascadesStepIdentity, MergingAggregatedStepDifferentFinalFlagIsUnequal)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto a = std::make_shared<GroupExpression>(
        makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4, /*final=*/true));
    auto b = std::make_shared<GroupExpression>(
        makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4, /*final=*/false));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `params.bucket_top_k` is not on the wire, but `Aggregator::convertOneBucketToChunk` reads it
/// unconditionally on `final` (regardless of `only_merge`) from `MergingAggregatedTransform::generate`
/// via `convertToChunks` - two merge steps differing only here produce different output row counts,
/// so the wire-serialization "safe to drop" argument does not carry over to identity.
TEST(CascadesStepIdentity, MergingAggregatedStepBucketTopKIsPartOfIdentity)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto a = std::make_shared<GroupExpression>(
        makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4, /*final=*/true, /*bucket_top_k=*/0));
    auto b = std::make_shared<GroupExpression>(
        makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4, /*final=*/true, /*bucket_top_k=*/100));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// A correlated `PLACEHOLDER` node makes `ActionsDAG::serialize` throw, even though
/// `isSerializable()` is unconditionally `true`. `supportsCascadesIdentity()` must additionally
/// check `hasCorrelatedExpressions()` to keep its "never throws" invariant true by construction.
TEST(CascadesStepIdentity, ExpressionStepWithCorrelatedExpressionsDoesNotSupportIdentity)
{
    auto header = makeHeader();
    auto type = std::make_shared<DataTypeUInt64>();
    ActionsDAG dag(NamesAndTypesList{{"x", type}});
    dag.addPlaceholder("correlated", type);

    ExpressionStep step(header, std::move(dag));

    ASSERT_TRUE(step.hasCorrelatedExpressions());
    EXPECT_FALSE(step.supportsCascadesIdentity());

    auto a = std::make_shared<GroupExpression>(step.clone());
    auto b = std::make_shared<GroupExpression>(step.clone());

    EXPECT_EQ(a->cachedStepFingerprint(), nullptr);
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// AggregatingStep

/// The full digest always serializes with `for_cache_key = false`, so `final` reaches the wire
/// through the flags byte. With no aggregate functions the output header is the same for both values,
/// so this fails the moment the cache-key mode leaks into the digest.
TEST(CascadesStepIdentity, AggregatingStepDifferentFinalFlagIsUnequal)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto a_step = makeAggregatingStep(/*final=*/true);
    auto b_step = makeAggregatingStep(/*final=*/false);

    ASSERT_EQ(
        a_step->getOutputHeader()->getNamesAndTypesList().toString(),
        b_step->getOutputHeader()->getNamesAndTypesList().toString());

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `merge_threads` is not on the wire (`deserialize` passes 0 and re-derives it from a session
/// setting), but it is the parallelism of the merge stage, so the audit puts it in the extras.
TEST(CascadesStepIdentity, AggregatingStepMergeThreadsIsPartOfIdentity)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto a = std::make_shared<GroupExpression>(makeAggregatingStep(/*final=*/true, /*merge_threads=*/4));
    auto b = std::make_shared<GroupExpression>(makeAggregatingStep(/*final=*/true, /*merge_threads=*/8));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `limit_hint` is not on the wire and truncates the aggregation result where it is read.
TEST(CascadesStepIdentity, AggregatingStepLimitHintIsPartOfIdentity)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto a = std::make_shared<GroupExpression>(makeAggregatingStep(/*final=*/true, /*merge_threads=*/4, /*limit_hint=*/0));
    auto b = std::make_shared<GroupExpression>(makeAggregatingStep(/*final=*/true, /*merge_threads=*/4, /*limit_hint=*/10));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

TEST(CascadesStepIdentity, AggregatingStepIndependentlyBuiltStepsAreEqual)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto a = std::make_shared<GroupExpression>(makeAggregatingStep(/*final=*/true));
    auto b = std::make_shared<GroupExpression>(makeAggregatingStep(/*final=*/true));

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_TRUE(a->fullyEqualTo(*b));
}

/// SortingStep

TEST(CascadesStepIdentity, SortingStepClonesAreEqual)
{
    auto header = makeHeader();
    auto step = makeSortingStep(header);

    auto a = std::make_shared<GroupExpression>(step->clone());
    auto b = std::make_shared<GroupExpression>(step->clone());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_TRUE(a->fullyEqualTo(*b));
}

/// `is_partial_top_n` is deliberately not serialized - the executed sort is the same - but
/// `TwoStageTopN` builds its partial stage by cloning the sort and flipping only this flag, so
/// without it in the extras memo-wide deduplication would fold the rule's output into a self-cycle.
TEST(CascadesStepIdentity, SortingStepPartialTopNIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a_step = makeSortingStep(header);
    auto b_step = makeSortingStep(header);
    b_step->setPartialTopN();

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// The LIMIT BY hint installs a row-dropping per-stream transform (`addPerStreamLimitByIfNeeded`).
TEST(CascadesStepIdentity, SortingStepLimitByHintIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a_step = makeSortingStep(header);
    auto b_step = makeSortingStep(header);
    b_step->updateLimitByHint(Names{"x"}, 1);

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// A scattered full sort (`convertToScatteredFullSort`) is not serializable, because
/// `scatter_partitions` has no place on the wire, so it must not get an identity either.
TEST(CascadesStepIdentity, ScatteredSortingStepDoesNotSupportIdentity)
{
    auto header = makeHeader();
    auto a_step = makeSortingStep(header);
    auto b_step = makeSortingStep(header);
    a_step->convertToScatteredFullSort(4);
    b_step->convertToScatteredFullSort(4);

    ASSERT_FALSE(a_step->isSerializable());
    EXPECT_FALSE(a_step->supportsCascadesIdentity());

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_EQ(a->cachedStepFingerprint(), nullptr);
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `isSerializable()` does not cover `serializeSettings`: a sort built from `Settings(size_t)` - as
/// `optimizeGroupByTopK` builds one - has `temporary_files_buffer_size == 0`, and assigning 0 to that
/// `NonZeroUInt64` plan setting throws. The predicate must reject such an instance so that the
/// encoding never throws.
TEST(CascadesStepIdentity, SortingStepWithZeroTemporaryFilesBufferDoesNotSupportIdentity)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<SortingStep>(header, makeSortDescription(), /*limit_=*/10, SortingStep::Settings(65536));
    auto b_step = std::make_unique<SortingStep>(header, makeSortDescription(), /*limit_=*/10, SortingStep::Settings(65536));

    ASSERT_TRUE(a_step->isSerializable());
    EXPECT_FALSE(a_step->supportsCascadesIdentity());

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_EQ(a->cachedStepFingerprint(), nullptr);
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// DistinctStep

TEST(CascadesStepIdentity, DistinctStepClonesAreEqual)
{
    auto header = makeHeader();
    DistinctStep step(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false);

    auto a = std::make_shared<GroupExpression>(step.clone());
    auto b = std::make_shared<GroupExpression>(step.clone());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_TRUE(a->fullyEqualTo(*b));
}

/// `serialize` deliberately skips `limit_hint`, but both DISTINCT transforms stop consuming once that
/// many distinct rows were produced, so the audit puts it in the extras.
TEST(CascadesStepIdentity, DistinctStepLimitHintIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a = std::make_shared<GroupExpression>(
        std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false));
    auto b = std::make_shared<GroupExpression>(
        std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/10, Names{"x"}, /*pre_distinct_=*/false));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `skip_stream_merging` lets the final DISTINCT skip the resize to one stream, which is only correct
/// when the input streams hold disjoint key sets.
TEST(CascadesStepIdentity, DistinctStepSkipStreamMergingIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false);
    auto b_step = std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false);
    b_step->skipStreamMerging();

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `pre_distinct` is not a `serialize` payload: it selects the serialization name, which the identity
/// encoding writes first.
TEST(CascadesStepIdentity, PreDistinctAndDistinctAreUnequal)
{
    auto header = makeHeader();
    auto a = std::make_shared<GroupExpression>(
        std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false));
    auto b = std::make_shared<GroupExpression>(
        std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/true));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `applyOrder` installs `distinct_sort_desc` after construction; it is not on the wire but it
/// switches `transformPipeline` to the range-based `DistinctSortedStreamTransform`.
TEST(CascadesStepIdentity, DistinctStepSortDescriptionIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false);
    auto b_step = std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false);
    b_step->applyOrder(makeSortDescription());

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// LimitByStep

TEST(CascadesStepIdentity, LimitByStepClonesAreEqual)
{
    auto header = makeHeader();
    LimitByStep step(header, /*group_length_=*/1, /*group_offset_=*/0, Names{"x"});

    auto a = std::make_shared<GroupExpression>(step.clone());
    auto b = std::make_shared<GroupExpression>(step.clone());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_TRUE(a->fullyEqualTo(*b));
}

/// `serialize` writes only length, offset and columns. `skip_stream_merging` decides whether
/// `transformPipeline` resizes to one stream, which changes which rows survive.
TEST(CascadesStepIdentity, LimitByStepSkipStreamMergingIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<LimitByStep>(header, /*group_length_=*/1, /*group_offset_=*/0, Names{"x"});
    auto b_step = std::make_unique<LimitByStep>(header, /*group_length_=*/1, /*group_offset_=*/0, Names{"x"});
    b_step->skipStreamMerging();

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `sorted_columns_descr` selects the range-based `LimitBySortedStreamTransform`.
TEST(CascadesStepIdentity, LimitByStepSortedColumnsArePartOfIdentity)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<LimitByStep>(header, /*group_length_=*/1, /*group_offset_=*/0, Names{"x"});
    auto b_step = std::make_unique<LimitByStep>(header, /*group_length_=*/1, /*group_offset_=*/0, Names{"x"});
    b_step->applyOrder(makeSortDescription());

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// JoinStepLogical

TEST(CascadesStepIdentity, JoinStepLogicalIndependentlyBuiltStepsAreEqual)
{
    auto a = std::make_shared<GroupExpression>(makeJoinStepLogical());
    auto b = std::make_shared<GroupExpression>(makeJoinStepLogical());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_TRUE(a->fullyEqualTo(*b));
}

/// `optimized` is not on the wire, but `optimizeJoin` refuses to reorder a join that has it set, and
/// correlated-subquery decorrelation relies on that to pin the layout of its result join.
TEST(CascadesStepIdentity, JoinStepLogicalOptimizedFlagIsPartOfIdentity)
{
    auto a_step = makeJoinStepLogical();
    auto b_step = makeJoinStepLogical();
    b_step->setOptimized();

    ASSERT_FALSE(a_step->isOptimized());
    ASSERT_TRUE(b_step->isOptimized());

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `disjunctions_optimization_applied` is not on the wire either, and `filterPushDown` refuses to
/// push a filter through a join that has it set.
TEST(CascadesStepIdentity, JoinStepLogicalDisjunctionsFlagIsPartOfIdentity)
{
    auto a_step = makeJoinStepLogical();
    auto b_step = makeJoinStepLogical();
    b_step->setDisjunctionsOptimizationApplied(true);

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// The right-side hash table cache key reaches `JoinAlgorithmParams` and the statistics key that
/// seeds the right-side size estimate, so it is not display-only.
TEST(CascadesStepIdentity, JoinStepLogicalRightHashTableCacheKeyIsPartOfIdentity)
{
    auto a_step = makeJoinStepLogical();
    auto b_step = makeJoinStepLogical();
    b_step->setRightHashTableCacheKey(42);

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `JoinSettings::updatePlanSettings` assigns `max_block_size`, `temporary_files_codec` and
/// `temporary_files_buffer_size`, but `serializeSettings` runs `sorting_settings.updatePlanSettings`
/// afterwards and that assigns the same three plan-setting names, so the sorting values overwrite
/// them and the join's three never reach the wire. Both steps below share the same
/// `sorting_settings`, so only the extras can tell them apart.
TEST(CascadesStepIdentity, JoinStepLogicalOverwrittenJoinSettingsArePartOfIdentity)
{
    auto a_step = makeJoinStepLogical();
    auto b_step = makeJoinStepLogical();
    b_step->getJoinSettings().max_block_size = a_step->getJoinSettings().max_block_size + 1;

    /// The settings bytes really are identical: the sorting values, which win, are untouched.
    QueryPlanSerializationSettings a_settings;
    QueryPlanSerializationSettings b_settings;
    a_step->serializeSettings(a_settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    b_step->serializeSettings(b_settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    WriteBufferFromOwnString a_bytes;
    WriteBufferFromOwnString b_bytes;
    a_settings.writeChangedBinary(a_bytes);
    b_settings.writeChangedBinary(b_bytes);
    ASSERT_EQ(a_bytes.str(), b_bytes.str());

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// A correlated `PLACEHOLDER` node in the join's DAG makes `ActionsDAG::serialize` throw, so the
/// predicate must reject such an instance even though `isSerializable()` is unconditionally `true`.
TEST(CascadesStepIdentity, JoinStepLogicalWithCorrelatedExpressionsDoesNotSupportIdentity)
{
    auto a_step = makeJoinStepLogical(/*with_correlated_column=*/true);
    auto b_step = makeJoinStepLogical(/*with_correlated_column=*/true);

    ASSERT_TRUE(a_step->hasCorrelatedExpressions());
    EXPECT_FALSE(a_step->supportsCascadesIdentity());

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_EQ(a->cachedStepFingerprint(), nullptr);
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// ReadFromMergeTree

namespace
{

/// The smallest storage a `ReadFromMergeTree` can be built over: one `UInt64` column `a`, `ORDER BY a`,
/// no partition key, attached (so no sanity checks) and with no data on disk.
struct MergeTreeReadFixture
{
    ContextMutablePtr context;
    std::shared_ptr<StorageMergeTree> storage;
    StorageMetadataPtr metadata_snapshot;
    StorageSnapshotPtr storage_snapshot;
    MergeTreeSettingsPtr data_settings;
    RangesInDataPartsPtr parts;
    String relative_data_path;

    explicit MergeTreeReadFixture(const String & table_name)
        : relative_data_path("store/test_cascades_step_identity_" + table_name + "/")
    {
        MainThreadStatus::getInstance();
        tryRegisterFunctions();
        /// `getMinMaxCountProjection` below builds `min`/`max`/`count` over the partition key.
        tryRegisterAggregateFunctions();

        getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

        context = Context::createCopy(getContext().context);

        StorageInMemoryMetadata metadata;

        ColumnsDescription columns;
        columns.add(ColumnDescription("a", std::make_shared<DataTypeUInt64>()));
        metadata.setColumns(columns);

        ASTPtr order_by_ast = make_intrusive<ASTIdentifier>("a");
        metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
        metadata.primary_key = metadata.sorting_key;
        metadata.primary_key.definition_ast = nullptr;
        metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, {}, context);

        auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
        auto partition_key = metadata.partition_key.expression_list_ast->clone();
        metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            columns, partition_key, minmax_columns, metadata.primary_key, &metadata.partition_key, context));

        auto storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());
        storage = std::make_shared<StorageMergeTree>(
            StorageID("test_cascades_identity", table_name),
            relative_data_path,
            metadata,
            LoadingStrictnessLevel::ATTACH,
            context,
            /*date_column_name=*/ "",
            MergeTreeData::MergingParams{},
            std::move(storage_settings));

        /// The handle only converts to a `StorageMetadataPtr` as an lvalue.
        const StorageMetadataHandle metadata_handle = storage->getInMemoryMetadataPtr(context, false);
        metadata_snapshot = metadata_handle;
        storage_snapshot = storage->getStorageSnapshotWithoutData(metadata_snapshot, context);
        data_settings = storage->getSettings();
        parts = std::make_shared<RangesInDataParts>();
    }

    ~MergeTreeReadFixture()
    {
        /// Capture the on-disk paths before shutdown, then remove them: `StorageMergeTree` never
        /// deletes its own directory, so a bare `flushAndShutdown` leaves `relative_data_path` behind
        /// on every run.
        const auto data_paths = storage->getDataPaths();
        storage->flushAndShutdown();
        for (const auto & path : data_paths)
            std::filesystem::remove_all(path);
    }

    /// `table_expression_modifiers` must be present: with no modifiers and no query tree `isFinal()`
    /// falls back to the (absent) select AST. Note that every call allocates its own `PreparedSets`,
    /// which the full digest witnesses - reads meant to differ in one field only must share one
    /// `SelectQueryInfo` (copies share the pointer).
    static SelectQueryInfo makeQueryInfo()
    {
        SelectQueryInfo query_info;
        query_info.table_expression_modifiers.emplace(/*has_final_=*/ false, std::nullopt, std::nullopt);
        return query_info;
    }

    std::unique_ptr<ReadFromMergeTree> makeRead(const SelectQueryInfo & query_info) const
    {
        return std::make_unique<ReadFromMergeTree>(
            parts,
            MergeTreeData::MutationsSnapshotPtr{},
            Names{"a"},
            *storage,
            data_settings,
            query_info,
            storage_snapshot,
            context,
            /*max_block_size_=*/ 8192,
            /*num_streams_=*/ 1,
            /*max_block_numbers_to_read_=*/ nullptr,
            getLogger("CascadesStepIdentityTest"),
            /*analyzed_result_ptr_=*/ nullptr,
            /*enable_parallel_reading_=*/ false);
    }
};

/// A filter over an `UInt8` input column, so `applyFilters` has a non-constant node to fold into
/// `filter_actions_dag`.
ActionsDAG makeReadFilterDag()
{
    ActionsDAG dag(NamesAndTypesList{{"a", std::make_shared<DataTypeUInt64>()}, {"f", std::make_shared<DataTypeUInt8>()}});
    dag.getOutputs().push_back(&dag.findInOutputs("f"));
    return dag;
}

String encodeIdentity(const IQueryPlanStep & step)
{
    WriteBufferFromOwnString out;
    writeStepFullDigest(step, out);
    return out.str();
}

}

/// Two reads of the same table over the same snapshots: every provenance witness matches, so the
/// content-based identity holds even though the steps are distinct objects.
TEST(CascadesStepIdentity, ReadFromMergeTreeIdenticalReadsAreEqual)
{
    MergeTreeReadFixture fixture("identical");
    auto query_info = MergeTreeReadFixture::makeQueryInfo();

    auto a_step = fixture.makeRead(query_info);
    ASSERT_TRUE(a_step->supportsCascadesIdentity());

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(fixture.makeRead(query_info));

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_TRUE(a->fullyEqualTo(*b));
}

/// `SourceStepWithFilter::filter_actions_dag` is not on the wire and is its own framed component,
/// separate from the `query_info` copy that `applyFilters` leaves behind.
TEST(CascadesStepIdentity, ReadFromMergeTreeStepFilterActionsDagIsPartOfIdentity)
{
    MergeTreeReadFixture fixture("step_filter");

    auto step = fixture.makeRead(MergeTreeReadFixture::makeQueryInfo());
    step->addFilter(makeReadFilterDag(), "f");
    /// The zero-argument overload is hidden by `ReadFromMergeTree::applyFilters`; it dispatches to it.
    step->SourceStepWithFilterBase::applyFilters();

    ASSERT_NE(step->getFilterActionsDAG(), nullptr);
    ASSERT_FALSE(step->hasPendingFilters());
    ASSERT_TRUE(step->supportsCascadesIdentity());
    const auto with_step_dag = encodeIdentity(*step);

    /// Detaching clears only the step-level slot: `query_info` keeps the copy `applyFilters` made, and
    /// `indexes` is already built, so nothing else the encoding covers changes.
    auto detached = step->detachFilterActionsDAG();
    ASSERT_EQ(step->getFilterActionsDAG(), nullptr);
    ASSERT_EQ(step->getQueryInfo().filter_actions_dag, detached);

    EXPECT_NE(with_step_dag, encodeIdentity(*step));
}

/// `SelectQueryInfo::filter_actions_dag` is the DAG index analysis reads, and it is a carrier of its
/// own: two reads with no step-level DAG at all must still be unequal when it differs.
TEST(CascadesStepIdentity, ReadFromMergeTreeQueryInfoFilterActionsDagIsPartOfIdentity)
{
    MergeTreeReadFixture fixture("query_info_filter");

    const auto base_query_info = MergeTreeReadFixture::makeQueryInfo();
    auto query_info_a = base_query_info;
    auto query_info_b = base_query_info;
    query_info_a.filter_actions_dag = std::make_shared<const ActionsDAG>(makeFilterDag(1));
    query_info_b.filter_actions_dag = std::make_shared<const ActionsDAG>(makeFilterDag(2));
    /// Everything else the `query_info` provenance witness covers is shared by the two copies.
    ASSERT_EQ(query_info_a.prepared_sets, query_info_b.prepared_sets);

    auto a_step = fixture.makeRead(query_info_a);
    auto b_step = fixture.makeRead(query_info_b);
    ASSERT_EQ(a_step->getFilterActionsDAG(), nullptr);
    ASSERT_EQ(b_step->getFilterActionsDAG(), nullptr);

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->fullFingerprint(), b->fullFingerprint());
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// `join_runtime_filters_for_index_analysis` is deliberately not serialized (a worker skips this
/// pruning) but it prunes granules locally, so it must be part of the identity.
TEST(CascadesStepIdentity, ReadFromMergeTreeJoinRuntimeFilterDescriptorsArePartOfIdentity)
{
    MergeTreeReadFixture fixture("join_runtime_filter");
    auto query_info = MergeTreeReadFixture::makeQueryInfo();

    auto a_step = fixture.makeRead(query_info);
    auto b_step = fixture.makeRead(query_info);
    ASSERT_EQ(encodeIdentity(*a_step), encodeIdentity(*b_step));

    a_step->addJoinRuntimeFilterIndexAnalysisOnDataRead("filter_1", "a", std::make_shared<DataTypeUInt64>());

    EXPECT_NE(encodeIdentity(*a_step), encodeIdentity(*b_step));
}

/// A pinned analysis result *is* the read set (`getParts()` returns it), so pinning one must not
/// deduplicate against a read that analyzes on its own.
TEST(CascadesStepIdentity, ReadFromMergeTreeAnalyzedResultPinningIsPartOfIdentity)
{
    MergeTreeReadFixture fixture("analyzed_result");
    auto query_info = MergeTreeReadFixture::makeQueryInfo();

    auto a_step = fixture.makeRead(query_info);
    auto b_step = fixture.makeRead(query_info);
    ASSERT_EQ(encodeIdentity(*a_step), encodeIdentity(*b_step));

    a_step->setAnalyzedResult(std::make_shared<ReadFromMergeTree::AnalysisResult>());

    EXPECT_NE(encodeIdentity(*a_step), encodeIdentity(*b_step));
}

/// `serialize` rejects the STREAM modifier, so the predicate must reject such an instance before the
/// encoding calls `serialize`.
TEST(CascadesStepIdentity, ReadFromMergeTreeStreamReadDoesNotSupportIdentity)
{
    MergeTreeReadFixture fixture("stream");
    auto query_info = MergeTreeReadFixture::makeQueryInfo();
    query_info.table_expression_modifiers.emplace(/*has_final_=*/ false, std::nullopt, std::nullopt, StreamSettings{});

    auto a_step = fixture.makeRead(query_info);
    ASSERT_TRUE(a_step->getQueryInfo().isStream());
    EXPECT_FALSE(a_step->supportsCascadesIdentity());

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(fixture.makeRead(query_info));

    EXPECT_EQ(a->cachedStepFingerprint(), nullptr);
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// Logical digest
///
/// The logical digest answers "do these two steps compute the same relation?", so it must be blind
/// to physical knobs and sensitive to everything that changes rows or the header. Every test below
/// pins one direction of that; the physical-knob direction (logically equal while fully unequal) is
/// the one the full digest could never express.
///
/// Not covered here: the purity `chassert` in `logicallyEqualTo` / `logicalFingerprint`. It fires
/// only in debug builds and aborts the process, which gtest cannot observe; `Memo::internExpression`
/// is the caller that has to respect it.

namespace
{

/// Two expressions over independently built steps, ready for a logical comparison. Both are pure
/// logical expressions (no strategy, no enforced property), which the logical methods require.
std::pair<GroupExpressionPtr, GroupExpressionPtr> logicalPair(QueryPlanStepPtr lhs, QueryPlanStepPtr rhs)
{
    return {std::make_shared<GroupExpression>(std::move(lhs)), std::make_shared<GroupExpression>(std::move(rhs))};
}

/// The relation-defining direction: the flip must be visible to both the fingerprint and the bytes.
void expectLogicallyUnequal(const GroupExpression & a, const GroupExpression & b)
{
    EXPECT_NE(a.logicalFingerprint(), b.logicalFingerprint());
    EXPECT_FALSE(a.logicallyEqualTo(b));
    EXPECT_FALSE(b.logicallyEqualTo(a));
}

/// The knob direction: one group, two costed alternatives.
void expectLogicallyEqualButNotFully(const GroupExpression & a, const GroupExpression & b)
{
    EXPECT_EQ(a.logicalFingerprint(), b.logicalFingerprint());
    EXPECT_TRUE(a.logicallyEqualTo(b));
    EXPECT_TRUE(b.logicallyEqualTo(a));
    EXPECT_FALSE(a.fullyEqualTo(b));
}

}

TEST(CascadesStepIdentity, LogicalDigestOfClonesIsEqual)
{
    auto header = makeHeader();
    ExpressionStep step(header, makeDag(1));

    auto [a, b] = logicalPair(step.clone(), step.clone());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->logicalFingerprint(), b->logicalFingerprint());
    EXPECT_TRUE(a->logicallyEqualTo(*b));
}

TEST(CascadesStepIdentity, LogicalDigestExpressionDagIsRelationDefining)
{
    auto header = makeHeader();
    auto [a, b] = logicalPair(
        std::make_unique<ExpressionStep>(header, makeDag(1)), std::make_unique<ExpressionStep>(header, makeDag(2)));

    expectLogicallyUnequal(*a, *b);
}

/// `prevent_input_removal` only forbids a later pass from pruning this step's inputs, so it stays in
/// the full digest and out of the logical one.
TEST(CascadesStepIdentity, LogicalDigestExcludesPreventInputRemoval)
{
    auto header = makeHeader();
    auto b_step = std::make_unique<ExpressionStep>(header, makeDag(1));
    b_step->setPreventInputRemoval();

    auto [a, b] = logicalPair(std::make_unique<ExpressionStep>(header, makeDag(1)), std::move(b_step));

    expectLogicallyEqualButNotFully(*a, *b);
}

/// The `PLACEHOLDER` guard survives into the logical digest: it too serializes the DAG.
TEST(CascadesStepIdentity, LogicalDigestRejectsCorrelatedExpressions)
{
    auto header = makeHeader();
    auto type = std::make_shared<DataTypeUInt64>();
    ActionsDAG dag(NamesAndTypesList{{"x", type}});
    dag.addPlaceholder("correlated", type);

    ExpressionStep step(header, std::move(dag));
    ASSERT_TRUE(step.hasCorrelatedExpressions());
    EXPECT_FALSE(step.hasLogicalDigest());

    auto [a, b] = logicalPair(step.clone(), step.clone());

    EXPECT_EQ(a->cachedStepLogicalFingerprint(), nullptr);
    EXPECT_FALSE(a->logicallyEqualTo(*b));
}

/// A step type with no logical digest never merges - not even with itself, so that a fresh group is
/// the only possible outcome.
TEST(CascadesStepIdentity, LogicalDigestWithoutOptInNeverMerges)
{
    auto header = makeHeader();
    auto a = std::make_shared<GroupExpression>(std::make_unique<OffsetStep>(header, 10));

    EXPECT_EQ(a->cachedStepLogicalFingerprint(), nullptr);

    // NOLINTNEXTLINE(performance-unnecessary-copy-initialization) - the shallow copy is the test subject
    GroupExpression shared_step_copy(*a);
    ASSERT_EQ(shared_step_copy.plan_step, a->plan_step);
    EXPECT_FALSE(a->logicallyEqualTo(shared_step_copy));
}

/// Stage D (`ReadFromMergeTree` content identity) has not landed, so reads stay fail-closed.
TEST(CascadesStepIdentity, LogicalDigestExcludesReadFromMergeTree)
{
    MergeTreeReadFixture fixture("logical_read");
    auto query_info = MergeTreeReadFixture::makeQueryInfo();

    auto a_step = fixture.makeRead(query_info);
    ASSERT_TRUE(a_step->supportsCascadesIdentity());
    EXPECT_FALSE(a_step->hasLogicalDigest());

    auto [a, b] = logicalPair(std::move(a_step), fixture.makeRead(query_info));

    EXPECT_EQ(a->cachedStepLogicalFingerprint(), nullptr);
    EXPECT_FALSE(a->logicallyEqualTo(*b));
}

/// The expression-level frame: own properties and the ordered inputs, each with its group id and its
/// required properties.
TEST(CascadesStepIdentity, LogicalDigestFrameCoversInputsAndProperties)
{
    auto header = makeHeader();
    auto [a, b] = logicalPair(
        std::make_unique<ExpressionStep>(header, makeDag(1)), std::make_unique<ExpressionStep>(header, makeDag(1)));

    a->inputs.push_back({1, {}});
    b->inputs.push_back({1, {}});
    ASSERT_TRUE(a->logicallyEqualTo(*b));

    b->inputs[0].group_id = 2;
    expectLogicallyUnequal(*a, *b);

    b->inputs[0].group_id = 1;
    b->inputs[0].required_properties.sorting = makeSortDescription();
    expectLogicallyUnequal(*a, *b);

    b->inputs[0].required_properties = {};
    ASSERT_TRUE(a->logicallyEqualTo(*b));

    b->properties.sorting = makeSortDescription();
    expectLogicallyUnequal(*a, *b);
}

/// `description_suffix` is optimizer-side display state and deliberately not in the logical frame,
/// unlike in the full one.
TEST(CascadesStepIdentity, LogicalDigestFrameExcludesDescriptionSuffix)
{
    auto header = makeHeader();
    auto [a, b] = logicalPair(
        std::make_unique<ExpressionStep>(header, makeDag(1)), std::make_unique<ExpressionStep>(header, makeDag(1)));
    b->description_suffix = "(by k)";

    expectLogicallyEqualButNotFully(*a, *b);
}

/// FilterStep

TEST(CascadesStepIdentity, LogicalDigestFilterDagIsRelationDefining)
{
    auto header = makeHeader();
    auto [a, b] = logicalPair(
        std::make_unique<FilterStep>(header, makeFilterDag(1), "f", /*remove_filter_column_=*/false),
        std::make_unique<FilterStep>(header, makeFilterDag(2), "f", /*remove_filter_column_=*/false));

    expectLogicallyUnequal(*a, *b);
}

/// The query-condition-cache key records granules that provably match nothing, so it changes how
/// fast a later read runs and never which rows this filter emits.
TEST(CascadesStepIdentity, LogicalDigestExcludesQueryConditionCacheKey)
{
    auto header = makeHeader();
    auto b_step = std::make_unique<FilterStep>(header, makeFilterDag(1), "f", /*remove_filter_column_=*/false);
    b_step->setConditionForQueryConditionCache(42, "x > 0");

    auto [a, b] = logicalPair(
        std::make_unique<FilterStep>(header, makeFilterDag(1), "f", /*remove_filter_column_=*/false), std::move(b_step));

    expectLogicallyEqualButNotFully(*a, *b);
}

/// LimitStep

TEST(CascadesStepIdentity, LogicalDigestLimitIsRelationDefining)
{
    auto header = makeHeader();
    auto [a, b] = logicalPair(std::make_unique<LimitStep>(header, 10, 0), std::make_unique<LimitStep>(header, 20, 0));

    expectLogicallyUnequal(*a, *b);
}

/// `is_shard_limit` is the stage marker of a split limit and changes the user-visible
/// `rows_before_limit_at_least`, so it is relation-defining (plan section 4.2).
TEST(CascadesStepIdentity, LogicalDigestShardLimitMarkerIsRelationDefining)
{
    auto header = makeHeader();
    auto b_step = std::make_unique<LimitStep>(header, 10, 0);
    b_step->markAsShardLimit();

    auto [a, b] = logicalPair(std::make_unique<LimitStep>(header, 10, 0), std::move(b_step));

    expectLogicallyUnequal(*a, *b);
}

/// MergingAggregatedStep

TEST(CascadesStepIdentity, LogicalDigestExcludesMergingAggregatedThreadCounts)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto [a, b] = logicalPair(
        makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4),
        makeMergingAggregatedStep(/*max_threads=*/8, /*memory_efficient_merge_threads=*/16));

    expectLogicallyEqualButNotFully(*a, *b);
}

/// Partial versus final merge: different rows, different header (plan section 3).
TEST(CascadesStepIdentity, LogicalDigestMergingAggregatedFinalIsRelationDefining)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto [a, b] = logicalPair(
        makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4, /*final=*/true),
        makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4, /*final=*/false));

    expectLogicallyUnequal(*a, *b);
}

TEST(CascadesStepIdentity, LogicalDigestMergingAggregatedBucketTopKIsRelationDefining)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto [a, b] = logicalPair(
        makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4, /*final=*/true, /*bucket_top_k=*/0),
        makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4, /*final=*/true, /*bucket_top_k=*/100));

    expectLogicallyUnequal(*a, *b);
}

/// AggregatingStep

TEST(CascadesStepIdentity, LogicalDigestExcludesAggregatingMergeThreads)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto [a, b] = logicalPair(
        makeAggregatingStep(/*final=*/true, /*merge_threads=*/4), makeAggregatingStep(/*final=*/true, /*merge_threads=*/8));

    expectLogicallyEqualButNotFully(*a, *b);
}

/// `params.max_block_size` only splits the aggregation result into chunks.
TEST(CascadesStepIdentity, LogicalDigestExcludesAggregatingParamsMaxBlockSize)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto [a, b] = logicalPair(
        makeAggregatingStep(/*final=*/true, /*merge_threads=*/4, /*limit_hint=*/0, /*params_max_block_size=*/65536),
        makeAggregatingStep(/*final=*/true, /*merge_threads=*/4, /*limit_hint=*/0, /*params_max_block_size=*/1024));

    expectLogicallyEqualButNotFully(*a, *b);
}

/// The partial/final stage marker of an aggregation.
TEST(CascadesStepIdentity, LogicalDigestAggregatingFinalIsRelationDefining)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto a_step = makeAggregatingStep(/*final=*/true);
    auto b_step = makeAggregatingStep(/*final=*/false);
    /// With no aggregate functions the two headers agree, so only the marker can tell them apart.
    ASSERT_EQ(
        a_step->getOutputHeader()->getNamesAndTypesList().toString(),
        b_step->getOutputHeader()->getNamesAndTypesList().toString());

    auto [a, b] = logicalPair(std::move(a_step), std::move(b_step));

    expectLogicallyUnequal(*a, *b);
}

TEST(CascadesStepIdentity, LogicalDigestAggregatingLimitHintIsRelationDefining)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto [a, b] = logicalPair(
        makeAggregatingStep(/*final=*/true, /*merge_threads=*/4, /*limit_hint=*/0),
        makeAggregatingStep(/*final=*/true, /*merge_threads=*/4, /*limit_hint=*/10));

    expectLogicallyUnequal(*a, *b);
}

/// A layout-dependent flag opts the instance out until the `stream_layout` normalization lands
/// (plan section 4.2).
TEST(CascadesStepIdentity, LogicalDigestAggregatingSkipMergingOptsOut)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto a_step = makeAggregatingStep(/*final=*/true);
    ASSERT_TRUE(a_step->hasLogicalDigest());
    a_step->skipMerging();
    EXPECT_FALSE(a_step->hasLogicalDigest());

    auto b_step = makeAggregatingStep(/*final=*/true);
    b_step->skipMerging();

    auto [a, b] = logicalPair(std::move(a_step), std::move(b_step));

    EXPECT_EQ(a->cachedStepLogicalFingerprint(), nullptr);
    EXPECT_FALSE(a->logicallyEqualTo(*b));
}

/// SortingStep

/// The stage marker of a two-stage top-N: the partial stage emits up to `limit` rows per node, the
/// merged one `limit` rows in total (plan section 3).
TEST(CascadesStepIdentity, LogicalDigestPartialTopNMarkerIsRelationDefining)
{
    auto header = makeHeader();
    auto b_step = makeSortingStep(header);
    b_step->setPartialTopN();

    auto [a, b] = logicalPair(makeSortingStep(header), std::move(b_step));

    expectLogicallyUnequal(*a, *b);
}

/// `is_sorting_for_merge_join` only tells later passes what the sort is for; the executed sort and
/// its rows are the same either way.
TEST(CascadesStepIdentity, LogicalDigestExcludesIsSortingForMergeJoin)
{
    auto header = makeHeader();
    auto [a, b] = logicalPair(
        makeSortingStep(header),
        std::make_unique<SortingStep>(
            header, makeSortDescription(), /*limit_=*/10, makeSortSettings(), /*is_sorting_for_merge_join_=*/true));

    expectLogicallyEqualButNotFully(*a, *b);
}

/// A read-in-order buffering knob: it only decides how much the final merge buffers ahead.
TEST(CascadesStepIdentity, LogicalDigestExcludesSortBufferingKnob)
{
    auto header = makeHeader();
    auto buffering_settings = makeSortSettings();
    buffering_settings.read_in_order_use_buffering = !buffering_settings.read_in_order_use_buffering;

    auto [a, b] = logicalPair(
        makeSortingStep(header),
        std::make_unique<SortingStep>(header, makeSortDescription(), /*limit_=*/10, buffering_settings));

    expectLogicallyEqualButNotFully(*a, *b);
}

/// The logical digest calls neither `serialize` nor `serializeSettings`, so the whole
/// `NonZeroUInt64` guard class disappears with it: a sort built from `Settings(size_t)` - as
/// `optimizeGroupByTopK` builds one - has no full digest but does have a logical one.
TEST(CascadesStepIdentity, LogicalDigestSurvivesZeroTemporaryFilesBuffer)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<SortingStep>(header, makeSortDescription(), /*limit_=*/10, SortingStep::Settings(65536));
    auto b_step = std::make_unique<SortingStep>(header, makeSortDescription(), /*limit_=*/10, SortingStep::Settings(65536));

    ASSERT_FALSE(a_step->supportsCascadesIdentity());
    EXPECT_TRUE(a_step->hasLogicalDigest());

    auto [a, b] = logicalPair(std::move(a_step), std::move(b_step));

    EXPECT_NE(a->cachedStepLogicalFingerprint(), nullptr);
    EXPECT_EQ(a->cachedStepFingerprint(), nullptr);
    EXPECT_TRUE(a->logicallyEqualTo(*b));
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

TEST(CascadesStepIdentity, LogicalDigestSortLimitIsRelationDefining)
{
    auto header = makeHeader();
    auto b_step = makeSortingStep(header);
    b_step->updateLimit(5);

    auto [a, b] = logicalPair(makeSortingStep(header), std::move(b_step));

    expectLogicallyUnequal(*a, *b);
}

/// DistinctStep

TEST(CascadesStepIdentity, LogicalDigestDistinctLimitHintIsRelationDefining)
{
    auto header = makeHeader();
    auto [a, b] = logicalPair(
        std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false),
        std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/10, Names{"x"}, /*pre_distinct_=*/false));

    expectLogicallyUnequal(*a, *b);
}

/// `serializeSettings` is what carries the DISTINCT size limits on the wire, and the logical digest
/// does not call it - so the logical writer has to write them itself: `break` truncates the result.
TEST(CascadesStepIdentity, LogicalDigestDistinctSizeLimitsAreRelationDefining)
{
    auto header = makeHeader();
    SizeLimits truncating(/*max_rows=*/100, /*max_bytes=*/0, OverflowMode::BREAK);

    auto [a, b] = logicalPair(
        std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false),
        std::make_unique<DistinctStep>(header, truncating, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false));

    expectLogicallyUnequal(*a, *b);
}

TEST(CascadesStepIdentity, LogicalDigestDistinctSkipStreamMergingOptsOut)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false);
    ASSERT_TRUE(a_step->hasLogicalDigest());
    a_step->skipStreamMerging();
    EXPECT_FALSE(a_step->hasLogicalDigest());

    auto b_step = std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false);
    b_step->skipStreamMerging();

    auto [a, b] = logicalPair(std::move(a_step), std::move(b_step));

    EXPECT_EQ(a->cachedStepLogicalFingerprint(), nullptr);
    EXPECT_FALSE(a->logicallyEqualTo(*b));
}

/// LimitByStep

TEST(CascadesStepIdentity, LogicalDigestLimitByGroupLengthIsRelationDefining)
{
    auto header = makeHeader();
    auto [a, b] = logicalPair(
        std::make_unique<LimitByStep>(header, /*group_length_=*/1, /*group_offset_=*/0, Names{"x"}),
        std::make_unique<LimitByStep>(header, /*group_length_=*/2, /*group_offset_=*/0, Names{"x"}));

    expectLogicallyUnequal(*a, *b);
}

TEST(CascadesStepIdentity, LogicalDigestLimitBySkipStreamMergingOptsOut)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<LimitByStep>(header, /*group_length_=*/1, /*group_offset_=*/0, Names{"x"});
    ASSERT_TRUE(a_step->hasLogicalDigest());
    a_step->skipStreamMerging();
    EXPECT_FALSE(a_step->hasLogicalDigest());

    auto b_step = std::make_unique<LimitByStep>(header, /*group_length_=*/1, /*group_offset_=*/0, Names{"x"});
    b_step->skipStreamMerging();

    auto [a, b] = logicalPair(std::move(a_step), std::move(b_step));

    EXPECT_EQ(a->cachedStepLogicalFingerprint(), nullptr);
    EXPECT_FALSE(a->logicallyEqualTo(*b));
}

/// JoinStepLogical

TEST(CascadesStepIdentity, LogicalDigestJoinIndependentlyBuiltStepsAreEqual)
{
    auto [a, b] = logicalPair(makeJoinStepLogical(), makeJoinStepLogical());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->logicalFingerprint(), b->logicalFingerprint());
    EXPECT_TRUE(a->logicallyEqualTo(*b));
}

/// Join planner bookkeeping informs later passes and does not change the join's rows, so both
/// variants belong in one group.
TEST(CascadesStepIdentity, LogicalDigestExcludesJoinPlannerBookkeeping)
{
    auto b_step = makeJoinStepLogical();
    b_step->setOptimized(/*estimated_rows_=*/1000);
    b_step->setDisjunctionsOptimizationApplied(true);
    b_step->setRightHashTableCacheKey(42);
    b_step->setTableStatsHint("t1:1000");

    auto [a, b] = logicalPair(makeJoinStepLogical(), std::move(b_step));

    expectLogicallyEqualButNotFully(*a, *b);
}

/// The join's own three plan settings never reach the wire (the sorting settings overwrite them),
/// and they only matter when an algorithm spills.
TEST(CascadesStepIdentity, LogicalDigestExcludesJoinBlockSize)
{
    auto b_step = makeJoinStepLogical();
    b_step->getJoinSettings().max_block_size += 1;

    auto [a, b] = logicalPair(makeJoinStepLogical(), std::move(b_step));

    expectLogicallyEqualButNotFully(*a, *b);
}

/// `join_any_take_last_row` picks the other row for an `ANY` join.
TEST(CascadesStepIdentity, LogicalDigestJoinAnyTakeLastRowIsRelationDefining)
{
    auto b_step = makeJoinStepLogical();
    b_step->getJoinSettings().join_any_take_last_row = !b_step->getJoinSettings().join_any_take_last_row;

    auto [a, b] = logicalPair(makeJoinStepLogical(), std::move(b_step));

    expectLogicallyUnequal(*a, *b);
}

TEST(CascadesStepIdentity, LogicalDigestJoinRejectsCorrelatedExpressions)
{
    auto a_step = makeJoinStepLogical(/*with_correlated_column=*/true);
    ASSERT_TRUE(a_step->hasCorrelatedExpressions());
    EXPECT_FALSE(a_step->hasLogicalDigest());

    auto [a, b] = logicalPair(std::move(a_step), makeJoinStepLogical(/*with_correlated_column=*/true));

    EXPECT_EQ(a->cachedStepLogicalFingerprint(), nullptr);
    EXPECT_FALSE(a->logicallyEqualTo(*b));
}

/// The logical passes are counted like the full ones - they cost the optimizer the same way.
TEST(CascadesStepIdentity, LogicalDigestCountersCountDigestPasses)
{
    auto header = makeHeader();
    auto [a, b] = logicalPair(
        std::make_unique<ExpressionStep>(header, makeDag(1)), std::make_unique<ExpressionStep>(header, makeDag(1)));

    StepDigestCounters counters;
    CurrentStepDigestCounters counters_scope(counters);

    a->logicalFingerprint();
    EXPECT_EQ(counters.digests_written, 1u);
    EXPECT_GT(counters.digest_bytes_written, 0u);
    EXPECT_EQ(counters.digest_confirmations, 0u);

    EXPECT_TRUE(a->logicallyEqualTo(*b));
    /// One pass fingerprints `b`, then both steps are re-digested to be compared byte for byte.
    EXPECT_EQ(counters.digest_confirmations, 2u);
    EXPECT_EQ(counters.digests_written, 4u);
}

/// Logical digest: an excluded knob that gates a relation-defining field
///
/// The one trap of the two-level design: a field is only relation-defining if something the digest
/// keeps decides whether it is read. Both cases below are resolved by opting the instance out, not
/// by pulling the knob into the digest - so the untruncated variants still merge with each other,
/// which is what excluding the knob was for.

TEST(CascadesStepIdentity, LogicalDigestMergingAggregatedTruncationOptsOut)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    /// `Aggregator::mergeBlocks(AggregatedChunks &, ...)` - the memory-efficient path - never calls
    /// `checkLimits` and never reaches the `bucket_top_k` branch of `convertOneBucketToChunk`, while
    /// the plain path applies both. So either truncation makes the excluded
    /// `memory_efficient_aggregation` row-visible, and the instance must not participate.
    auto rows_limited = makeMergingAggregatedVariant({.limits = AggregationLimits{.max_rows_to_group_by = 100}});
    auto bucket_truncated
        = makeMergingAggregatedVariant({.tweak_params = [](Aggregator::Params & params) { params.bucket_top_k = 100; }});

    EXPECT_TRUE(makeMergingAggregatedVariant({})->hasLogicalDigest());
    EXPECT_FALSE(rows_limited->hasLogicalDigest());
    EXPECT_FALSE(bucket_truncated->hasLogicalDigest());

    /// Both flips therefore also fail the comparison - no truncating merge step ever merges.
    auto [plain, limited] = logicalPair(makeMergingAggregatedVariant({.limits = AggregationLimits{}}), std::move(rows_limited));
    expectLogicallyUnequal(*plain, *limited);

    auto [plain_2, truncated] = logicalPair(makeMergingAggregatedVariant({}), std::move(bucket_truncated));
    expectLogicallyUnequal(*plain_2, *truncated);
}

/// The payoff of the gate: with no truncation configured, the merge strategy itself is free.
TEST(CascadesStepIdentity, LogicalDigestExcludesMemoryEfficientMergeStrategy)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto [a, b] = logicalPair(
        makeMergingAggregatedVariant({.memory_efficient_aggregation = false}),
        makeMergingAggregatedVariant({.memory_efficient_aggregation = true}));

    expectLogicallyEqualButNotFully(*a, *b);
}

/// Same shape on `AggregatingStep`: `bucket_top_k` fires in `convertOneBucketToChunk`, which runs on
/// two-level data only, and the two-level thresholds are execution-only and excluded.
TEST(CascadesStepIdentity, LogicalDigestAggregatingBucketTopKOptsOut)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto truncated = makeAggregatingVariant({.tweak_params = [](Aggregator::Params & params) { params.bucket_top_k = 100; }});

    EXPECT_TRUE(makeAggregatingVariant({})->hasLogicalDigest());
    EXPECT_FALSE(truncated->hasLogicalDigest());

    auto [a, b] = logicalPair(makeAggregatingVariant({}), std::move(truncated));
    expectLogicallyUnequal(*a, *b);
}

/// Logical digest: one pin per relation-defining field
///
/// Every field the audits classify as IN gets a flip here, so a later edit that quietly drops one
/// from a writer fails a test instead of silently widening group membership.

TEST(CascadesStepIdentity, LogicalDigestFilterColumnAndItsRemovalAreRelationDefining)
{
    auto header = makeHeader();
    /// Two constant `UInt8` outputs, so the two steps can name different filter columns over one DAG.
    auto two_condition_dag = [] {
        auto type = std::make_shared<DataTypeUInt64>();
        auto filter_type = std::make_shared<DataTypeUInt8>();
        ActionsDAG dag(NamesAndTypesList{{"x", type}});
        dag.getOutputs().push_back(&dag.addColumn(filter_type->createColumnConst(1, Field(static_cast<UInt8>(1))), filter_type, "f"));
        dag.getOutputs().push_back(&dag.addColumn(filter_type->createColumnConst(1, Field(static_cast<UInt8>(1))), filter_type, "g"));
        return dag;
    };

    auto [named_f, named_g] = logicalPair(
        std::make_unique<FilterStep>(header, two_condition_dag(), "f", /*remove_filter_column_=*/false),
        std::make_unique<FilterStep>(header, two_condition_dag(), "g", /*remove_filter_column_=*/false));
    expectLogicallyUnequal(*named_f, *named_g);

    auto [kept, removed] = logicalPair(
        std::make_unique<FilterStep>(header, makeFilterDag(1), "f", /*remove_filter_column_=*/false),
        std::make_unique<FilterStep>(header, makeFilterDag(1), "f", /*remove_filter_column_=*/true));
    expectLogicallyUnequal(*kept, *removed);
}

TEST(CascadesStepIdentity, LogicalDigestLimitWindowFieldsAreRelationDefining)
{
    auto header = makeHeader();

    auto [no_offset, offset] = logicalPair(std::make_unique<LimitStep>(header, 10, 0), std::make_unique<LimitStep>(header, 10, 5));
    expectLogicallyUnequal(*no_offset, *offset);

    auto [plain, with_ties] = logicalPair(
        std::make_unique<LimitStep>(header, 10, 0),
        std::make_unique<LimitStep>(header, 10, 0, /*always_read_till_end_=*/false, /*with_ties_=*/true, makeSortDescription()));
    expectLogicallyUnequal(*plain, *with_ties);

    /// The tie description on its own, with `with_ties` equal on both sides.
    auto [no_desc, desc] = logicalPair(
        std::make_unique<LimitStep>(header, 10, 0, /*always_read_till_end_=*/false, /*with_ties_=*/true, SortDescription{}),
        std::make_unique<LimitStep>(header, 10, 0, /*always_read_till_end_=*/false, /*with_ties_=*/true, makeSortDescription()));
    expectLogicallyUnequal(*no_desc, *desc);

    /// Keeps the input running past the limit, which is what makes `totals` see every row.
    auto [stops, drains] = logicalPair(
        std::make_unique<LimitStep>(header, 10, 0, /*always_read_till_end_=*/false),
        std::make_unique<LimitStep>(header, 10, 0, /*always_read_till_end_=*/true));
    expectLogicallyUnequal(*stops, *drains);
}

TEST(CascadesStepIdentity, LogicalDigestSortShapeIsRelationDefining)
{
    auto header = makeHeader();

    /// `type`: both sides have an empty prefix, so only the shape differs.
    auto [full, finish] = logicalPair(makeSortingStep(header), makeFinishSortingStep(header));
    expectLogicallyUnequal(*full, *finish);

    /// The assumed input order of a `FinishSorting`.
    auto [no_prefix, with_prefix] = logicalPair(
        makeFinishSortingStep(header, /*always_read_till_end=*/false, SortDescription{}),
        makeFinishSortingStep(header, /*always_read_till_end=*/false, makeSortDescription()));
    expectLogicallyUnequal(*no_prefix, *with_prefix);

    /// A partitioned sort sorts each partition alone; under a limit that changes which rows survive.
    auto [unpartitioned, partitioned] = logicalPair(makeSortingStep(header), makePartitionedSortingStep(header, makeSortDescription()));
    expectLogicallyUnequal(*unpartitioned, *partitioned);

    /// And skipping the scatter asserts the input is already split that way.
    auto skipping = makePartitionedSortingStep(header, makeSortDescription());
    skipping->skipScatterByPartition();
    auto [scattering, not_scattering] = logicalPair(makePartitionedSortingStep(header, makeSortDescription()), std::move(skipping));
    expectLogicallyUnequal(*scattering, *not_scattering);
}

TEST(CascadesStepIdentity, LogicalDigestSortDrainAndVirtualRowsAreRelationDefining)
{
    auto header = makeHeader();

    auto [stops, drains] = logicalPair(
        makeFinishSortingStep(header, /*always_read_till_end=*/false), makeFinishSortingStep(header, /*always_read_till_end=*/true));
    expectLogicallyUnequal(*stops, *drains);

    /// Decides whether a `RemoveVirtualRowTransform` strips the read-in-order marker rows.
    auto [keeps, converts] = logicalPair(
        makeFinishSortingStep(header, /*always_read_till_end=*/false, SortDescription{}, /*apply_virtual_row_conversions=*/false),
        makeFinishSortingStep(header, /*always_read_till_end=*/false, SortDescription{}, /*apply_virtual_row_conversions=*/true));
    expectLogicallyUnequal(*keeps, *converts);
}

/// `serializeSettings` carries the sort size limits on the wire, which the logical digest does not
/// call - so the logical writer has to write them itself: `break` truncates, `throw` fails the query.
TEST(CascadesStepIdentity, LogicalDigestSortSizeLimitsAreRelationDefining)
{
    auto header = makeHeader();
    auto sort_with = [&header](const SizeLimits & size_limits)
    {
        auto settings = makeSortSettings();
        settings.size_limits = size_limits;
        return std::make_unique<SortingStep>(header, makeSortDescription(), /*limit_=*/10, settings);
    };

    auto [unlimited, row_limited] = logicalPair(sort_with(SizeLimits{}), sort_with(SizeLimits(100, 0, OverflowMode::THROW)));
    expectLogicallyUnequal(*unlimited, *row_limited);

    auto [unlimited_2, byte_limited] = logicalPair(sort_with(SizeLimits{}), sort_with(SizeLimits(0, 4096, OverflowMode::THROW)));
    expectLogicallyUnequal(*unlimited_2, *byte_limited);

    auto [throwing, breaking]
        = logicalPair(sort_with(SizeLimits(100, 0, OverflowMode::THROW)), sort_with(SizeLimits(100, 0, OverflowMode::BREAK)));
    expectLogicallyUnequal(*throwing, *breaking);
}

TEST(CascadesStepIdentity, LogicalDigestAggregatingParamsAreRelationDefining)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    /// `only_merge` is true out of the merge-only `Params` ctor; the projection path is what flips it.
    auto [merging, aggregating] = logicalPair(
        makeAggregatingVariant({}), makeAggregatingVariant({.tweak_params = [](Aggregator::Params & p) { p.only_merge = false; }}));
    expectLogicallyUnequal(*merging, *aggregating);

    auto [no_overflow, overflow] = logicalPair(
        makeAggregatingVariant({.limits = AggregationLimits{}}),
        makeAggregatingVariant({.limits = AggregationLimits{.overflow_row = true}}));
    expectLogicallyUnequal(*no_overflow, *overflow);

    auto [one_row, no_row] = logicalPair(
        makeAggregatingVariant({}),
        makeAggregatingVariant({.tweak_params = [](Aggregator::Params & p) { p.empty_result_for_aggregation_by_empty_set = true; }}));
    expectLogicallyUnequal(*one_row, *no_row);

    /// `checkLimits` runs on the aggregation path regardless of the hash-table method, so unlike on
    /// `MergingAggregatedStep` this pair is a digest difference, not a predicate opt-out.
    auto limited = makeAggregatingVariant({.limits = AggregationLimits{.max_rows_to_group_by = 100}});
    ASSERT_TRUE(limited->hasLogicalDigest());
    auto [unlimited, row_limited] = logicalPair(makeAggregatingVariant({.limits = AggregationLimits{}}), std::move(limited));
    expectLogicallyUnequal(*unlimited, *row_limited);

    auto [throwing, breaking] = logicalPair(
        makeAggregatingVariant({.limits = AggregationLimits{.max_rows_to_group_by = 100}}),
        makeAggregatingVariant(
            {.limits = AggregationLimits{.max_rows_to_group_by = 100, .group_by_overflow_mode = OverflowMode::BREAK}}));
    expectLogicallyUnequal(*throwing, *breaking);
}

TEST(CascadesStepIdentity, LogicalDigestAggregatingKeyShapeAndOrderClaimAreRelationDefining)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto [plain_keys, null_keys] = logicalPair(makeAggregatingVariant({}), makeAggregatingVariant({.group_by_use_nulls = true}));
    expectLogicallyUnequal(*plain_keys, *null_keys);

    auto [unordered, ordered]
        = logicalPair(makeAggregatingVariant({}), makeAggregatingVariant({.group_by_sort_description = makeSortDescription()}));
    expectLogicallyUnequal(*unordered, *ordered);

    auto [any_order, bucket_order]
        = logicalPair(makeAggregatingVariant({}), makeAggregatingVariant({.results_in_bucket_order = true}));
    expectLogicallyUnequal(*any_order, *bucket_order);

    auto [plain_merge, bound_merge] = logicalPair(makeAggregatingVariant({}), makeAggregatingVariant({.memory_bound_merging = true}));
    expectLogicallyUnequal(*plain_merge, *bound_merge);
}

TEST(CascadesStepIdentity, LogicalDigestMergingAggregatedParamsAndOrderClaimAreRelationDefining)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto [no_overflow, overflow] = logicalPair(
        makeMergingAggregatedVariant({.limits = AggregationLimits{}}),
        makeMergingAggregatedVariant({.limits = AggregationLimits{.overflow_row = true}}));
    expectLogicallyUnequal(*no_overflow, *overflow);

    auto [one_row, no_row] = logicalPair(
        makeMergingAggregatedVariant({}),
        makeMergingAggregatedVariant({.tweak_params = [](Aggregator::Params & p) { p.empty_result_for_aggregation_by_empty_set = true; }}));
    expectLogicallyUnequal(*one_row, *no_row);

    auto [unordered, ordered] = logicalPair(
        makeMergingAggregatedVariant({}), makeMergingAggregatedVariant({.group_by_sort_description = makeSortDescription()}));
    expectLogicallyUnequal(*unordered, *ordered);

    auto [any_order, bucket_order]
        = logicalPair(makeMergingAggregatedVariant({}), makeMergingAggregatedVariant({.results_in_bucket_order = true}));
    expectLogicallyUnequal(*any_order, *bucket_order);

    auto [plain_merge, bound_merge]
        = logicalPair(makeMergingAggregatedVariant({}), makeMergingAggregatedVariant({.memory_bound_merging = true}));
    expectLogicallyUnequal(*plain_merge, *bound_merge);
}

/// `applyOrder` installs it after construction; it is what `getSortDescription` reports and it
/// selects `DistinctSortedStreamTransform`, which is correct only for an input sorted this way.
TEST(CascadesStepIdentity, LogicalDigestDistinctSortDescriptionIsRelationDefining)
{
    auto header = makeHeader();
    auto ordered = std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false);
    ordered->applyOrder(makeSortDescription());

    auto [a, b] = logicalPair(
        std::make_unique<DistinctStep>(header, SizeLimits{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false), std::move(ordered));

    expectLogicallyUnequal(*a, *b);
}

/// Selects `LimitBySortedStreamTransform`, i.e. it asserts the input arrives grouped in this order.
TEST(CascadesStepIdentity, LogicalDigestLimitBySortedColumnsAreRelationDefining)
{
    auto header = makeHeader();
    auto ordered = std::make_unique<LimitByStep>(header, /*group_length_=*/1, /*group_offset_=*/0, Names{"x"});
    ordered->applyOrder(makeSortDescription());

    auto [a, b] = logicalPair(
        std::make_unique<LimitByStep>(header, /*group_length_=*/1, /*group_offset_=*/0, Names{"x"}), std::move(ordered));

    expectLogicallyUnequal(*a, *b);
}

TEST(CascadesStepIdentity, LogicalDigestJoinSizeLimitsAreRelationDefining)
{
    auto with_settings = [](auto && tweak)
    {
        auto step = makeJoinStepLogical();
        tweak(step->getJoinSettings());
        return step;
    };

    auto [a, rows] = logicalPair(makeJoinStepLogical(), with_settings([](JoinSettings & s) { s.max_rows_in_join = 100; }));
    expectLogicallyUnequal(*a, *rows);

    auto [b, bytes] = logicalPair(makeJoinStepLogical(), with_settings([](JoinSettings & s) { s.max_bytes_in_join = 4096; }));
    expectLogicallyUnequal(*b, *bytes);

    auto [c, default_bytes]
        = logicalPair(makeJoinStepLogical(), with_settings([](JoinSettings & s) { s.default_max_bytes_in_join = 4096; }));
    expectLogicallyUnequal(*c, *default_bytes);

    auto [d, breaking]
        = logicalPair(makeJoinStepLogical(), with_settings([](JoinSettings & s) { s.join_overflow_mode = OverflowMode::BREAK; }));
    expectLogicallyUnequal(*d, *breaking);

    /// A permission flag, but it decides between a result and an exception - kept in, fail-closed.
    auto [e, dynamic_keys] = logicalPair(
        makeJoinStepLogical(),
        with_settings([](JoinSettings & s) { s.allow_dynamic_type_in_join_keys = !s.allow_dynamic_type_in_join_keys; }));
    expectLogicallyUnequal(*e, *dynamic_keys);
}

/// `joinRuntimeFilter` sets these, and they are what makes `HashJoin` publish filters that prune the
/// other side at all - kept in, matching the read side of plan section 8.
TEST(CascadesStepIdentity, LogicalDigestJoinRuntimeFilterDescriptorsAreRelationDefining)
{
    auto filtering = makeJoinStepLogical();
    filtering->getJoinOperator().shared_runtime_filter_descriptors.emplace_back("filter_1", "l");

    auto [a, b] = logicalPair(makeJoinStepLogical(), std::move(filtering));

    expectLogicallyUnequal(*a, *b);
}
