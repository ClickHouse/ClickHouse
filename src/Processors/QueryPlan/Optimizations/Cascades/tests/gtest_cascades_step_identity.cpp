#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StepIdentity.h>
#include <Processors/QueryPlan/StepIdentity.h>
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
Aggregator::Params makeMergingAggregatedParams(size_t max_threads)
{
    return Aggregator::Params(
        Names{"k"},
        AggregateDescriptions{},
        /*overflow_row=*/false,
        max_threads,
        /*max_block_size=*/65536,
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

}

TEST(CascadesStepIdentity, ClonesOfSameStepAreEqual)
{
    auto header = makeHeader();
    ExpressionStep step(header, makeDag(1));

    auto a = std::make_shared<GroupExpression>(step.clone());
    auto b = std::make_shared<GroupExpression>(step.clone());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_TRUE(a->globallyEqualTo(*b));
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
    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
}

TEST(CascadesStepIdentity, StepDescriptionIsExcluded)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<ExpressionStep>(header, makeDag(1));
    auto b_step = std::make_unique<ExpressionStep>(header, makeDag(1));
    b_step->setStepDescription("projection over a very different reason");

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_EQ(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_TRUE(a->globallyEqualTo(*b));
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

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
}

/// `Group` uses `enforced_property` to keep a self-referential enforcer from satisfying its own
/// input, so an enforcer and a plain expression over the same step must never be judged equal.
TEST(CascadesStepIdentity, EnforcedPropertyIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    auto b = exprWithExpressionStep(header, 1);
    ASSERT_TRUE(a->globallyEqualTo(*b));

    b->enforced_property = EnforcedProperty::Sorting;

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
}

/// `description_suffix` is GroupExpression state set by rules (e.g. "(by col)"), not the step's
/// display description, and nothing guarantees it is free of meaning - so it is compared.
TEST(CascadesStepIdentity, DescriptionSuffixIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    auto b = exprWithExpressionStep(header, 1);
    ASSERT_TRUE(a->globallyEqualTo(*b));

    b->description_suffix = "(by k)";

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
}

/// The hash must be usable before any equality call, so that a memo can bucket expressions eagerly.
TEST(CascadesStepIdentity, IndependentlyBuiltStepsShareFingerprint)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 7);
    auto b = exprWithExpressionStep(header, 7);

    EXPECT_EQ(a->globalFingerprint(), b->globalFingerprint());
}

TEST(CascadesStepIdentity, StepWithoutOptInComparesByPointer)
{
    auto header = makeHeader();
    auto a = std::make_shared<GroupExpression>(std::make_unique<OffsetStep>(header, 10));
    auto b = std::make_shared<GroupExpression>(std::make_unique<OffsetStep>(header, 10));

    EXPECT_EQ(a->getStepIdentity(), nullptr);
    /// Equal-looking but distinct instances are not interchangeable without a field audit.
    EXPECT_FALSE(a->globallyEqualTo(*b));

    // NOLINTNEXTLINE(performance-unnecessary-copy-initialization) - the shallow copy is the test subject
    GroupExpression shared_step_copy(*a);
    EXPECT_EQ(shared_step_copy.plan_step, a->plan_step);
    EXPECT_TRUE(a->globallyEqualTo(shared_step_copy));
    EXPECT_EQ(a->globalFingerprint(), shared_step_copy.globalFingerprint());
}

/// A rule may shallow-copy an expression and then replace its step. The inherited identity cache
/// must not be trusted for the new step.
TEST(CascadesStepIdentity, ReplacedStepInvalidatesInheritedIdentity)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    const auto fingerprint_a = a->globalFingerprint();

    auto b = std::make_shared<GroupExpression>(*a);
    ASSERT_EQ(b->globalFingerprint(), fingerprint_a);

    b->plan_step = std::make_shared<const ExpressionStep>(header, makeDag(2));

    EXPECT_NE(b->globalFingerprint(), fingerprint_a);
    EXPECT_FALSE(a->globallyEqualTo(*b));
    EXPECT_FALSE(b->globallyEqualTo(*a));
}

/// Moving a value between tags, or re-splitting two adjacent variable-length components, must
/// change the bytes - otherwise different field assignments could collide.
TEST(CascadesStepIdentity, ExtrasFramingIsInjective)
{
    SerializedSetsRegistry registry;
    auto dag = makeDag(1);

    WriteBufferFromOwnString dag_first;
    {
        CascadesIdentityExtras extras(dag_first, registry);
        extras.addDAG(1, &dag);
        extras.addAbsent(2);
    }

    WriteBufferFromOwnString dag_second;
    {
        CascadesIdentityExtras extras(dag_second, registry);
        extras.addAbsent(1);
        extras.addDAG(2, &dag);
    }

    EXPECT_NE(dag_first.str(), dag_second.str());

    WriteBufferFromOwnString split_left;
    {
        CascadesIdentityExtras extras(split_left, registry);
        extras.addStrings(1, Names{"a", "b"});
        extras.addStrings(2, Names{});
    }

    WriteBufferFromOwnString split_right;
    {
        CascadesIdentityExtras extras(split_right, registry);
        extras.addStrings(1, Names{"a"});
        extras.addStrings(2, Names{"b"});
    }

    EXPECT_NE(split_left.str(), split_right.str());
}

TEST(CascadesStepIdentity, MetricsCountEncodingPasses)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    auto b = exprWithExpressionStep(header, 1);

    CascadesIdentityMetrics::reset();

    a->globalFingerprint();
    EXPECT_EQ(CascadesIdentityMetrics::encoded_steps.load(), 1u);
    EXPECT_GT(CascadesIdentityMetrics::encoded_bytes.load(), 0u);
    EXPECT_EQ(CascadesIdentityMetrics::exact_reencodes.load(), 0u);

    EXPECT_TRUE(a->globallyEqualTo(*b));
    /// One pass hashes `b`, then both steps are re-encoded to be compared byte for byte.
    EXPECT_EQ(CascadesIdentityMetrics::exact_reencodes.load(), 2u);
    EXPECT_EQ(CascadesIdentityMetrics::encoded_steps.load(), 4u);
}

/// FilterStep

TEST(CascadesStepIdentity, FilterStepClonesAreEqual)
{
    auto header = makeHeader();
    FilterStep step(header, makeFilterDag(1), "f", /*remove_filter_column_=*/false);

    auto a = std::make_shared<GroupExpression>(step.clone());
    auto b = std::make_shared<GroupExpression>(step.clone());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_TRUE(a->globallyEqualTo(*b));
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

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
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

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
}

TEST(CascadesStepIdentity, FilterStepDifferentDagContentIsUnequal)
{
    auto header = makeHeader();
    auto a = std::make_shared<GroupExpression>(
        std::make_unique<FilterStep>(header, makeFilterDag(1), "f", /*remove_filter_column_=*/false));
    auto b = std::make_shared<GroupExpression>(
        std::make_unique<FilterStep>(header, makeFilterDag(2), "f", /*remove_filter_column_=*/false));

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
}

/// LimitStep

TEST(CascadesStepIdentity, LimitStepClonesAreEqual)
{
    auto header = makeHeader();
    LimitStep step(header, 10, 0);

    auto a = std::make_shared<GroupExpression>(step.clone());
    auto b = std::make_shared<GroupExpression>(step.clone());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_TRUE(a->globallyEqualTo(*b));
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

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
}

TEST(CascadesStepIdentity, LimitStepDifferentLimitIsUnequal)
{
    auto header = makeHeader();
    auto a = std::make_shared<GroupExpression>(std::make_unique<LimitStep>(header, 10, 0));
    auto b = std::make_shared<GroupExpression>(std::make_unique<LimitStep>(header, 20, 0));

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
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
    EXPECT_EQ(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_TRUE(a->globallyEqualTo(*b));
}

/// `max_threads` is not on the wire - `deserialize` re-derives it from the session setting - but it
/// controls how far `transformPipeline` resizes the pipeline, so the audit puts it in the extras.
TEST(CascadesStepIdentity, MergingAggregatedStepMaxThreadsIsPartOfIdentity)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto a = std::make_shared<GroupExpression>(makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4));
    auto b = std::make_shared<GroupExpression>(makeMergingAggregatedStep(/*max_threads=*/8, /*memory_efficient_merge_threads=*/4));

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
}

TEST(CascadesStepIdentity, MergingAggregatedStepDifferentFinalFlagIsUnequal)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto a = std::make_shared<GroupExpression>(
        makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4, /*final=*/true));
    auto b = std::make_shared<GroupExpression>(
        makeMergingAggregatedStep(/*max_threads=*/4, /*memory_efficient_merge_threads=*/4, /*final=*/false));

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
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

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
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

    EXPECT_EQ(a->getStepIdentity(), nullptr);
    EXPECT_FALSE(a->globallyEqualTo(*b));
}
