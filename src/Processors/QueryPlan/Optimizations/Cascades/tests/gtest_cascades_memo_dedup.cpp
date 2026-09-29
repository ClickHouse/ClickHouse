#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/tests/gtest_merge_tree_read_fixture.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

/// Memo-wide group deduplication (`Memo::internExpression`, `cascades_memo_deduplication`): one
/// group per distinct logical relation. The identity itself is pinned in
/// `gtest_cascades_step_identity.cpp`; these tests pin what the memo does with it - which
/// expressions merge, which get a fresh group, and that no group is left unreachable.

namespace
{

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "x")}));
}

/// Input `x` plus a constant output `c`, so two DAGs differing only in `constant` share the output
/// header and differ only in the serialized payload.
ActionsDAG makeDag(UInt64 constant)
{
    auto type = std::make_shared<DataTypeUInt64>();
    ActionsDAG dag(NamesAndTypesList{{"x", type}});
    dag.getOutputs().push_back(&dag.addColumn(type->createColumnConst(1, Field(constant)), type, "c"));
    return dag;
}

GroupExpressionPtr expressionOver(QueryPlanStepPtr step, std::vector<GroupId> input_group_ids = {})
{
    auto expression = std::make_shared<GroupExpression>(std::move(step));
    for (GroupId input_group_id : input_group_ids)
        expression->inputs.push_back({.group_id = input_group_id, .required_properties = {}});
    return expression;
}

/// A projection over the read fixture's column `a`, so an `ExpressionStep` can sit on top of a read.
ActionsDAG readDag()
{
    auto type = std::make_shared<DataTypeUInt64>();
    ActionsDAG dag(NamesAndTypesList{{"a", type}});
    dag.getOutputs().push_back(&dag.addColumn(type->createColumnConst(1, Field(UInt64(1))), type, "c"));
    return dag;
}

GroupExpressionPtr projection(const SharedHeader & header, UInt64 constant)
{
    return expressionOver(std::make_unique<ExpressionStep>(header, makeDag(constant)));
}

Memo makeMemo(bool deduplicate)
{
    Memo memo(getLogger("CascadesMemoDedupTest"));
    memo.getContext().cascades_memo_deduplication = deduplicate;
    return memo;
}

/// Merge-only `Aggregator::Params` with no aggregate functions: the output header is the key list
/// for both values of `final`, so flipping the stage marker really varies nothing else.
Aggregator::Params makeAggregationParams(size_t max_threads = 4)
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

SharedHeader makeAggregatedHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

std::unique_ptr<AggregatingStep> makeAggregatingStep(bool final, size_t merge_threads = 4)
{
    return std::make_unique<AggregatingStep>(
        makeAggregatedHeader(),
        makeAggregationParams(),
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
        /*explicit_sorting_required_for_aggregation_in_order=*/false);
}

std::unique_ptr<MergingAggregatedStep> makeMergingAggregatedStep()
{
    return std::make_unique<MergingAggregatedStep>(
        makeAggregatedHeader(),
        makeAggregationParams(),
        GroupingSetsParamsList{},
        /*final_=*/true,
        /*memory_efficient_aggregation=*/false,
        /*memory_efficient_merge_threads=*/4,
        /*should_produce_results_in_order_of_bucket_number=*/false,
        /*max_block_size=*/65536,
        /*memory_bound_merging_max_block_bytes=*/0,
        /*memory_bound_merging_of_aggregation_results_enabled=*/false);
}

/// `addTwoStageSplit` is protected, so exercising it needs a rule. This one has no pattern of its
/// own: the test drives the split directly.
class TestSplitRule : public IOptimizationRule
{
public:
    String getName() const override { return "TestSplit"; }
    bool checkPattern(GroupExpressionPtr, const ExpressionProperties &, const Memo &) const override { return true; }
    Promise getPromise() const override { return 0; }
    bool isTransformation() const override { return true; }

    GroupExpressionPtr split(Memo & memo, const GroupExpressionPtr & source_expression) const
    {
        return addTwoStageSplit(
            memo, source_expression, expressionOver(makeAggregatingStep(/*final=*/false)), makeMergingAggregatedStep(), {});
    }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr, const ExpressionProperties &, Memo &) const override
    {
        return {};
    }
};

}

/// The two outcomes of an intern: a distinct relation gets its own group, an equal one joins the
/// group that already holds it and is dropped there as a fully-equal duplicate.
TEST(CascadesMemoDedup, InternHitsAndMisses)
{
    auto memo = makeMemo(/*deduplicate=*/true);
    auto header = makeHeader();

    const GroupId first = memo.internExpression(projection(header, 1));
    const GroupId other_relation = memo.internExpression(projection(header, 2));
    EXPECT_NE(first, other_relation);

    const GroupId same_relation = memo.internExpression(projection(header, 1));
    EXPECT_EQ(same_relation, first);

    EXPECT_EQ(memo.getGroupCount(), 2u);
    EXPECT_EQ(memo.getGroup(first)->logical_expressions.size(), 1u);
    EXPECT_EQ(memo.getContext().memo_counters.groups_created, 2u);
    EXPECT_EQ(memo.getContext().memo_counters.groups_reused, 1u);
}

/// With the setting off nothing merges: every intern creates a group, exactly as before Stage B.
TEST(CascadesMemoDedup, SettingOffCreatesAFreshGroupEveryTime)
{
    auto memo = makeMemo(/*deduplicate=*/false);
    auto header = makeHeader();

    EXPECT_EQ(memo.internExpression(projection(header, 1)), 0u);
    EXPECT_EQ(memo.internExpression(projection(header, 1)), 1u);
    EXPECT_EQ(memo.internExpression(projection(header, 1)), 2u);

    EXPECT_EQ(memo.getGroupCount(), 3u);
    EXPECT_EQ(memo.getContext().memo_counters.groups_created, 3u);
    EXPECT_EQ(memo.getContext().memo_counters.groups_reused, 0u);
}

/// The participation gate, fail-closed: a step type that never opted into the logical digest gets a
/// fresh group even with the setting on, and even against an identical step object.
TEST(CascadesMemoDedup, StepWithoutLogicalDigestNeverMerges)
{
    auto memo = makeMemo(/*deduplicate=*/true);
    auto header = makeHeader();

    auto step = std::shared_ptr<const IQueryPlanStep>(std::make_shared<const OffsetStep>(header, 10));
    ASSERT_FALSE(step->hasLogicalDigest());

    auto first = expressionOver(QueryPlanStepPtr{});
    first->plan_step = step;
    auto second = expressionOver(QueryPlanStepPtr{});
    second->plan_step = step;

    EXPECT_NE(memo.internExpression(first), memo.internExpression(second));
    EXPECT_EQ(memo.getGroupCount(), 2u);
    EXPECT_EQ(memo.getContext().memo_counters.groups_reused, 0u);
}

/// The stage marker keeps a partial aggregation out of the group of the final one over the same
/// input. This is what stops deduplication from folding a partial stage into its own source group
/// and forming a self-cycle.
TEST(CascadesMemoDedup, StageMarkerSeparatesPartialFromFinal)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto memo = makeMemo(/*deduplicate=*/true);
    const GroupId input_group_id = memo.internExpression(projection(makeHeader(), 1));

    const GroupId final_group_id
        = memo.internExpression(expressionOver(makeAggregatingStep(/*final=*/true), {input_group_id}));
    const GroupId partial_group_id
        = memo.internExpression(expressionOver(makeAggregatingStep(/*final=*/false), {input_group_id}));

    EXPECT_NE(final_group_id, partial_group_id);
    EXPECT_EQ(memo.getGroupCount(), 3u);
}

/// The point of a logical - not full - identity: two expressions differing only in a physical knob
/// share one group and survive there as costed alternatives.
TEST(CascadesMemoDedup, KnobVariantJoinsTheSameGroupAsAnAlternative)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto memo = makeMemo(/*deduplicate=*/true);
    const GroupId input_group_id = memo.internExpression(projection(makeHeader(), 1));

    const GroupId group_id
        = memo.internExpression(expressionOver(makeAggregatingStep(/*final=*/true, /*merge_threads=*/4), {input_group_id}));
    const GroupId variant_group_id
        = memo.internExpression(expressionOver(makeAggregatingStep(/*final=*/true, /*merge_threads=*/8), {input_group_id}));

    EXPECT_EQ(variant_group_id, group_id);
    EXPECT_EQ(memo.getGroupCount(), 2u);
    /// Both alternatives are in the group: the full identity keeps them apart.
    EXPECT_EQ(memo.getGroup(group_id)->logical_expressions.size(), 2u);
    EXPECT_EQ(memo.getContext().memo_counters.groups_reused, 1u);
}

/// The cycle check of interning: an expression may never join a group its own subtree already
/// consumes, transitively. Only asserted in debug builds, so the walk is pinned directly here.
TEST(CascadesMemoDedup, ReachabilityFollowsInputLinksTransitively)
{
    auto memo = makeMemo(/*deduplicate=*/true);
    auto header = makeHeader();

    const GroupId leaf = memo.internExpression(projection(header, 1));
    const GroupId middle = memo.internExpression(expressionOver(std::make_unique<OffsetStep>(header, 10), {leaf}));
    const GroupId unrelated = memo.internExpression(projection(header, 2));

    auto probe = expressionOver(std::make_unique<OffsetStep>(header, 20), {middle});
    EXPECT_TRUE(memo.isGroupReachableFromInputs(*probe, middle));
    EXPECT_TRUE(memo.isGroupReachableFromInputs(*probe, leaf));
    EXPECT_FALSE(memo.isGroupReachableFromInputs(*probe, unrelated));
}

/// Detection only (plan section 9): an expression inserted into one group that matches an interned
/// expression of another group proves the two groups equal. It is counted, and nothing is merged.
TEST(CascadesMemoDedup, DuplicateGroupDetectionCountsWithoutMerging)
{
    auto memo = makeMemo(/*deduplicate=*/true);
    auto header = makeHeader();

    const GroupId first = memo.internExpression(projection(header, 1));
    const GroupId second = memo.internExpression(projection(header, 2));
    ASSERT_EQ(memo.getContext().memo_counters.duplicate_group_detections, 0u);

    /// An expression computing the relation of group `first`, inserted into group `second`.
    EXPECT_TRUE(memo.addLogicalExpressionToGroup(second, projection(header, 1)));

    EXPECT_EQ(memo.getContext().memo_counters.duplicate_group_detections, 1u);
    EXPECT_EQ(memo.getGroupCount(), 2u);
    EXPECT_EQ(memo.getGroup(first)->logical_expressions.size(), 1u);
    EXPECT_EQ(memo.getGroup(second)->logical_expressions.size(), 2u);
}

/// Addendum ruling 6: interning makes the `addTwoStageSplit` orphan reachable and fixes it in the
/// same move. A second identical split finds the partial group already in the memo, so the final
/// expression is a duplicate in the source group and is dropped - and because the partial group was
/// interned rather than created, nothing is left behind. Creating the partial group unconditionally
/// (the pre-Stage-B code) would leave a group here that no expression consumes.
TEST(CascadesMemoDedup, RepeatedTwoStageSplitLeavesNoOrphanGroup)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto memo = makeMemo(/*deduplicate=*/true);
    const GroupId input_group_id = memo.internExpression(projection(makeHeader(), 1));
    auto source_expression = expressionOver(makeAggregatingStep(/*final=*/true), {input_group_id});
    const GroupId source_group_id = memo.internExpression(source_expression);

    TestSplitRule rule;
    ASSERT_NE(rule.split(memo, source_expression), nullptr);
    const size_t group_count_after_first_split = memo.getGroupCount();
    ASSERT_EQ(group_count_after_first_split, 3u);
    ASSERT_EQ(memo.countGroupsUnreachableFrom(source_group_id), 0u);

    /// The second, identical split: no new group, and the final expression is dropped.
    EXPECT_EQ(rule.split(memo, source_expression), nullptr);
    EXPECT_EQ(memo.getGroupCount(), group_count_after_first_split);
    EXPECT_EQ(memo.getGroup(source_group_id)->logical_expressions.size(), 2u);
    EXPECT_EQ(memo.countGroupsUnreachableFrom(source_group_id), 0u);
    EXPECT_EQ(memo.getContext().memo_counters.groups_reused, 1u);
    /// The two stages compute different relations, so neither firing proves two groups equal.
    EXPECT_EQ(memo.getContext().memo_counters.duplicate_group_detections, 0u);
}

/// Without deduplication the same second split cannot be recognized: the fresh partial group makes
/// the final expression unique, so the memo grows by a whole duplicate stage. Nothing is orphaned
/// either - which is why the orphan was latent until interning landed.
TEST(CascadesMemoDedup, RepeatedTwoStageSplitDuplicatesWithoutDeduplication)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto memo = makeMemo(/*deduplicate=*/false);
    const GroupId input_group_id = memo.internExpression(projection(makeHeader(), 1));
    auto source_expression = expressionOver(makeAggregatingStep(/*final=*/true), {input_group_id});
    const GroupId source_group_id = memo.internExpression(source_expression);

    TestSplitRule rule;
    ASSERT_NE(rule.split(memo, source_expression), nullptr);
    EXPECT_NE(rule.split(memo, source_expression), nullptr);

    EXPECT_EQ(memo.getGroupCount(), 4u);
    EXPECT_EQ(memo.getGroup(source_group_id)->logical_expressions.size(), 3u);
    EXPECT_EQ(memo.countGroupsUnreachableFrom(source_group_id), 0u);
}

/// Stage D, end to end at memo level: two reads of one table built the way the two table expressions
/// of a self-join are (their own query info, storage snapshot and part list) intern into ONE group,
/// and a parent over that group then interns too - the merged read is what makes its consumers
/// mergeable at all.
TEST(CascadesMemoDedup, IndependentlyBuiltReadsOfOneTableInternIntoOneGroup)
{
    MergeTreeReadFixture fixture("memo_dedup_reads");
    auto memo = makeMemo(/*deduplicate=*/true);

    auto first_read = fixture.makeIndependentRead();
    auto second_read = fixture.makeIndependentRead();
    ASSERT_TRUE(first_read->hasLogicalDigest());
    auto read_header = first_read->getOutputHeader();

    const GroupId read_group = memo.internExpression(expressionOver(std::move(first_read)));
    EXPECT_EQ(memo.internExpression(expressionOver(std::move(second_read))), read_group);

    /// Two identical projections over that one read group.
    auto projection_over_read = [&] { return expressionOver(std::make_unique<ExpressionStep>(read_header, readDag()), {read_group}); };

    const GroupId projection_group = memo.internExpression(projection_over_read());
    EXPECT_EQ(memo.internExpression(projection_over_read()), projection_group);
    EXPECT_NE(projection_group, read_group);

    EXPECT_EQ(memo.getGroupCount(), 2u);
    EXPECT_EQ(memo.getContext().memo_counters.groups_created, 2u);
    EXPECT_EQ(memo.getContext().memo_counters.groups_reused, 2u);
    EXPECT_EQ(memo.countGroupsUnreachableFrom(projection_group), 0u);
}

/// With the setting off a read behaves like everything else: a group each, and the parents cannot
/// match either, because their input group ids differ.
TEST(CascadesMemoDedup, ReadsDoNotMergeWithDeduplicationOff)
{
    MergeTreeReadFixture fixture("memo_dedup_reads_off");
    auto memo = makeMemo(/*deduplicate=*/false);

    EXPECT_NE(
        memo.internExpression(expressionOver(fixture.makeIndependentRead())),
        memo.internExpression(expressionOver(fixture.makeIndependentRead())));
    EXPECT_EQ(memo.getGroupCount(), 2u);
    EXPECT_EQ(memo.getContext().memo_counters.groups_reused, 0u);
}
