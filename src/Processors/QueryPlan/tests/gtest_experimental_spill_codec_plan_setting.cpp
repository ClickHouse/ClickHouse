#include <gtest/gtest.h>

#include <base/unit.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Defines.h>
#include <Core/Joins.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>

using namespace DB;

/// `allow_experimental_codecs` tells a remote shard that the initiator accepted an experimental
/// `temporary_files_codec`, so the shard resolves the same codec when it spills.
///
/// `QueryPlanSerializationSettings` is a strict named schema: `readBinary` throws on a name it does not know,
/// so a peer that predates the setting rejects any plan carrying it. It must therefore go on the wire only
/// when the spill behavior of the step depends on it - the step can reach temporary files at all, the opt-in
/// is set, and the codec it enables is experimental. For every other plan the reader's default (`false`)
/// encodes the identical behavior, so emitting the name would break a rolling upgrade for nothing.
namespace
{

/// `ALP` is experimental and, unlike `PCO`, always compiled in.
const String experimental_codec = "ALP";
const String plain_codec = "LZ4";

/// The name as it appears in the binary settings stream written by `writeChangedBinary`.
bool wireCarriesSetting(const QueryPlanSerializationSettings & settings)
{
    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out);
    return out.str().contains("allow_experimental_codecs");
}

bool sortingStepCarriesSetting(
    const String & codec, bool allow_experimental_codecs, size_t max_bytes_before_external_sort, bool sorting_is_reachable = true)
{
    SortingStep::Settings sort_settings(/*max_block_size_=*/65536);
    sort_settings.temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    sort_settings.temporary_files_codec = codec;
    sort_settings.allow_experimental_codecs = allow_experimental_codecs;
    sort_settings.max_bytes_in_block_before_external_sort = max_bytes_before_external_sort;

    QueryPlanSerializationSettings settings;
    sort_settings.updatePlanSettings(settings, sorting_is_reachable);
    return wireCarriesSetting(settings);
}

JoinSettings makeJoinSettings(const String & codec, bool allow_experimental_codecs, std::vector<JoinAlgorithm> algorithms)
{
    JoinSettings join_settings(QueryPlanSerializationSettings{});
    join_settings.temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    join_settings.temporary_files_codec = codec;
    join_settings.allow_experimental_codecs = allow_experimental_codecs;
    join_settings.join_algorithms = std::move(algorithms);
    join_settings.max_bytes_before_external_join = 0;
    join_settings.max_bytes_ratio_before_external_join = 0.;
    join_settings.max_rows_in_join = 0;
    join_settings.max_bytes_in_join = 0;
    return join_settings;
}

bool joinCarriesSetting(const JoinSettings & join_settings, const JoinOperator & join_operator)
{
    QueryPlanSerializationSettings settings;
    join_settings.updatePlanSettings(settings, join_operator);
    return wireCarriesSetting(settings);
}

/// A join operator over two single-column inputs, keyed by the equality `l.k = r.k` (which rules
/// `ConstantJoin` out) unless `keyed` is false. Owns the expression DAG the operator refers to.
struct TestJoinOperator
{
    JoinExpressionActions expression_actions;
    JoinOperator join_operator;

    explicit TestJoinOperator(JoinKind kind, JoinStrictness strictness = JoinStrictness::All, bool keyed = true)
        : expression_actions(
              ColumnsWithTypeAndName{{std::make_shared<DataTypeUInt64>(), "l.k"}},
              ColumnsWithTypeAndName{{std::make_shared<DataTypeUInt64>(), "r.k"}})
        , join_operator(kind, strictness)
    {
        auto actions_dag = expression_actions.getActionsDAG();
        actions_dag->getOutputs() = actions_dag->getInputs();
        if (keyed)
            join_operator.expression.push_back(JoinActionRef::transform({
                JoinActionRef(actions_dag->tryFindInOutputs("l.k"), expression_actions),
                JoinActionRef(actions_dag->tryFindInOutputs("r.k"), expression_actions),
            }, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    }
};

}

TEST(ExperimentalSpillCodecPlanSetting, EmittedOnlyWhenSpillingCanReachTheCodec)
{
    /// External aggregation: `Aggregator` reaches `writeToTemporaryFile` only with a non-zero
    /// `max_bytes_before_external_group_by`, which is what `AggregatingStep::serializeSettings` passes here.
    EXPECT_TRUE(spillCodecNeedsExperimentalCodecsOptIn(/*spill_is_reachable=*/true, true, experimental_codec));
    EXPECT_FALSE(spillCodecNeedsExperimentalCodecsOptIn(/*spill_is_reachable=*/false, true, experimental_codec));

    /// Nothing to communicate for a codec the receiver accepts without the opt-in, or when the initiator
    /// itself did not opt in.
    EXPECT_FALSE(spillCodecNeedsExperimentalCodecsOptIn(true, true, plain_codec));
    EXPECT_FALSE(spillCodecNeedsExperimentalCodecsOptIn(true, true, /*compression_codec=*/""));
    EXPECT_FALSE(spillCodecNeedsExperimentalCodecsOptIn(true, false, experimental_codec));
}

TEST(ExperimentalSpillCodecPlanSetting, SortingStepEmitsItOnlyForAnExternalSort)
{
    EXPECT_TRUE(sortingStepCarriesSetting(experimental_codec, true, /*max_bytes_before_external_sort=*/1_MiB));

    /// `MergeSortingTransform::consume` never touches the temporary data with the threshold at `0`.
    EXPECT_FALSE(sortingStepCarriesSetting(experimental_codec, true, /*max_bytes_before_external_sort=*/0));

    /// Sorting settings riding along a join that no sorting-based algorithm can execute configure nothing,
    /// so even an external-sort threshold does not put the opt-in on the wire.
    EXPECT_FALSE(sortingStepCarriesSetting(experimental_codec, true, 1_MiB, /*sorting_is_reachable=*/false));

    EXPECT_FALSE(sortingStepCarriesSetting(plain_codec, true, 1_MiB));
    EXPECT_FALSE(sortingStepCarriesSetting(experimental_codec, false, 1_MiB));
}

TEST(ExperimentalSpillCodecPlanSetting, JoinEmitsItOnlyForASpillingJoin)
{
    tryRegisterFunctions();

    TestJoinOperator keyed_inner(JoinKind::Inner);
    TestJoinOperator cross_join(JoinKind::Cross, JoinStrictness::All, /*keyed=*/false);

    /// An in-memory hash join with no external-join threshold and no in-memory size limits never reaches
    /// temporary-file join code.
    auto in_memory_hash = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    EXPECT_FALSE(in_memory_hash.canSpillToTemporaryFiles(cross_join.join_operator));
    EXPECT_FALSE(joinCarriesSetting(in_memory_hash, keyed_inner.join_operator));

    auto in_memory_parallel_hash = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::PARALLEL_HASH});
    EXPECT_FALSE(joinCarriesSetting(in_memory_parallel_hash, keyed_inner.join_operator));

    /// The automatic conversion to a spilling hash join is gated on an external-join threshold; either the
    /// absolute setting or the ratio enables it.
    auto spilling_hash = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    spilling_hash.max_bytes_before_external_join = 1_MiB;
    EXPECT_TRUE(spilling_hash.canSpillToTemporaryFiles(keyed_inner.join_operator));
    EXPECT_TRUE(joinCarriesSetting(spilling_hash, keyed_inner.join_operator));

    auto ratio_hash = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    ratio_hash.max_bytes_ratio_before_external_join = 0.5;
    EXPECT_TRUE(joinCarriesSetting(ratio_hash, keyed_inner.join_operator));

    /// Only the algorithms that build a hash join consult the external-join thresholds; the others never
    /// look at them, so the threshold alone must not put the opt-in on the wire.
    for (auto algorithm : {JoinAlgorithm::DIRECT, JoinAlgorithm::FULL_SORTING_MERGE,
                           JoinAlgorithm::PARALLEL_FULL_SORTING_MERGE, JoinAlgorithm::IE_JOIN})
    {
        auto non_hash = makeJoinSettings(experimental_codec, true, {algorithm});
        non_hash.max_bytes_before_external_join = 1_MiB;
        EXPECT_FALSE(joinCarriesSetting(non_hash, keyed_inner.join_operator));
    }

    /// The conversion to a spilling hash join also requires a kind/strictness pair `GraceHashJoin` accepts;
    /// an ASOF join stays in memory whatever the threshold.
    TestJoinOperator asof_join(JoinKind::Left, JoinStrictness::Asof);
    auto asof_hash = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    asof_hash.max_bytes_before_external_join = 1_MiB;
    EXPECT_FALSE(joinCarriesSetting(asof_hash, asof_join.join_operator));
    EXPECT_FALSE(joinCarriesSetting(makeJoinSettings(experimental_codec, true, {JoinAlgorithm::GRACE_HASH}), asof_join.join_operator));

    /// `grace_hash` always spills; `partial_merge` and `auto` can end up in `MergeJoin`, which writes the
    /// right table to disk.
    for (auto algorithm : {JoinAlgorithm::GRACE_HASH, JoinAlgorithm::PARTIAL_MERGE,
                           JoinAlgorithm::PREFER_PARTIAL_MERGE, JoinAlgorithm::AUTO})
        EXPECT_TRUE(joinCarriesSetting(makeJoinSettings(experimental_codec, true, {algorithm}), keyed_inner.join_operator));

    /// ... but only for the kind/strictness pairs `MergeJoin` supports. A keyed `RIGHT ANY` join under
    /// `auto` or `partial_merge` falls back to an in-memory hash join, so with no external-join threshold
    /// it cannot spill; with one it converts to a spilling hash join again.
    TestJoinOperator right_any(JoinKind::Right, JoinStrictness::Any);
    EXPECT_FALSE(joinCarriesSetting(makeJoinSettings(experimental_codec, true, {JoinAlgorithm::AUTO}), right_any.join_operator));
    EXPECT_FALSE(joinCarriesSetting(makeJoinSettings(experimental_codec, true, {JoinAlgorithm::PARTIAL_MERGE}), right_any.join_operator));
    auto right_any_external = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::AUTO});
    right_any_external.max_bytes_before_external_join = 1_MiB;
    EXPECT_TRUE(joinCarriesSetting(right_any_external, right_any.join_operator));

    /// `ConstantJoin` (`CROSS`, comma and constant-predicate joins) spills once the in-memory size limits
    /// would be exceeded, whatever the algorithm is - but only a join whose shape admits a `ConstantJoin`
    /// can reach it. A join keyed by a genuine equality never does, so for it the size limits alone must
    /// not put the opt-in on the wire.
    auto hash_with_size_limit = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    hash_with_size_limit.max_bytes_in_join = 1_MiB;
    EXPECT_TRUE(joinCarriesSetting(hash_with_size_limit, cross_join.join_operator));
    EXPECT_FALSE(joinCarriesSetting(hash_with_size_limit, keyed_inner.join_operator));

    auto hash_with_row_limit = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    hash_with_row_limit.max_rows_in_join = 1000;
    EXPECT_TRUE(joinCarriesSetting(hash_with_row_limit, cross_join.join_operator));
    EXPECT_FALSE(joinCarriesSetting(hash_with_row_limit, keyed_inner.join_operator));

    /// A spilling join still needs nothing on the wire for a non-experimental codec, or without the opt-in.
    auto plain = makeJoinSettings(plain_codec, true, {JoinAlgorithm::GRACE_HASH});
    EXPECT_FALSE(joinCarriesSetting(plain, keyed_inner.join_operator));

    auto no_opt_in = makeJoinSettings(experimental_codec, false, {JoinAlgorithm::GRACE_HASH});
    EXPECT_FALSE(joinCarriesSetting(no_opt_in, keyed_inner.join_operator));
}

TEST(ExperimentalSpillCodecPlanSetting, ConstantJoinIsRuledOutByACrossSideEquality)
{
    tryRegisterFunctions();

    /// CROSS / comma joins are always executed by `ConstantJoin`.
    EXPECT_TRUE(JoinOperator(JoinKind::Cross).canBecomeConstantJoin());
    EXPECT_TRUE(JoinOperator(JoinKind::Comma).canBecomeConstantJoin());

    /// An empty expression is an always-true predicate: a join with a constant.
    EXPECT_TRUE(JoinOperator(JoinKind::Inner).canBecomeConstantJoin());

    auto type = std::make_shared<DataTypeUInt64>();
    ColumnsWithTypeAndName left_header{{type, "l.k"}, {type, "l.v"}};
    ColumnsWithTypeAndName right_header{{type, "r.k"}, {type, "r.v"}};
    JoinExpressionActions expression_actions(left_header, right_header);
    auto actions_dag = expression_actions.getActionsDAG();
    actions_dag->getOutputs() = actions_dag->getInputs();

    auto make_equality = [&](const String & lhs, const String & rhs)
    {
        return JoinActionRef::transform({
            JoinActionRef(actions_dag->tryFindInOutputs(lhs), expression_actions),
            JoinActionRef(actions_dag->tryFindInOutputs(rhs), expression_actions),
        }, JoinActionRef::AddFunction(JoinConditionOperator::Equals));
    };

    /// `l.k = r.k` becomes a hash-join key, so the join can never degenerate to a `ConstantJoin`.
    JoinOperator keyed_join(JoinKind::Inner);
    keyed_join.expression.push_back(make_equality("l.k", "r.k"));
    EXPECT_FALSE(keyed_join.canBecomeConstantJoin());

    /// A same-side equality is not a join key; without a cross-side one the join may still convert to CROSS.
    JoinOperator same_side_join(JoinKind::Inner);
    same_side_join.expression.push_back(make_equality("l.k", "l.v"));
    EXPECT_TRUE(same_side_join.canBecomeConstantJoin());
}

TEST(ExperimentalSpillCodecPlanSetting, IEJoinNeedsTwoCrossSideInequalities)
{
    tryRegisterFunctions();

    auto type = std::make_shared<DataTypeUInt64>();
    ColumnsWithTypeAndName left_header{{type, "l.k"}, {type, "l.v"}};
    ColumnsWithTypeAndName right_header{{type, "r.k"}, {type, "r.v"}};
    JoinExpressionActions expression_actions(left_header, right_header);
    auto actions_dag = expression_actions.getActionsDAG();
    actions_dag->getOutputs() = actions_dag->getInputs();

    auto make_condition = [&](JoinConditionOperator op, const String & lhs, const String & rhs)
    {
        return JoinActionRef::transform({
            JoinActionRef(actions_dag->tryFindInOutputs(lhs), expression_actions),
            JoinActionRef(actions_dag->tryFindInOutputs(rhs), expression_actions),
        }, JoinActionRef::AddFunction(op));
    };

    /// A pure equi-join has no IEJoin shape, so with `join_algorithm='hash,ie_join'` the sort carrier of
    /// `JoinStepLogical::serializeSettings` must stay silent.
    JoinOperator equi_join(JoinKind::Inner);
    equi_join.expression.push_back(make_condition(JoinConditionOperator::Equals, "l.k", "r.k"));
    EXPECT_FALSE(equi_join.hasCrossSideInequalityPair());

    /// One cross-side inequality is not enough: `tryExtractIEJoinDescription` needs exactly two keys.
    JoinOperator single_inequality(JoinKind::Inner);
    single_inequality.expression.push_back(make_condition(JoinConditionOperator::Less, "l.k", "r.k"));
    EXPECT_FALSE(single_inequality.hasCrossSideInequalityPair());

    /// Same-side inequalities are filters, not IEJoin keys.
    JoinOperator same_side_inequalities(JoinKind::Inner);
    same_side_inequalities.expression.push_back(make_condition(JoinConditionOperator::Less, "l.k", "l.v"));
    same_side_inequalities.expression.push_back(make_condition(JoinConditionOperator::Greater, "r.k", "r.v"));
    EXPECT_FALSE(same_side_inequalities.hasCrossSideInequalityPair());

    /// Two inequalities between the two inputs - the IEJoin shape - in either operand orientation.
    JoinOperator ie_join(JoinKind::Inner);
    ie_join.expression.push_back(make_condition(JoinConditionOperator::Less, "l.k", "r.k"));
    ie_join.expression.push_back(make_condition(JoinConditionOperator::GreaterOrEquals, "r.v", "l.v"));
    EXPECT_TRUE(ie_join.hasCrossSideInequalityPair());
}
