#include <gtest/gtest.h>

#include <base/unit.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Defines.h>
#include <Core/Joins.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>

using namespace DB;

/// `spill_codec_authorized` tells a remote shard that the initiator accepted an experimental
/// `temporary_files_codec`, so the shard resolves the same codec when it spills.
///
/// `QueryPlanSerializationSettings` is a strict named schema: `readBinary` throws on a name it does not know,
/// so a peer that predates the setting rejects any plan carrying it. It must therefore go on the wire only
/// when the spill behavior of the step depends on it - the step can reach temporary files at all, the opt-in
/// is set, and the codec it enables is experimental. For every other plan the reader's default (`false`)
/// encodes the identical behavior, so emitting the name would break a rolling upgrade for nothing.
namespace
{

/// `ZXC` is experimental, usable on untyped spill data (unlike `PCO` and `ALP`, which require a column
/// type and so cannot compress temporary files at all - see `temporaryFilesCodecIsGated`), and,
/// unlike `PCO`, always compiled in.
const String experimental_codec = "ZXC";
const String plain_codec = "LZ4";

/// The name as it appears in the binary settings stream written by `writeChangedBinary`.
bool wireCarriesSetting(const QueryPlanSerializationSettings & settings)
{
    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out);
    return out.str().contains("spill_codec_authorized");
}

bool sortingStepCarriesSetting(
    const String & codec, bool spill_codec_authorized, size_t max_bytes_before_external_sort,
    bool sorting_is_reachable = true, UInt64 version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION)
{
    SortingStep::Settings sort_settings(/*max_block_size_=*/65536);
    sort_settings.temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    sort_settings.temporary_files_codec = codec;
    sort_settings.spill_codec_authorized = spill_codec_authorized;
    sort_settings.max_bytes_in_block_before_external_sort = max_bytes_before_external_sort;

    QueryPlanSerializationSettings settings;
    sort_settings.updatePlanSettings(settings, sorting_is_reachable, version);
    return wireCarriesSetting(settings);
}

JoinSettings makeJoinSettings(const String & codec, bool spill_codec_authorized, std::vector<JoinAlgorithm> algorithms)
{
    JoinSettings join_settings(QueryPlanSerializationSettings{});
    join_settings.temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    join_settings.temporary_files_codec = codec;
    join_settings.spill_codec_authorized = spill_codec_authorized;
    join_settings.join_algorithms = std::move(algorithms);
    join_settings.max_bytes_before_external_join = 0;
    join_settings.max_bytes_ratio_before_external_join = 0.;
    join_settings.max_rows_in_join = 0;
    join_settings.max_bytes_in_join = 0;
    return join_settings;
}

bool joinCarriesSetting(
    const JoinSettings & join_settings, const JoinOperator & join_operator, UInt64 version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION)
{
    QueryPlanSerializationSettings settings;
    join_settings.updatePlanSettings(settings, join_operator, version);
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
    EXPECT_TRUE(spillCodecAuthorizationMustBeSerialized(/*spill_is_reachable=*/true, true, experimental_codec));
    EXPECT_FALSE(spillCodecAuthorizationMustBeSerialized(/*spill_is_reachable=*/false, true, experimental_codec));

    /// Nothing to communicate for a codec the receiver accepts without the opt-in, or when the initiator
    /// itself did not opt in.
    EXPECT_FALSE(spillCodecAuthorizationMustBeSerialized(true, true, plain_codec));
    EXPECT_FALSE(spillCodecAuthorizationMustBeSerialized(true, true, /*compression_codec=*/""));
    EXPECT_FALSE(spillCodecAuthorizationMustBeSerialized(true, false, experimental_codec));

    /// A codec that cannot compress untyped data at all makes the spill itself fail with the same error
    /// on every peer, with and without the opt-in, so there is nothing to communicate - and classifying
    /// it must not throw at plan-serialization time, because the query may never actually spill. The same
    /// goes for a codec string that does not resolve at all.
    EXPECT_FALSE(spillCodecAuthorizationMustBeSerialized(true, true, "ALP"));
    EXPECT_FALSE(spillCodecAuthorizationMustBeSerialized(true, true, "T64('bit')"));
    EXPECT_FALSE(spillCodecAuthorizationMustBeSerialized(true, true, "NO_SUCH_CODEC"));
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

    /// A pre-v8 worker would silently lose the opt-in and fail only after spilling, so reject the plan
    /// before sending it to that worker.
    EXPECT_THROW(sortingStepCarriesSetting(
        experimental_codec, true, 1_MiB, /*sorting_is_reachable=*/true,
        DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXPERIMENTAL_SPILL_CODEC - 1), Exception);
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

    /// A worker without temporary storage stays in memory, so it must not receive an opt-in for a
    /// temporary-file codec that it will never resolve.
    auto spilling_hash_without_temporary_storage = spilling_hash;
    spilling_hash_without_temporary_storage.temporary_storage_available = false;
    EXPECT_FALSE(spilling_hash_without_temporary_storage.canSpillToTemporaryFiles(keyed_inner.join_operator));
    EXPECT_FALSE(joinCarriesSetting(spilling_hash_without_temporary_storage, keyed_inner.join_operator));
    EXPECT_FALSE(joinCarriesSetting(
        spilling_hash_without_temporary_storage, keyed_inner.join_operator,
        DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXPERIMENTAL_SPILL_CODEC - 1));

    auto ratio_hash = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    ratio_hash.max_bytes_ratio_before_external_join = 0.5;
    EXPECT_TRUE(joinCarriesSetting(ratio_hash, keyed_inner.join_operator));

    /// `prefer_partial_merge` chooses `MergeJoin` before considering the hash fallback. When every
    /// merge limit is disabled, `MergeJoin` fails before it can spill, so an external hash threshold
    /// must not claim the codec is reachable. `auto` still starts with the hash branch and can spill.
    auto prefer_partial_merge_without_limits = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::PREFER_PARTIAL_MERGE});
    prefer_partial_merge_without_limits.max_bytes_before_external_join = 1_MiB;
    prefer_partial_merge_without_limits.default_max_bytes_in_join = 0;
    EXPECT_FALSE(joinCarriesSetting(prefer_partial_merge_without_limits, keyed_inner.join_operator));

    auto auto_without_merge_limits = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::AUTO});
    auto_without_merge_limits.max_bytes_before_external_join = 1_MiB;
    auto_without_merge_limits.default_max_bytes_in_join = 0;
    EXPECT_TRUE(joinCarriesSetting(auto_without_merge_limits, keyed_inner.join_operator));

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

    /// `chooseJoinAlgorithm` walks the algorithm list in order and the first algorithm that builds a join
    /// wins, so a spill-capable algorithm listed after `hash` (whose branch always builds an in-memory
    /// join when no external-join threshold is set) is never consulted.
    EXPECT_FALSE(joinCarriesSetting(
        makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH, JoinAlgorithm::GRACE_HASH}), keyed_inner.join_operator));
    EXPECT_TRUE(joinCarriesSetting(
        makeJoinSettings(experimental_codec, true, {JoinAlgorithm::GRACE_HASH, JoinAlgorithm::HASH}), keyed_inner.join_operator));
    EXPECT_FALSE(joinCarriesSetting(
        makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH, JoinAlgorithm::PARTIAL_MERGE}), keyed_inner.join_operator));
    EXPECT_TRUE(joinCarriesSetting(
        makeJoinSettings(experimental_codec, true, {JoinAlgorithm::PARTIAL_MERGE, JoinAlgorithm::HASH}), keyed_inner.join_operator));

    /// ... while an external-join threshold makes the leading `hash` entry itself the spilling one.
    auto ordered_spilling = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH, JoinAlgorithm::GRACE_HASH});
    ordered_spilling.max_bytes_before_external_join = 1_MiB;
    EXPECT_TRUE(joinCarriesSetting(ordered_spilling, keyed_inner.join_operator));

    /// An entry that builds a join only for the shapes its algorithm supports (`partial_merge` for a keyed
    /// `RIGHT ANY` join) lets the selection move on, so it does not cut off a later spilling entry.
    EXPECT_TRUE(joinCarriesSetting(
        makeJoinSettings(experimental_codec, true, {JoinAlgorithm::PARTIAL_MERGE, JoinAlgorithm::GRACE_HASH}), right_any.join_operator));

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

TEST(ExperimentalSpillCodecPlanSetting, ExternalAggregationNeedsATwoLevelConvertibleMethod)
{
    auto number_type = std::make_shared<DataTypeUInt64>();
    auto tiny_type = std::make_shared<DataTypeUInt8>();
    auto string_type = std::make_shared<DataTypeString>();
    Block header({
        ColumnWithTypeAndName(number_type->createColumn(), number_type, "n"),
        ColumnWithTypeAndName(tiny_type->createColumn(), tiny_type, "t"),
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "s")});

    /// `count()` with no keys aggregates into a single value (`without_key`), and a tiny fixed map
    /// (`GROUP BY` over a `UInt8` - `key8`) never converts to two-level either, so neither can ever reach
    /// `writeToTemporaryFile`.
    EXPECT_FALSE(aggregationCanGoTwoLevel(header, {}, {}));
    EXPECT_FALSE(aggregationCanGoTwoLevel(header, {"t"}, {}));

    EXPECT_TRUE(aggregationCanGoTwoLevel(header, {"n"}, {}));
    EXPECT_TRUE(aggregationCanGoTwoLevel(header, {"s"}, {}));

    /// Every grouping set gets its own hash table; one convertible set is enough.
    const GroupingSetsParamsList tiny_and_number = {GroupingSetsParams({"t"}, {"n"}), GroupingSetsParams({"n"}, {"t"})};
    EXPECT_TRUE(aggregationCanGoTwoLevel(header, {"t", "n"}, tiny_and_number));
    const GroupingSetsParamsList tiny_only = {GroupingSetsParams({"t"}, {})};
    EXPECT_FALSE(aggregationCanGoTwoLevel(header, {"t"}, tiny_only));

    /// Fail closed: an unresolvable key counts as convertible, so the opt-in is communicated.
    EXPECT_TRUE(aggregationCanGoTwoLevel(header, {"absent"}, {}));
}

TEST(ExperimentalSpillCodecPlanSetting, ADisjunctiveJoinSpillsOnlyThroughConstantJoin)
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
    auto combine = [&](JoinConditionOperator op, const std::vector<JoinActionRef> & args)
    {
        return JoinActionRef::transform(args, JoinActionRef::AddFunction(op));
    };

    /// `l.k = r.k OR l.v = r.v`: every disjunct carries its own key, so the planning splits the join into
    /// one `TableJoin` clause per disjunct. Every spilling implementation requires the single-clause shape
    /// (`TableJoin::oneDisjunct`), so only an in-memory hash join can execute this join: neither an
    /// external-join threshold nor a merge-based algorithm nor the in-memory size limits may put the
    /// opt-in on the wire.
    JoinOperator keyed_disjunction(JoinKind::Inner);
    keyed_disjunction.expression.push_back(combine(JoinConditionOperator::Or, {
        make_condition(JoinConditionOperator::Equals, "l.k", "r.k"),
        make_condition(JoinConditionOperator::Equals, "l.v", "r.v"),
    }));
    EXPECT_TRUE(keyed_disjunction.expressionIsTopLevelDisjunction());
    EXPECT_FALSE(keyed_disjunction.canBecomeConstantJoin());

    auto spilling_hash = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    spilling_hash.max_bytes_before_external_join = 1_MiB;
    EXPECT_FALSE(joinCarriesSetting(spilling_hash, keyed_disjunction));

    for (auto algorithm : {JoinAlgorithm::GRACE_HASH, JoinAlgorithm::PARTIAL_MERGE,
                           JoinAlgorithm::PREFER_PARTIAL_MERGE, JoinAlgorithm::AUTO})
        EXPECT_FALSE(joinCarriesSetting(makeJoinSettings(experimental_codec, true, {algorithm}), keyed_disjunction));

    auto hash_with_size_limit = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    hash_with_size_limit.max_bytes_in_join = 1_MiB;
    EXPECT_FALSE(joinCarriesSetting(hash_with_size_limit, keyed_disjunction));

    /// `l.k = r.k OR l.v > r.v`: the keyless disjunct makes the planning fall back to the conversion to
    /// CROSS, so the join can reach `ConstantJoin`, which spills on the in-memory size limits - but still
    /// never consults the external-join threshold.
    JoinOperator keyless_disjunct(JoinKind::Inner);
    keyless_disjunct.expression.push_back(combine(JoinConditionOperator::Or, {
        make_condition(JoinConditionOperator::Equals, "l.k", "r.k"),
        make_condition(JoinConditionOperator::Greater, "l.v", "r.v"),
    }));
    EXPECT_TRUE(keyless_disjunct.canBecomeConstantJoin());
    EXPECT_TRUE(joinCarriesSetting(hash_with_size_limit, keyless_disjunct));
    EXPECT_FALSE(joinCarriesSetting(spilling_hash, keyless_disjunct));

    /// A key inside an AND-nested disjunct still keys its clause:
    /// `(l.k = r.k AND l.v < r.v) OR l.v = r.v` splits into two keyed clauses.
    JoinOperator and_nested_disjunction(JoinKind::Inner);
    and_nested_disjunction.expression.push_back(combine(JoinConditionOperator::Or, {
        combine(JoinConditionOperator::And, {
            make_condition(JoinConditionOperator::Equals, "l.k", "r.k"),
            make_condition(JoinConditionOperator::Less, "l.v", "r.v"),
        }),
        make_condition(JoinConditionOperator::Equals, "l.v", "r.v"),
    }));
    EXPECT_FALSE(and_nested_disjunction.canBecomeConstantJoin());
    EXPECT_FALSE(joinCarriesSetting(spilling_hash, and_nested_disjunction));

    /// A disjunction that is one of several top-level conjuncts does not split the join: the other
    /// conjunct provides the key of a single clause, and the join converts to a spilling hash join under
    /// an external-join threshold as usual.
    JoinOperator conjunct_with_disjunction(JoinKind::Inner);
    conjunct_with_disjunction.expression.push_back(make_condition(JoinConditionOperator::Equals, "l.k", "r.k"));
    conjunct_with_disjunction.expression.push_back(combine(JoinConditionOperator::Or, {
        make_condition(JoinConditionOperator::Less, "l.v", "r.v"),
        make_condition(JoinConditionOperator::Greater, "l.v", "r.v"),
    }));
    EXPECT_FALSE(conjunct_with_disjunction.expressionIsTopLevelDisjunction());
    EXPECT_TRUE(joinCarriesSetting(spilling_hash, conjunct_with_disjunction));
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

TEST(ExperimentalSpillCodecPlanSetting, IEJoinNeedsOperandTypesItsOperatorCanCompare)
{
    tryRegisterFunctions();

    /// `tryGetIEJoinKeyCondition` declines an operand the IEJoin operator cannot order the way the
    /// comparison functions do, so the join keeps the IEJoin shape but is planned as an ordinary join over a
    /// filter: no sorts are added around it and the sort carrier must stay silent.
    auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt64>()});
    EXPECT_FALSE(ieJoinCanCompareOperandTypes(tuple_type, tuple_type));
    EXPECT_FALSE(ieJoinCanCompareOperandTypes(std::make_shared<DataTypeDynamic>(), std::make_shared<DataTypeDynamic>()));
    EXPECT_FALSE(ieJoinCanCompareOperandTypes(
        std::make_shared<DataTypeArray>(tuple_type), std::make_shared<DataTypeArray>(tuple_type)));

    /// Operands without a common type cannot be casted for the comparison either.
    EXPECT_FALSE(ieJoinCanCompareOperandTypes(std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeString>()));

    /// A pair the operator handles: equal types, and types with a common supertype.
    EXPECT_TRUE(ieJoinCanCompareOperandTypes(std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()));
    EXPECT_TRUE(ieJoinCanCompareOperandTypes(std::make_shared<DataTypeUInt32>(), std::make_shared<DataTypeInt64>()));

    ColumnsWithTypeAndName left_header{{tuple_type, "l.t"}};
    ColumnsWithTypeAndName right_header{{tuple_type, "r.t"}};
    JoinExpressionActions expression_actions(left_header, right_header);
    auto actions_dag = expression_actions.getActionsDAG();
    actions_dag->getOutputs() = actions_dag->getInputs();

    auto make_condition = [&](JoinConditionOperator op)
    {
        return JoinActionRef::transform({
            JoinActionRef(actions_dag->tryFindInOutputs("l.t"), expression_actions),
            JoinActionRef(actions_dag->tryFindInOutputs("r.t"), expression_actions),
        }, JoinActionRef::AddFunction(op));
    };

    /// The IEJoin condition shape over `Tuple` operands: two cross-side inequalities that
    /// `tryGetIEJoinKeyCondition` nevertheless declines.
    JoinOperator tuple_inequalities(JoinKind::Inner);
    tuple_inequalities.expression.push_back(make_condition(JoinConditionOperator::Less));
    tuple_inequalities.expression.push_back(make_condition(JoinConditionOperator::GreaterOrEquals));
    EXPECT_FALSE(tuple_inequalities.hasCrossSideInequalityPair());
}

TEST(ExperimentalSpillCodecPlanSetting, FullSortingMergeJoinNeedsAClauseWithoutAPreFilterCondition)
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

    /// A condition over a single input that the optimizer cannot push out of the ON expression becomes the
    /// pre-filter condition of the join clause, which `FullSortingMergeJoin::isSupported` rejects: the join
    /// falls back to an in-memory algorithm, no sorts are added, and the sort carrier of
    /// `JoinStepLogical::serializeSettings` must stay silent. For a `LEFT` join only a right-side condition
    /// can be pushed down.
    JoinOperator left_join_with_left_condition(JoinKind::Left);
    left_join_with_left_condition.expression.push_back(make_condition(JoinConditionOperator::Equals, "l.k", "r.k"));
    left_join_with_left_condition.expression.push_back(make_condition(JoinConditionOperator::Less, "l.k", "l.v"));
    EXPECT_TRUE(left_join_with_left_condition.hasSingleSidePreFilterCondition());

    JoinOperator left_join_with_right_condition(JoinKind::Left);
    left_join_with_right_condition.expression.push_back(make_condition(JoinConditionOperator::Equals, "l.k", "r.k"));
    left_join_with_right_condition.expression.push_back(make_condition(JoinConditionOperator::Less, "r.k", "r.v"));
    EXPECT_FALSE(left_join_with_right_condition.hasSingleSidePreFilterCondition());

    /// An `INNER` join pushes a condition of either side down.
    JoinOperator inner_join_with_conditions(JoinKind::Inner);
    inner_join_with_conditions.expression.push_back(make_condition(JoinConditionOperator::Equals, "l.k", "r.k"));
    inner_join_with_conditions.expression.push_back(make_condition(JoinConditionOperator::Less, "l.k", "l.v"));
    inner_join_with_conditions.expression.push_back(make_condition(JoinConditionOperator::Less, "r.k", "r.v"));
    EXPECT_FALSE(inner_join_with_conditions.hasSingleSidePreFilterCondition());

    /// `ANY` never pushes a condition down, whatever the kind.
    JoinOperator any_inner_join_with_condition(JoinKind::Inner, JoinStrictness::Any);
    any_inner_join_with_condition.expression.push_back(make_condition(JoinConditionOperator::Equals, "l.k", "r.k"));
    any_inner_join_with_condition.expression.push_back(make_condition(JoinConditionOperator::Less, "r.k", "r.v"));
    EXPECT_TRUE(any_inner_join_with_condition.hasSingleSidePreFilterCondition());

    /// The cross-side conditions of a keyed join are the keys and the mixed conditions of the ON clause,
    /// never a pre-filter condition of the clause.
    JoinOperator cross_side_conditions_only(JoinKind::Left);
    cross_side_conditions_only.expression.push_back(make_condition(JoinConditionOperator::Equals, "l.k", "r.k"));
    cross_side_conditions_only.expression.push_back(make_condition(JoinConditionOperator::Less, "l.v", "r.v"));
    EXPECT_FALSE(cross_side_conditions_only.hasSingleSidePreFilterCondition());
}

TEST(ExperimentalSpillCodecPlanSetting, TheMergeAlgorithmsDeclineAMixedOnExpression)
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

    /// The extra cross-side inequality of a `LEFT` join affects matching, so it cannot become a filter over
    /// the join result and is evaluated during the join as a mixed join expression. Neither `MergeJoin` nor
    /// `FullSortingMergeJoin` evaluates one, so both decline the join and neither the spill carrier of
    /// `partial_merge` nor the sort carrier of `full_sorting_merge` may put the opt-in on the wire.
    JoinOperator left_join_with_inequality(JoinKind::Left);
    left_join_with_inequality.expression.push_back(make_condition(JoinConditionOperator::Equals, "l.k", "r.k"));
    left_join_with_inequality.expression.push_back(make_condition(JoinConditionOperator::Less, "l.v", "r.v"));
    EXPECT_TRUE(left_join_with_inequality.buildsMixedJoinExpression());
    EXPECT_FALSE(joinCarriesSetting(
        makeJoinSettings(experimental_codec, true, {JoinAlgorithm::PARTIAL_MERGE, JoinAlgorithm::HASH}),
        left_join_with_inequality));

    /// For an `INNER` join the same condition is equivalent to a filter over the join result, so no mixed
    /// expression is built and `MergeJoin` takes the join.
    JoinOperator inner_join_with_inequality(JoinKind::Inner);
    inner_join_with_inequality.expression.push_back(make_condition(JoinConditionOperator::Equals, "l.k", "r.k"));
    inner_join_with_inequality.expression.push_back(make_condition(JoinConditionOperator::Less, "l.v", "r.v"));
    EXPECT_FALSE(inner_join_with_inequality.buildsMixedJoinExpression());
    EXPECT_TRUE(joinCarriesSetting(
        makeJoinSettings(experimental_codec, true, {JoinAlgorithm::PARTIAL_MERGE, JoinAlgorithm::HASH}),
        inner_join_with_inequality));

    /// The keys of a keyed join and the single-side conditions of the clause are not mixed conditions.
    JoinOperator left_join_keyed(JoinKind::Left);
    left_join_keyed.expression.push_back(make_condition(JoinConditionOperator::Equals, "l.k", "r.k"));
    left_join_keyed.expression.push_back(make_condition(JoinConditionOperator::Less, "l.k", "l.v"));
    EXPECT_FALSE(left_join_keyed.buildsMixedJoinExpression());

    /// An ASOF join claims its one cross-side inequality as the ASOF key.
    JoinOperator asof_join(JoinKind::Left, JoinStrictness::Asof);
    asof_join.expression.push_back(make_condition(JoinConditionOperator::Equals, "l.k", "r.k"));
    asof_join.expression.push_back(make_condition(JoinConditionOperator::Less, "l.v", "r.v"));
    EXPECT_FALSE(asof_join.buildsMixedJoinExpression());
}
