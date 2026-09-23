#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

/// `legacy_join_size_limits_trigger_spilling` selects the spill contract of a join: with it, `max_rows_in_join` /
/// `max_bytes_in_join` trigger spilling, without it they are hard caps and spilling is driven by
/// `max_bytes_before_external_join` alone.
///
/// A peer below `DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_LEGACY_JOIN_SIZE_LIMITS` does not know the name -
/// `QueryPlanSerializationSettings` is a strict named schema, so `readBinary` would reject the plan outright - but
/// it does know the settings whose meaning changed, and it applies the legacy contract to them. Omitting the name
/// alone would therefore hand such a peer a plan it happily runs with a different spill behavior, so serialization
/// fails closed whenever the two contracts can diverge for the step being serialized, and only then.
namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_setting_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_LEGACY_JOIN_SIZE_LIMITS - 1;

JoinSettings makeJoinSettings(const std::vector<std::pair<String, Field>> & changes)
{
    Settings query_settings;
    for (const auto & [name, value] : changes)
        query_settings.set(name, value);
    return JoinSettings(query_settings);
}

/// The name as it appears in the binary settings stream written by `writeChangedBinary`.
bool wireCarriesSetting(const QueryPlanSerializationSettings & settings)
{
    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out);
    return out.str().contains("legacy_join_size_limits_trigger_spilling");
}

/// A join step to serialize the settings for: two columns per side, and an `ON` clause made of the given binary
/// predicates over the first column of each side. `JoinActionRef` only refers into the DAG, so the actions outlive
/// the operator here.
struct Step
{
    Step(JoinKind kind, JoinStrictness strictness, const std::vector<JoinConditionOperator> & predicates = {JoinConditionOperator::Equals})
        : expression_actions(makeHeader("l"), makeHeader("r"))
        , join_operator(kind, strictness)
    {
        tryRegisterFunctions();
        for (auto op : predicates)
            join_operator.expression.push_back(predicate(op, 0));
    }

    static Block makeHeader(const String & column_name)
    {
        auto type = std::make_shared<DataTypeUInt64>();
        return Block(
            {ColumnWithTypeAndName(type->createColumn(), type, column_name),
             ColumnWithTypeAndName(type->createColumn(), type, column_name + "2")});
    }

    /// `l <op> r` over the `index`-th column of each side. The inputs of the DAG are the left header followed by
    /// the right one.
    JoinActionRef predicate(JoinConditionOperator op, size_t index) const
    {
        const auto & inputs = expression_actions.getActionsDAG()->getInputs();
        JoinActionRef left(inputs.at(index), expression_actions);
        JoinActionRef right(inputs.at(2 + index), expression_actions);
        return JoinActionRef::transform({left, right}, JoinActionRef::AddFunction(op));
    }

    /// `ON (l = r) OR (l2 = r2)`: a single disjunction, which `JoinStepLogical::buildPhysicalJoinImpl` splits into one
    /// `TableJoin` clause per disjunct (`tryAddDisjunctiveConditions`), so the step never has a single clause.
    Step & onDisjunction()
    {
        join_operator.expression.clear();
        join_operator.expression.push_back(JoinActionRef::transform(
            {predicate(JoinConditionOperator::Equals, 0), predicate(JoinConditionOperator::Equals, 1)},
            JoinActionRef::AddFunction(JoinConditionOperator::Or)));
        return *this;
    }

    /// `ON l = r AND ((l = r) OR (l2 = r2))`: the equality is a join key, the disjunction a residual condition of the
    /// same single clause.
    Step & onEqualityAndDisjunction()
    {
        onDisjunction();
        join_operator.expression.push_back(predicate(JoinConditionOperator::Equals, 0));
        return *this;
    }

    /// `ON NULL`: a single constant of type `Nullable(Nothing)` that is not a binary predicate, the shape
    /// `JoinStepLogical::buildPhysicalJoinImpl` turns into an always-false `ConstantJoin`.
    Step & onNull()
    {
        join_operator.expression.clear();
        DataTypePtr type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
        ColumnConstPtr constant = type->createColumnConstWithDefaultValue(1);
        join_operator.expression.emplace_back(
            &expression_actions.getActionsDAG()->addColumn(std::move(constant), type, "NULL"), expression_actions);
        return *this;
    }

    JoinExpressionActions expression_actions;
    JoinOperator join_operator;
};

/// A plain `INNER ALL` equi-join: every algorithm of the preference list can run it.
const JoinOperator & equiJoin()
{
    static const Step step(JoinKind::Inner, JoinStrictness::All);
    return step.join_operator;
}

QueryPlanSerializationSettings serializeAt(const JoinSettings & join_settings, UInt64 version, const JoinOperator & join_operator = equiJoin())
{
    QueryPlanSerializationSettings settings;
    join_settings.updatePlanSettings(settings, version, join_operator);
    return settings;
}

}

TEST(JoinSpillTriggerPlanSetting, AlwaysOnTheWireTowardsCurrentPeers)
{
    /// A peer at the current version knows the name, so the value is always written and the contract is explicit,
    /// whatever the size limits and thresholds are.
    EXPECT_TRUE(wireCarriesSetting(serializeAt(makeJoinSettings({}), current_version)));
    EXPECT_TRUE(wireCarriesSetting(serializeAt(makeJoinSettings({{"max_rows_in_join", 100u}}), current_version)));
    EXPECT_TRUE(wireCarriesSetting(
        serializeAt(makeJoinSettings({{"legacy_join_size_limits_trigger_spilling", true}}), current_version)));

    /// And it is read back as it was sent.
    EXPECT_FALSE(JoinSettings(serializeAt(makeJoinSettings({}), current_version), current_version)
                     .legacy_join_size_limits_trigger_spilling);
    EXPECT_TRUE(JoinSettings(
                    serializeAt(makeJoinSettings({{"legacy_join_size_limits_trigger_spilling", true}}), current_version),
                    current_version)
                    .legacy_join_size_limits_trigger_spilling);
}

TEST(JoinSpillTriggerPlanSetting, NotOnTheWireTowardsOldPeersAndReadBackAsLegacy)
{
    /// Nothing that depends on the contract: the plan goes out without the name, and the old peer's own default -
    /// the legacy contract - is what a receiver reconstructs from it.
    const auto settings = serializeAt(makeJoinSettings({}), pre_setting_version);
    EXPECT_FALSE(wireCarriesSetting(settings));
    EXPECT_TRUE(JoinSettings(settings, pre_setting_version).legacy_join_size_limits_trigger_spilling);
}

TEST(JoinSpillTriggerPlanSetting, RefusedTowardsOldPeersWhenTheContractsDiverge)
{
    /// A size limit is a hard cap here and a spill trigger there: whether the query truncates, throws or spills
    /// depends on which side runs it, so the plan must not be downgraded. The default settings are spill-capable
    /// (`max_bytes_ratio_before_external_join` is non-zero out of the box, and `parallel_hash` runs the step), so a
    /// bare size limit already diverges.
    EXPECT_THROW(serializeAt(makeJoinSettings({{"max_rows_in_join", 100u}}), pre_setting_version), Exception);
    EXPECT_THROW(serializeAt(makeJoinSettings({{"max_bytes_in_join", 1000u}}), pre_setting_version), Exception);
    EXPECT_THROW(
        serializeAt(
            makeJoinSettings({{"join_algorithm", "hash"}, {"max_bytes_ratio_before_external_join", 0.0},
                              {"max_bytes_before_external_join", 1000000u}, {"max_rows_in_join", 100u}}),
            pre_setting_version),
        Exception);

    /// Nothing in the list is known to run the step: `direct` needs a key-value right side. Whether the old peer
    /// spills on the limit cannot be told from the step, so the gate stays on the safe side.
    EXPECT_THROW(
        serializeAt(makeJoinSettings({{"join_algorithm", "direct"}, {"max_rows_in_join", 100u}}), pre_setting_version),
        Exception);

    /// Standalone `grace_hash` takes its spill threshold from `max_bytes_before_external_join` /
    /// `max_bytes_ratio_before_external_join` here and ignores both there, where the (unset) size limits are its
    /// only trigger, so it would never spill.
    EXPECT_THROW(
        serializeAt(
            makeJoinSettings({{"join_algorithm", "grace_hash"}, {"max_bytes_before_external_join", 1000000u}}),
            pre_setting_version),
        Exception);
    EXPECT_THROW(
        serializeAt(
            makeJoinSettings({{"join_algorithm", "grace_hash"}, {"max_bytes_ratio_before_external_join", 0.5}}),
            pre_setting_version),
        Exception);

    /// The same threshold reaches an old peer through the preference list too, as long as `grace_hash` is the
    /// first entry that runs this step.
    EXPECT_THROW(
        serializeAt(
            makeJoinSettings({{"join_algorithm", "grace_hash,hash"}, {"max_bytes_before_external_join", 1000000u}}),
            pre_setting_version),
        Exception);
    EXPECT_THROW(
        serializeAt(
            makeJoinSettings({{"join_algorithm", "direct,grace_hash,hash"}, {"max_bytes_before_external_join", 1000000u}}),
            pre_setting_version),
        Exception);

    /// `grace_hash` without a spill threshold is not a runnable algorithm here - listed alone the query is
    /// refused, in a preference list it is demoted to the next entry - while an old peer runs it as a standalone
    /// `GraceHashJoin` that never spills. `max_bytes_ratio_before_external_join` is non-zero by default, so both
    /// thresholds have to be cleared to reach this case.
    EXPECT_THROW(
        serializeAt(
            makeJoinSettings({{"join_algorithm", "grace_hash"}, {"max_bytes_ratio_before_external_join", 0.0}}),
            pre_setting_version),
        Exception);
    EXPECT_THROW(
        serializeAt(
            makeJoinSettings({{"join_algorithm", "grace_hash,hash"}, {"max_bytes_ratio_before_external_join", 0.0}}),
            pre_setting_version),
        Exception);
}

TEST(JoinSpillTriggerPlanSetting, AllowedTowardsOldPeersWhenBothContractsAgree)
{
    /// Asked for the legacy contract explicitly - which is exactly what an old peer applies - so the limits may go
    /// out with their old meaning, and the receiver reads the same contract back.
    for (const auto & change : {std::pair<String, Field>{"max_rows_in_join", 100u},
                                std::pair<String, Field>{"max_bytes_in_join", 1000u}})
    {
        const auto join_settings = makeJoinSettings({{"legacy_join_size_limits_trigger_spilling", true}, change});
        const auto settings = serializeAt(join_settings, pre_setting_version);
        EXPECT_FALSE(wireCarriesSetting(settings));
        EXPECT_TRUE(JoinSettings(settings, pre_setting_version).legacy_join_size_limits_trigger_spilling);
    }

    /// `grace_hash` in legacy mode is what an old peer runs anyway: the size limits are its spill trigger on both
    /// sides, and the threshold that only this side would apply is ignored here too.
    EXPECT_NO_THROW(serializeAt(
        makeJoinSettings({{"legacy_join_size_limits_trigger_spilling", true},
                          {"join_algorithm", "grace_hash"},
                          {"max_bytes_before_external_join", 1000000u}}),
        pre_setting_version));

    /// A spill threshold without `grace_hash`: `hash` spills at `max_bytes_before_external_join` on both sides.
    EXPECT_NO_THROW(
        serializeAt(makeJoinSettings({{"join_algorithm", "hash"}, {"max_bytes_before_external_join", 1000000u}}),
                    pre_setting_version));

    /// The default `join_algorithm` does not list `grace_hash`, so the default settings pass the gate even though
    /// `max_bytes_ratio_before_external_join` is non-zero out of the box.
    EXPECT_NO_THROW(serializeAt(makeJoinSettings({}), pre_setting_version));

    /// A size limit on a step that never spills on the old peer either: without a spill threshold `hash` /
    /// `parallel_hash` build a plain in-memory join there, which checks the limits as hard caps, exactly like here.
    for (const auto & algorithms : {"hash", "parallel_hash", "direct,parallel_hash,hash", "auto"})
    {
        for (const auto & limit : {std::pair<String, Field>{"max_rows_in_join", 100u},
                                   std::pair<String, Field>{"max_bytes_in_join", 1000u}})
        {
            EXPECT_NO_THROW(serializeAt(
                makeJoinSettings({{"join_algorithm", algorithms}, {"max_bytes_ratio_before_external_join", 0.0}, limit}),
                pre_setting_version))
                << algorithms << " " << limit.first;
        }
    }

    /// The merge algorithms never spill on the size limits, on either side, so a step they run may carry one
    /// whatever the spill threshold says.
    for (const auto & algorithms : {"full_sorting_merge", "parallel_full_sorting_merge", "partial_merge", "direct,partial_merge,hash"})
    {
        EXPECT_NO_THROW(serializeAt(
            makeJoinSettings({{"join_algorithm", algorithms}, {"max_bytes_before_external_join", 1000000u}, {"max_rows_in_join", 100u}}),
            pre_setting_version))
            << algorithms;
    }
}

TEST(JoinSpillTriggerPlanSetting, TrailingGraceHashBehindAnAlgorithmThatRunsTheStep)
{
    /// `join_algorithm` is walked in order and the walk stops at the first entry that produces a join, so a
    /// `grace_hash` listed behind one is never consulted - not here, and not on an old peer walking the same list.
    /// Both sides build the same join, with or without a spill threshold, so the plan may be downgraded.
    ///
    /// `hash`, `parallel_hash` and `auto` end in a hash join for whatever step reaches them. `full_sorting_merge`,
    /// `partial_merge` and `prefer_partial_merge` run a plain `INNER ALL` equi-join too.
    for (const auto & algorithms : {"hash,grace_hash", "parallel_hash,grace_hash", "prefer_partial_merge,grace_hash",
                                    "auto,grace_hash", "direct,hash,grace_hash", "full_sorting_merge,grace_hash",
                                    "parallel_full_sorting_merge,grace_hash", "partial_merge,grace_hash",
                                    "direct,full_sorting_merge,grace_hash"})
    {
        EXPECT_NO_THROW(
            serializeAt(makeJoinSettings({{"join_algorithm", algorithms}, {"max_bytes_before_external_join", 1000000u}}),
                        pre_setting_version))
            << algorithms;
        EXPECT_NO_THROW(
            serializeAt(makeJoinSettings({{"join_algorithm", algorithms}, {"max_bytes_ratio_before_external_join", 0.0}}),
                        pre_setting_version))
            << algorithms;

    }

    /// The size limits diverge behind a hash-family entry with the default, non-zero spill ratio: the old peer
    /// spills on them once the collected right side crosses the threshold, this side caps.
    for (const auto & algorithms : {"hash,grace_hash", "parallel_hash,grace_hash", "auto,grace_hash", "direct,hash,grace_hash"})
    {
        EXPECT_THROW(
            serializeAt(makeJoinSettings({{"join_algorithm", algorithms}, {"max_rows_in_join", 100u}}), pre_setting_version),
            Exception)
            << algorithms;
    }

    /// Behind a merge algorithm that runs the step they are hard caps on both sides - and `prefer_partial_merge`
    /// stays on `MergeJoin` for a plain `INNER ALL` equi-join, on both sides.
    for (const auto & algorithms : {"full_sorting_merge,grace_hash", "parallel_full_sorting_merge,grace_hash",
                                    "partial_merge,grace_hash", "direct,full_sorting_merge,grace_hash",
                                    "prefer_partial_merge,grace_hash", "prefer_partial_merge"})
    {
        EXPECT_NO_THROW(
            serializeAt(makeJoinSettings({{"join_algorithm", algorithms}, {"max_rows_in_join", 100u}}), pre_setting_version))
            << algorithms;
    }
}

TEST(JoinSpillTriggerPlanSetting, PreferPartialMergeFallsBackToTheHashFamilyOnlyWhereMergeJoinDeclines)
{
    /// `prefer_partial_merge` tries `MergeJoin` first and takes the hash branch only where `MergeJoin::isSupported`
    /// declines the step - on both sides. Where it stays on `MergeJoin` the size limits are hard caps there too;
    /// where it falls back, the old peer's `SpillingHashJoin` spills on them once it has a threshold (the default,
    /// non-zero ratio counts), so a size limit refuses the downgrade.
    for (const auto & algorithms : {"prefer_partial_merge", "prefer_partial_merge,grace_hash"})
    {
        const auto with_size_limit = makeJoinSettings({{"join_algorithm", algorithms}, {"max_rows_in_join", 100u}});

        /// `MergeJoin` runs ALL joins of the four outer kinds, and ANY / SEMI ones for INNER and LEFT.
        const Step inner_all(JoinKind::Inner, JoinStrictness::All);
        const Step full_all(JoinKind::Full, JoinStrictness::All);
        const Step left_any(JoinKind::Left, JoinStrictness::Any);
        const Step left_semi(JoinKind::Left, JoinStrictness::Semi);
        for (const auto * step : std::initializer_list<const Step *>{&inner_all, &full_all, &left_any, &left_semi})
            EXPECT_NO_THROW(serializeAt(with_size_limit, pre_setting_version, step->join_operator)) << algorithms;

        /// It declines ANY / SEMI joins of the other kinds, and the hash family runs them.
        const Step right_any(JoinKind::Right, JoinStrictness::Any);
        const Step full_any(JoinKind::Full, JoinStrictness::Any);
        const Step right_semi(JoinKind::Right, JoinStrictness::Semi);
        for (const auto * step : std::initializer_list<const Step *>{&right_any, &full_any, &right_semi})
            EXPECT_THROW(serializeAt(with_size_limit, pre_setting_version, step->join_operator), Exception) << algorithms;

        /// Whether `MergeJoin` takes a step whose `ON` clause is more than plain equalities depends on more than the
        /// step tells, so the hash fallback is assumed and the plan is refused.
        const Step mixed(JoinKind::Inner, JoinStrictness::All, {JoinConditionOperator::Equals, JoinConditionOperator::Less});
        EXPECT_THROW(serializeAt(with_size_limit, pre_setting_version, mixed.join_operator), Exception) << algorithms;
        Step residual(JoinKind::Inner, JoinStrictness::All);
        residual.onEqualityAndDisjunction();
        EXPECT_THROW(serializeAt(with_size_limit, pre_setting_version, residual.join_operator), Exception) << algorithms;

        /// Without a spill threshold the hash fallback never spills either, on either side.
        const auto no_threshold = makeJoinSettings(
            {{"join_algorithm", algorithms}, {"max_rows_in_join", 100u}, {"max_bytes_ratio_before_external_join", 0.0}});
        EXPECT_NO_THROW(serializeAt(no_threshold, pre_setting_version, right_any.join_operator)) << algorithms;
    }
}

TEST(JoinSpillTriggerPlanSetting, MergeAlgorithmsRunOnlySomeSteps)
{
    /// The merge algorithms take a step by its kind and strictness, so the same list is safe for one step and
    /// reaches `grace_hash` for another. `full_sorting_merge` runs ANY / ALL joins of the four outer kinds and
    /// declines SEMI; `partial_merge` runs ALL for the four kinds, and ANY / SEMI only for INNER and LEFT.
    const auto full_sorting_merge_first
        = makeJoinSettings({{"join_algorithm", "full_sorting_merge,grace_hash"}, {"max_bytes_before_external_join", 1000000u}});
    const auto partial_merge_first
        = makeJoinSettings({{"join_algorithm", "partial_merge,grace_hash"}, {"max_bytes_before_external_join", 1000000u}});

    const Step left_any(JoinKind::Left, JoinStrictness::Any);
    EXPECT_NO_THROW(serializeAt(full_sorting_merge_first, pre_setting_version, left_any.join_operator));
    EXPECT_NO_THROW(serializeAt(partial_merge_first, pre_setting_version, left_any.join_operator));

    const Step left_semi(JoinKind::Left, JoinStrictness::Semi);
    EXPECT_THROW(serializeAt(full_sorting_merge_first, pre_setting_version, left_semi.join_operator), Exception);
    EXPECT_NO_THROW(serializeAt(partial_merge_first, pre_setting_version, left_semi.join_operator));

    const Step right_any(JoinKind::Right, JoinStrictness::Any);
    EXPECT_NO_THROW(serializeAt(full_sorting_merge_first, pre_setting_version, right_any.join_operator));
    EXPECT_THROW(serializeAt(partial_merge_first, pre_setting_version, right_any.join_operator), Exception);

    const Step full_all(JoinKind::Full, JoinStrictness::All);
    EXPECT_NO_THROW(serializeAt(full_sorting_merge_first, pre_setting_version, full_all.join_operator));
    EXPECT_NO_THROW(serializeAt(partial_merge_first, pre_setting_version, full_all.join_operator));

    /// Neither merge algorithm runs an ANTI join, so `grace_hash` is what both lists come down to.
    const Step left_anti(JoinKind::Left, JoinStrictness::Anti);
    EXPECT_THROW(serializeAt(full_sorting_merge_first, pre_setting_version, left_anti.join_operator), Exception);
    EXPECT_THROW(serializeAt(partial_merge_first, pre_setting_version, left_anti.join_operator), Exception);
}

TEST(JoinSpillTriggerPlanSetting, MergeAlgorithmsDeclineMoreThanPlainEqualities)
{
    /// A merge algorithm declines an `ON` clause with anything but equalities between the two sides - a mixed
    /// condition is never evaluated by it - and the step falls through to `grace_hash`. The gate does not replay
    /// that decision in detail; it refuses the plan as soon as the clause is not plain equalities.
    const auto full_sorting_merge_first
        = makeJoinSettings({{"join_algorithm", "full_sorting_merge,grace_hash"}, {"max_bytes_before_external_join", 1000000u}});

    const Step with_inequality(JoinKind::Inner, JoinStrictness::All, {JoinConditionOperator::Equals, JoinConditionOperator::Less});
    EXPECT_THROW(serializeAt(full_sorting_merge_first, pre_setting_version, with_inequality.join_operator), Exception);

    /// The hash family does not care what the clause looks like.
    const auto hash_first = makeJoinSettings({{"join_algorithm", "hash,grace_hash"}, {"max_bytes_before_external_join", 1000000u}});
    EXPECT_NO_THROW(serializeAt(hash_first, pre_setting_version, with_inequality.join_operator));
}

TEST(JoinSpillTriggerPlanSetting, StepsGraceHashCannotRun)
{
    /// `GraceHashJoin::isSupported` declines an ASOF join and any kind outside INNER / LEFT / RIGHT / FULL, on both
    /// sides, so listing `grace_hash` for such a step changes nothing about how it runs.
    const auto grace_hash_only = makeJoinSettings({{"join_algorithm", "grace_hash"}, {"max_bytes_before_external_join", 1000000u}});
    const auto grace_hash_first
        = makeJoinSettings({{"join_algorithm", "grace_hash,full_sorting_merge,hash"}, {"max_bytes_before_external_join", 1000000u}});

    const Step asof(JoinKind::Left, JoinStrictness::Asof, {JoinConditionOperator::Equals, JoinConditionOperator::Less});
    EXPECT_NO_THROW(serializeAt(grace_hash_only, pre_setting_version, asof.join_operator));
    EXPECT_NO_THROW(serializeAt(grace_hash_first, pre_setting_version, asof.join_operator));

    /// A CROSS join and a join on a constant do not consult the preference list at all.
    const Step cross(JoinKind::Cross, JoinStrictness::All, {});
    EXPECT_NO_THROW(serializeAt(grace_hash_only, pre_setting_version, cross.join_operator));
    const Step on_constant(JoinKind::Inner, JoinStrictness::All, {});
    EXPECT_NO_THROW(serializeAt(grace_hash_only, pre_setting_version, on_constant.join_operator));
    Step on_null(JoinKind::Left, JoinStrictness::All, {});
    on_null.onNull();
    EXPECT_NO_THROW(serializeAt(grace_hash_only, pre_setting_version, on_null.join_operator));
    EXPECT_NO_THROW(serializeAt(grace_hash_first, pre_setting_version, on_null.join_operator));

    /// The size limits do not diverge for these steps either: the old peer cannot switch them to `GraceHashJoin`,
    /// so its plain hash join (or `ConstantJoin`) checks them as hard caps, like this side. A plain equi-join with
    /// the same settings is spill-capable there and is refused.
    const auto with_size_limit = makeJoinSettings({{"max_rows_in_join", 100u}});
    EXPECT_NO_THROW(serializeAt(with_size_limit, pre_setting_version, asof.join_operator));
    EXPECT_NO_THROW(serializeAt(with_size_limit, pre_setting_version, cross.join_operator));
    EXPECT_NO_THROW(serializeAt(with_size_limit, pre_setting_version, on_constant.join_operator));
    EXPECT_NO_THROW(serializeAt(with_size_limit, pre_setting_version, on_null.join_operator));
    EXPECT_NO_THROW(serializeAt(makeJoinSettings({{"max_bytes_in_join", 1000u}}), pre_setting_version, on_null.join_operator));
    EXPECT_THROW(serializeAt(with_size_limit, pre_setting_version), Exception);

    /// An ASOF join is never a join on a constant, even with such a clause; it keeps walking the list like any
    /// other step, and `grace_hash` does not run it.
    Step asof_on_null(JoinKind::Left, JoinStrictness::Asof, {});
    asof_on_null.onNull();
    EXPECT_NO_THROW(serializeAt(grace_hash_only, pre_setting_version, asof_on_null.join_operator));
}

TEST(JoinSpillTriggerPlanSetting, StepsWithoutASingleEqualityClause)
{
    /// `GraceHashJoin::isSupported` also requires a single join clause (`TableJoin::oneDisjunct`), which a step has
    /// only when its `ON` clause carries an equality between the two sides at the top level. Without one, neither
    /// side ever spills the step: a disjunction is split into one clause per disjunct and run by a plain
    /// multi-clause hash join, an inequality-only join becomes an `IEJoinStep` (or a CROSS join with a residual
    /// filter), and every one of those checks the size limits as hard caps, like this side.
    Step disjunction(JoinKind::Inner, JoinStrictness::All, {});
    disjunction.onDisjunction();
    Step left_disjunction(JoinKind::Left, JoinStrictness::All, {});
    left_disjunction.onDisjunction();
    const Step inequalities(JoinKind::Inner, JoinStrictness::All, {JoinConditionOperator::Less, JoinConditionOperator::Greater});
    const Step one_inequality(JoinKind::Inner, JoinStrictness::All, {JoinConditionOperator::Less});

    /// A size limit with the default, spill-capable settings, and with a hash-family list and an explicit threshold.
    const auto with_size_limit = makeJoinSettings({{"max_rows_in_join", 100u}});
    const auto hash_with_threshold_and_limit
        = makeJoinSettings({{"join_algorithm", "hash"}, {"max_bytes_before_external_join", 1000000u}, {"max_bytes_in_join", 1000u}});
    const auto parallel_hash_with_threshold_and_limit = makeJoinSettings(
        {{"join_algorithm", "parallel_hash,hash"}, {"max_bytes_before_external_join", 1000000u}, {"max_rows_in_join", 100u}});
    for (const Step * step : std::initializer_list<const Step *>{&disjunction, &left_disjunction, &inequalities, &one_inequality})
    {
        EXPECT_NO_THROW(serializeAt(with_size_limit, pre_setting_version, step->join_operator));
        EXPECT_NO_THROW(serializeAt(hash_with_threshold_and_limit, pre_setting_version, step->join_operator));
        EXPECT_NO_THROW(serializeAt(parallel_hash_with_threshold_and_limit, pre_setting_version, step->join_operator));
    }

    /// `ie_join` claims an inequality-only step before the preference list is consulted, and a list nothing else in
    /// which runs the step is not a reason to refuse it either: the limits are hard caps for `IEJoinStep` on both sides.
    for (const auto & algorithms : {"ie_join", "ie_join,hash", "hash,ie_join", "direct"})
    {
        EXPECT_NO_THROW(serializeAt(
            makeJoinSettings({{"join_algorithm", algorithms}, {"max_rows_in_join", 100u}}), pre_setting_version, inequalities.join_operator))
            << algorithms;
    }

    /// `grace_hash` cannot run such a step on either side, so listing it changes nothing.
    const auto grace_hash_only = makeJoinSettings({{"join_algorithm", "grace_hash"}, {"max_bytes_before_external_join", 1000000u}});
    const auto grace_hash_first
        = makeJoinSettings({{"join_algorithm", "grace_hash,hash"}, {"max_bytes_ratio_before_external_join", 0.0}, {"max_rows_in_join", 100u}});
    for (const Step * step : std::initializer_list<const Step *>{&disjunction, &left_disjunction, &inequalities, &one_inequality})
    {
        EXPECT_NO_THROW(serializeAt(grace_hash_only, pre_setting_version, step->join_operator));
        EXPECT_NO_THROW(serializeAt(grace_hash_first, pre_setting_version, step->join_operator));
    }

    /// One top-level equality is enough for a single clause: the disjunction is then a residual condition of a
    /// spill-capable hash join, and an equality next to inequalities is a join key unless `ie_join` is listed first -
    /// which an old peer may not honor, so the gate does not rely on it. Both stay refused with a size limit.
    Step equality_and_disjunction(JoinKind::Inner, JoinStrictness::All, {});
    equality_and_disjunction.onEqualityAndDisjunction();
    EXPECT_THROW(serializeAt(with_size_limit, pre_setting_version, equality_and_disjunction.join_operator), Exception);
    EXPECT_THROW(serializeAt(hash_with_threshold_and_limit, pre_setting_version, equality_and_disjunction.join_operator), Exception);
    EXPECT_THROW(serializeAt(grace_hash_only, pre_setting_version, equality_and_disjunction.join_operator), Exception);

    const Step equality_and_inequalities(
        JoinKind::Inner, JoinStrictness::All, {JoinConditionOperator::Equals, JoinConditionOperator::Less, JoinConditionOperator::Greater});
    EXPECT_THROW(serializeAt(with_size_limit, pre_setting_version, equality_and_inequalities.join_operator), Exception);
    EXPECT_THROW(
        serializeAt(
            makeJoinSettings({{"join_algorithm", "ie_join,hash"}, {"max_rows_in_join", 100u}}), pre_setting_version,
            equality_and_inequalities.join_operator),
        Exception);

    /// `IS NOT DISTINCT FROM` is a join key too.
    const Step null_safe(JoinKind::Inner, JoinStrictness::All, {JoinConditionOperator::NullSafeEquals});
    EXPECT_THROW(serializeAt(with_size_limit, pre_setting_version, null_safe.join_operator), Exception);
}

TEST(JoinSpillTriggerPlanSetting, NullSafeEqualityIsAKeyForTheMergeAlgorithmsToo)
{
    /// `ON l IS NOT DISTINCT FROM r` becomes an ordinary join key of the single clause on both sides
    /// (`addJoinPredicatesToTableJoin`; nullable keys are wrapped into `tuple(...)` first), and neither merge
    /// algorithm tells it apart from `=`. A merge algorithm listed first therefore runs the step here and on an old
    /// peer, and a spill-capable entry behind it is never consulted, whatever the limits and thresholds say.
    const Step null_safe(JoinKind::Inner, JoinStrictness::All, {JoinConditionOperator::NullSafeEquals});
    const Step both_keys(JoinKind::Left, JoinStrictness::All, {JoinConditionOperator::Equals, JoinConditionOperator::NullSafeEquals});

    for (const auto & algorithms : {"full_sorting_merge,hash", "full_sorting_merge,grace_hash", "parallel_full_sorting_merge,grace_hash",
                                    "partial_merge,grace_hash", "partial_merge,hash", "direct,full_sorting_merge,grace_hash"})
    {
        for (const auto * step : std::initializer_list<const Step *>{&null_safe, &both_keys})
        {
            EXPECT_NO_THROW(
                serializeAt(makeJoinSettings({{"join_algorithm", algorithms}, {"max_rows_in_join", 100u}}), pre_setting_version, step->join_operator))
                << algorithms;
            EXPECT_NO_THROW(
                serializeAt(
                    makeJoinSettings({{"join_algorithm", algorithms}, {"max_bytes_before_external_join", 1000000u}, {"max_bytes_in_join", 100u}}),
                    pre_setting_version, step->join_operator))
                << algorithms;
        }
    }

    /// The merge algorithms still take the step by kind and strictness: for a SEMI join `full_sorting_merge` steps
    /// aside and the list is walked on to the spill-capable entry.
    const Step left_semi(JoinKind::Left, JoinStrictness::Semi, {JoinConditionOperator::NullSafeEquals});
    EXPECT_THROW(
        serializeAt(
            makeJoinSettings({{"join_algorithm", "full_sorting_merge,grace_hash"}, {"max_bytes_before_external_join", 1000000u}}),
            pre_setting_version, left_semi.join_operator),
        Exception);
    EXPECT_NO_THROW(
        serializeAt(
            makeJoinSettings({{"join_algorithm", "partial_merge,grace_hash"}, {"max_bytes_before_external_join", 1000000u}}),
            pre_setting_version, left_semi.join_operator));

    /// And a null-safe key still makes the step one `GraceHashJoin` can run, so the hash family with a spill
    /// threshold and a size limit keeps failing closed.
    for (const auto & algorithms : {"hash,full_sorting_merge", "parallel_hash", "grace_hash"})
    {
        EXPECT_THROW(
            serializeAt(makeJoinSettings({{"join_algorithm", algorithms}, {"max_rows_in_join", 100u}}), pre_setting_version, null_safe.join_operator),
            Exception)
            << algorithms;
    }
}
