#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
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

/// A join step to serialize the settings for: one column per side, and an `ON` clause made of the given binary
/// predicates over them. `JoinActionRef` only refers into the DAG, so the actions outlive the operator here.
struct Step
{
    Step(JoinKind kind, JoinStrictness strictness, const std::vector<JoinConditionOperator> & predicates = {JoinConditionOperator::Equals})
        : expression_actions(makeHeader("l"), makeHeader("r"))
        , join_operator(kind, strictness)
    {
        tryRegisterFunctions();
        const auto & inputs = expression_actions.getActionsDAG()->getInputs();
        JoinActionRef left(inputs.at(0), expression_actions);
        JoinActionRef right(inputs.at(1), expression_actions);
        for (auto op : predicates)
            join_operator.expression.push_back(JoinActionRef::transform({left, right}, JoinActionRef::AddFunction(op)));
    }

    static Block makeHeader(const String & column_name)
    {
        auto type = std::make_shared<DataTypeUInt64>();
        return Block({ColumnWithTypeAndName(type->createColumn(), type, column_name)});
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
    /// depends on which side runs it, so the plan must not be downgraded.
    EXPECT_THROW(serializeAt(makeJoinSettings({{"max_rows_in_join", 100u}}), pre_setting_version), Exception);
    EXPECT_THROW(serializeAt(makeJoinSettings({{"max_bytes_in_join", 1000u}}), pre_setting_version), Exception);

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
}

TEST(JoinSpillTriggerPlanSetting, TrailingGraceHashBehindAnAlgorithmThatRunsTheStep)
{
    /// `join_algorithm` is walked in order and the walk stops at the first entry that produces a join, so a
    /// `grace_hash` listed behind one is never consulted - not here, and not on an old peer walking the same list.
    /// Both sides build the same join, with or without a spill threshold, so the plan may be downgraded.
    ///
    /// `hash`, `parallel_hash`, `prefer_partial_merge` and `auto` end in a hash join for whatever step reaches them.
    /// `full_sorting_merge` and `partial_merge` run a plain `INNER ALL` equi-join too.
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

        /// The size limits still diverge whatever the list looks like.
        EXPECT_THROW(
            serializeAt(makeJoinSettings({{"join_algorithm", algorithms}, {"max_rows_in_join", 100u}}), pre_setting_version),
            Exception)
            << algorithms;
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

    /// The size limits diverge for these steps like for any other.
    const auto with_size_limit = makeJoinSettings({{"max_rows_in_join", 100u}});
    EXPECT_THROW(serializeAt(with_size_limit, pre_setting_version, asof.join_operator), Exception);
    EXPECT_THROW(serializeAt(with_size_limit, pre_setting_version, cross.join_operator), Exception);
}
