#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

using namespace DB;

/// `legacy_join_size_limits_trigger_spilling` selects the spill contract of a join: with it, `max_rows_in_join` /
/// `max_bytes_in_join` trigger spilling, without it they are hard caps and spilling is driven by
/// `max_bytes_before_external_join` alone.
///
/// A peer below `DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_LEGACY_JOIN_SIZE_LIMITS` does not know the name -
/// `QueryPlanSerializationSettings` is a strict named schema, so `readBinary` would reject the plan outright - but
/// it does know the settings whose meaning changed, and it applies the legacy contract to them. Omitting the name
/// alone would therefore hand such a peer a plan it happily runs with a different spill behavior, so serialization
/// fails closed whenever the two contracts can diverge, and only then.
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

QueryPlanSerializationSettings serializeAt(const JoinSettings & join_settings, UInt64 version)
{
    QueryPlanSerializationSettings settings;
    join_settings.updatePlanSettings(settings, version);
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

    /// The same threshold reaches an old peer through the preference list too.
    EXPECT_THROW(
        serializeAt(
            makeJoinSettings({{"join_algorithm", "hash,grace_hash"}, {"max_bytes_before_external_join", 1000000u}}),
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

    /// `grace_hash` without a spill threshold - `max_bytes_ratio_before_external_join` is non-zero by default, so
    /// both have to be cleared: neither side spills, the size limits are unset, so there is nothing the two
    /// contracts can disagree about.
    EXPECT_NO_THROW(serializeAt(
        makeJoinSettings({{"join_algorithm", "grace_hash"}, {"max_bytes_ratio_before_external_join", 0.0}}),
        pre_setting_version));

    /// A spill threshold without `grace_hash`: `hash` spills at `max_bytes_before_external_join` on both sides.
    EXPECT_NO_THROW(
        serializeAt(makeJoinSettings({{"join_algorithm", "hash"}, {"max_bytes_before_external_join", 1000000u}}),
                    pre_setting_version));

    /// The default `join_algorithm` does not list `grace_hash`, so the default settings pass the gate even though
    /// `max_bytes_ratio_before_external_join` is non-zero out of the box.
    EXPECT_NO_THROW(serializeAt(makeJoinSettings({}), pre_setting_version));
}
