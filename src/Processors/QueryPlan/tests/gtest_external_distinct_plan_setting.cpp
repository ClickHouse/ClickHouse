#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

namespace DB
{
namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_distinct;
    extern const QueryPlanSerializationSettingsDouble max_bytes_ratio_before_external_distinct;
}
}

using namespace DB;

/// `max_bytes_before_external_distinct` and `max_bytes_ratio_before_external_distinct` may go on the wire
/// only towards a peer whose query-plan serialization version knows the names.
///
/// `QueryPlanSerializationSettings` is a strict named schema: `writeChangedBinary` writes every touched
/// entry by name and `readBinary` throws on a name it does not know. Writing either name towards a peer
/// that predates it would make every serialized DISTINCT plan unreadable there. Leaving them off costs
/// nothing: such a peer has no external DISTINCT at all, so it runs the in-memory DISTINCT, exactly as
/// with the feature disabled, and the result is identical either way.
namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_setting_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXTERNAL_DISTINCT - 1;

QueryPlanSerializationSettings serializeDistinctStep(const DistinctStep::Settings & distinct_settings, UInt64 version)
{
    auto type = std::make_shared<DataTypeUInt64>();
    Block header({ColumnWithTypeAndName(type->createColumn(), type, "k")});

    DistinctStep step(
        std::make_shared<const Block>(header), distinct_settings, /*limit_hint_=*/0, Names{"k"}, /*pre_distinct_=*/false);

    QueryPlanSerializationSettings settings;
    step.serializeSettings(settings, version);
    return settings;
}

/// The names as they appear in the binary settings stream written by `writeChangedBinary`.
bool wireCarries(const QueryPlanSerializationSettings & settings, std::string_view name)
{
    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out);
    return out.str().contains(name);
}

}

TEST(ExternalDistinctPlanSetting, CarriedTowardsAPeerThatKnowsTheNames)
{
    DistinctStep::Settings distinct_settings;
    distinct_settings.max_bytes_before_external_distinct = 123;
    distinct_settings.max_bytes_ratio_before_external_distinct = 0.3;

    const auto settings = serializeDistinctStep(distinct_settings, current_version);
    EXPECT_TRUE(wireCarries(settings, "max_bytes_before_external_distinct"));
    EXPECT_TRUE(wireCarries(settings, "max_bytes_ratio_before_external_distinct"));
}

TEST(ExternalDistinctPlanSetting, NotCarriedTowardsAPeerThatPredatesTheNames)
{
    /// Neither name may appear towards an older peer - it would reject the whole plan.
    DistinctStep::Settings distinct_settings;
    distinct_settings.max_bytes_before_external_distinct = 123;
    distinct_settings.max_bytes_ratio_before_external_distinct = 0.3;

    const auto settings = serializeDistinctStep(distinct_settings, pre_setting_version);
    EXPECT_FALSE(wireCarries(settings, "max_bytes_before_external_distinct"));
    EXPECT_FALSE(wireCarries(settings, "max_bytes_ratio_before_external_distinct"));
}

TEST(ExternalDistinctPlanSetting, DisabledStepCarriesExplicitZerosToAPeerThatKnowsTheNames)
{
    /// An assignment marks a plan setting as changed whatever the value, so a step with external
    /// DISTINCT disabled (e.g. the internal DISTINCT steps built with the default-constructed
    /// settings) ships explicit zeros to a peer at the current version: the initiator's decision
    /// reaches the receiver instead of being left to its defaults.
    const auto settings = serializeDistinctStep(DistinctStep::Settings{}, current_version);
    EXPECT_TRUE(wireCarries(settings, "max_bytes_before_external_distinct"));
    EXPECT_TRUE(wireCarries(settings, "max_bytes_ratio_before_external_distinct"));
}

TEST(ExternalDistinctPlanSetting, DefaultsToPreFeatureBehaviorWhenAbsent)
{
    /// A new worker reading a plan of an initiator that predates external DISTINCT does not receive the
    /// thresholds at all. Its defaults must preserve the behavior of that initiator (no spilling)
    /// instead of arming a mode the initiator could not have selected.
    QueryPlanSerializationSettings settings;
    EXPECT_EQ(settings[QueryPlanSerializationSetting::max_bytes_before_external_distinct], 0);
    EXPECT_EQ(settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_distinct], 0.);
}
