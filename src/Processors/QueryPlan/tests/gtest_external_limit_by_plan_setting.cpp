#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

namespace DB
{
namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_limit_by;
    extern const QueryPlanSerializationSettingsDouble max_bytes_ratio_before_external_limit_by;
}
}

using namespace DB;

/// External `LIMIT BY` thresholds are serialized only to peers whose plan version supports them.
/// `QueryPlanSerializationSettings::readBinary` rejects unknown setting names, so older peers receive
/// neither threshold and keep the in-memory `LIMIT BY`. A new worker reading a plan that omits the
/// names must also stay on that path: the schema defaults are the pre-feature values, not the
/// session defaults that enable spilling.
namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_setting_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXTERNAL_LIMIT_BY - 1;

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

QueryPlanSerializationSettings serializeLimitByStep(const LimitByStep::ExternalSettings & external_settings, UInt64 version)
{
    LimitByStep step(makeHeader(), /*group_length_=*/1, /*group_offset_=*/0, Names{"k"}, external_settings);
    QueryPlanSerializationSettings settings;
    step.serializeSettings(settings, version);
    return settings;
}

bool wireCarries(const QueryPlanSerializationSettings & settings, std::string_view name)
{
    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out);
    return out.str().contains(name);
}

}

TEST(ExternalLimitByPlanSetting, CarriedTowardsAPeerThatKnowsTheNames)
{
    LimitByStep::ExternalSettings external_settings;
    external_settings.max_bytes_in_state_before_external_limit_by = 123;
    external_settings.max_bytes_ratio_before_external_limit_by = 0.3;

    const auto settings = serializeLimitByStep(external_settings, current_version);
    EXPECT_TRUE(wireCarries(settings, "max_bytes_before_external_limit_by"));
    EXPECT_TRUE(wireCarries(settings, "max_bytes_ratio_before_external_limit_by"));
}

TEST(ExternalLimitByPlanSetting, NotCarriedTowardsAPeerThatPredatesTheNames)
{
    LimitByStep::ExternalSettings external_settings;
    external_settings.max_bytes_in_state_before_external_limit_by = 123;
    external_settings.max_bytes_ratio_before_external_limit_by = 0.3;

    const auto settings = serializeLimitByStep(external_settings, pre_setting_version);
    EXPECT_FALSE(wireCarries(settings, "max_bytes_before_external_limit_by"));
    EXPECT_FALSE(wireCarries(settings, "max_bytes_ratio_before_external_limit_by"));
}

TEST(ExternalLimitByPlanSetting, DisabledStepCarriesExplicitZerosToAPeerThatKnowsTheNames)
{
    const auto settings = serializeLimitByStep(LimitByStep::ExternalSettings{}, current_version);
    EXPECT_TRUE(wireCarries(settings, "max_bytes_before_external_limit_by"));
    EXPECT_TRUE(wireCarries(settings, "max_bytes_ratio_before_external_limit_by"));
}

TEST(ExternalLimitByPlanSetting, DefaultsToPreFeatureBehaviorWhenAbsent)
{
    QueryPlanSerializationSettings settings;
    EXPECT_EQ(settings[QueryPlanSerializationSetting::max_bytes_before_external_limit_by], 0);
    EXPECT_EQ(settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_limit_by], 0.);
    EXPECT_FALSE(LimitByStep::ExternalSettings(settings).isEnabled());
}
