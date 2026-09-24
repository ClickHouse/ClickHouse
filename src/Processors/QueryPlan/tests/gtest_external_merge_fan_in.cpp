#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

namespace DB
{
void registerDistinctStep(QueryPlanStepRegistry & registry);
void registerSortingStep(QueryPlanStepRegistry & registry);
void registerJoinStep(QueryPlanStepRegistry & registry);

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 max_external_merge_fan_in;
}
}

using namespace DB;

namespace
{

SharedHeader makeHeader(const String & name)
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(type->createColumn(), type, name)});
}

class ExternalMergeFanIn : public testing::TestWithParam<String>
{
protected:
    QueryPlanStepRegistry registry;

    void SetUp() override
    {
        registerDistinctStep(registry);
        registerSortingStep(registry);
        registerJoinStep(registry);
    }

    QueryPlanStepPtr makeStep(size_t fan_in) const
    {
        auto header = makeHeader("k");
        if (GetParam() == "Distinct" || GetParam() == "PreDistinct")
        {
            DistinctStep::Settings settings;
            settings.max_external_merge_fan_in = fan_in;
            return std::make_unique<DistinctStep>(
                header, settings, /*limit_hint_=*/ 0, Names{"k"}, GetParam() == "PreDistinct");
        }

        QueryPlanSerializationSettings plan_settings;
        SortingStep::Settings settings(plan_settings);
        settings.max_external_merge_fan_in = fan_in;
        if (GetParam() == "Sorting")
        {
            SortDescription description;
            description.emplace_back("k", 1, 1);
            return std::make_unique<SortingStep>(header, description, /*limit_=*/ 0, settings);
        }

        auto right_header = makeHeader("r");
        JoinExpressionActions actions(*header, *right_header);
        auto dag = actions.getActionsDAG();
        for (const auto * input : dag->getInputs())
            dag->getOutputs().push_back(input);
        return std::make_unique<JoinStepLogical>(
            header, right_header, JoinOperator{}, std::move(actions), ActionsDAG::NodeRawConstPtrs{},
            JoinSettings(plan_settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION), settings);
    }

    String serializeStep(const IQueryPlanStep & step, UInt64 plan_version, UInt64 step_version) const
    {
        QueryPlanSerializationSettings settings;
        step.serializeSettings(settings, plan_version);
        WriteBufferFromOwnString out;
        settings.writeChangedBinary(out);
        SerializedSetsRegistry sets;
        IQueryPlanStep::Serialization ctx{out, sets};
        ctx.version = plan_version;
        ctx.step_version = step_version;
        step.serialize(ctx);
        return out.str();
    }

    QueryPlanStepPtr deserializeStep(
        const String & bytes, const IQueryPlanStep & original, UInt64 plan_version, UInt64 step_version) const
    {
        registry.checkVersionReadable(GetParam(), step_version);
        ReadBufferFromString in(bytes);
        QueryPlanSerializationSettings settings;
        settings.readBinary(in);
        DeserializedSetsRegistry sets;
        IQueryPlanStep::Deserialization ctx{
            in, sets, {}, getContext().context, original.getInputHeaders(), original.getOutputHeader(), settings,
            /*max_type_complexity=*/ 0, plan_version, step_version, /*skipping=*/ false};
        auto step = registry.createStep(GetParam(), ctx);
        EXPECT_TRUE(in.eof());
        return step;
    }

    UInt64 restoredFanIn(const IQueryPlanStep & step) const
    {
        QueryPlanSerializationSettings settings;
        const auto version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
        step.serializeSettings(settings, version);
        return settings[QueryPlanSerializationSetting::max_external_merge_fan_in];
    }
};

}

TEST_P(ExternalMergeFanIn, CurrentStepVersionPreservesTheSetting)
{
    const auto version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    const auto step_version = registry.versionToWrite(GetParam(), version);
    EXPECT_EQ(step_version, GetParam() == "Distinct" || GetParam() == "PreDistinct" ? 2 : 1);
    for (const size_t fan_in : {0, 2, 64, 128})
    {
        SCOPED_TRACE(fan_in);
        const auto step = makeStep(fan_in);
        const auto bytes = serializeStep(*step, version, step_version);
        EXPECT_NE(bytes.find("max_external_merge_fan_in"), String::npos);
        const auto restored = deserializeStep(bytes, *step, version, step_version);
        EXPECT_EQ(restoredFanIn(*restored), fan_in);
    }
}

TEST_P(ExternalMergeFanIn, OlderReceiverRejectsTheNewStepVersion)
{
    QueryPlanStepRegistry older_registry;
    QueryPlanStepRegistry::StepVersions versions{{0, 0}};
    if (GetParam() == "Distinct" || GetParam() == "PreDistinct")
        versions.push_back({1, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXTERNAL_DISTINCT});
    older_registry.registerStep(GetParam(), {}, std::move(versions));

    /// A peer can share the global version without knowing the setting. Its step-version check
    /// rejects the format before the settings reader encounters an unknown name.
    const auto step_version = registry.versionToWrite(GetParam(), DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    EXPECT_THROW(older_registry.checkVersionReadable(GetParam(), step_version), Exception);
}

TEST_P(ExternalMergeFanIn, OlderPlanVersionsSelectOlderStepFormats)
{
    const auto step = makeStep(2);
    for (const UInt64 version : {18, 19})
    {
        SCOPED_TRACE(version);
        const auto step_version = registry.versionToWrite(GetParam(), version);
        const bool distinct = GetParam() == "Distinct" || GetParam() == "PreDistinct";
        EXPECT_EQ(step_version, distinct && version == 19 ? 1 : 0);
        const auto bytes = serializeStep(*step, version, step_version);
        EXPECT_EQ(bytes.find("max_external_merge_fan_in"), String::npos);
        const auto restored = deserializeStep(bytes, *step, version, step_version);
        EXPECT_EQ(restoredFanIn(*restored), 0);
    }
}

INSTANTIATE_TEST_SUITE_P(
    Steps, ExternalMergeFanIn, testing::Values("Sorting", "Distinct", "PreDistinct", "Join"),
    [](const testing::TestParamInfo<String> & test_parameter) { return test_parameter.param; });

TEST(ExternalMergeFanInSettings, InvalidSerializedValueIsRejected)
{
    QueryPlanSerializationSettings settings;
    settings[QueryPlanSerializationSetting::max_external_merge_fan_in] = 1;
    EXPECT_THROW((void)SortingStep::Settings{settings}, Exception);
    EXPECT_THROW((void)DistinctStep::Settings{settings}, Exception);
}
