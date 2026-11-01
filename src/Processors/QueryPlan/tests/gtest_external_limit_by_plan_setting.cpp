#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/tests/gtest_global_context.h>

namespace DB
{
void registerLimitByStep(QueryPlanStepRegistry & registry);

}

using namespace DB;

/// External `LIMIT BY` thresholds are serialized only to peers whose plan version supports them.
/// `QueryPlanSerializationSettings::readBinary` rejects unknown setting names, so older peers receive
/// neither threshold and use in-memory execution with its memory requirements.
///
/// Step version 1 adds the input-order bit to the flags byte, followed by the sort description of the order,
/// and is introduced at the same global version. Older peers preserve input order through their in-memory
/// execution; newer peers receive the order that a spill restores.
namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_setting_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXTERNAL_LIMIT_BY - 1;

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

/// Serializes the step without its settings at the given version.
String serializeStep(const LimitByStep & step, UInt64 version, UInt64 step_version, bool for_cache_key)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry serialized_sets;
    serialized_sets.for_cache_key = for_cache_key;
    IQueryPlanStep::Serialization serialization{out, serialized_sets, for_cache_key, version, step_version};
    step.serialize(serialization);
    return out.str();
}

LimitByStep makeStep(const SharedHeader & header, bool preserve_input_order, bool always_read_till_end = false)
{
    LimitByStep step(
        header, LimitByStep::Settings{}, /*group_length_=*/ 2, /*group_offset_=*/ 1, Names{"k"}, always_read_till_end);
    if (preserve_input_order)
    {
        SortDescription input_order;
        input_order.emplace_back("k", 1, 1);
        step.preserveInputOrder(std::move(input_order));
    }
    return step;
}

/// Reads the bytes of a step through the registry, the way a peer at the given version reads them.
std::unique_ptr<LimitByStep> deserializeStep(const String & bytes, const SharedHeader & header, UInt64 version, UInt64 step_version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry deserialized_sets;
    const SharedHeaders input_headers{header};
    const QueryPlanSerializationSettings settings;
    IQueryPlanStep::Deserialization deserialization{
        in, deserialized_sets, {}, getContext().context, input_headers, header, settings,
        /*max_type_complexity=*/ 0, version, step_version, /*skipping=*/ false};
    QueryPlanStepRegistry registry;
    registerLimitByStep(registry);
    registry.checkVersionReadable("LimitBy", step_version);
    auto restored = registry.createStep("LimitBy", deserialization);
    EXPECT_TRUE(in.eof());
    return std::unique_ptr<LimitByStep>(static_cast<LimitByStep *>(restored.release()));
}

/// Round-trips a step through its own `serialize` and `deserialize` at the given version.
std::unique_ptr<LimitByStep> roundTrip(const LimitByStep & step, const SharedHeader & header, UInt64 version, UInt64 step_version)
{
    return deserializeStep(serializeStep(step, version, step_version, /*for_cache_key=*/ false), header, version, step_version);
}

}

TEST(ExternalLimitByPlanSetting, ThresholdSettingsRespectPeerVersion)
{
    /// An assignment marks a plan setting as changed whatever the value, so a step with external `LIMIT BY`
    /// disabled (e.g. the steps of decorrelated subqueries built with the default-constructed settings) ships
    /// explicit zeros to a peer at the current version: the initiator's decision reaches the receiver
    /// instead of being left to its defaults.
    const auto header = makeHeader();
    for (const auto version : {pre_setting_version, current_version})
    for (const bool enabled : {false, true})
    {
        SCOPED_TRACE(::testing::Message() << "version=" << version << ", enabled=" << enabled);
        LimitByStep::Settings settings;
        if (enabled)
        {
            settings.max_bytes_before_external_limit_by = 123;
            settings.max_bytes_ratio_before_external_limit_by = 0.3;
        }
        LimitByStep step(header, settings, 2, 1, Names{"k"});
        QueryPlanSerializationSettings plan_settings;
        step.serializeSettings(plan_settings, version);
        WriteBufferFromOwnString out;
        plan_settings.writeChangedBinary(out);
        for (const auto * name : {"max_bytes_before_external_limit_by", "max_bytes_ratio_before_external_limit_by"})
            EXPECT_EQ(out.str().contains(name), version == current_version) << name;
    }
}

TEST(ExternalLimitByPlanSetting, InputOrderFlagRespectsPeerVersion)
{
    /// The input-order flag shares the flags byte with `always_read_till_end`, so each flag must survive
    /// independently of the other.
    /// The older peer reads the step without the input-order flag and preserves order through its in-memory
    /// `LIMIT BY` execution.
    const auto header = makeHeader();
    QueryPlanStepRegistry registry;
    registerLimitByStep(registry);
    for (const auto version : {pre_setting_version, current_version})
    for (const bool preserve_input_order : {false, true})
    for (const bool always_read_till_end : {false, true})
    {
        SCOPED_TRACE(::testing::Message() << "version=" << version << ", preserve_input_order=" << preserve_input_order
            << ", always_read_till_end=" << always_read_till_end);
        const auto step = makeStep(header, preserve_input_order, always_read_till_end);
        const auto step_version = registry.versionToWrite(step.getSerializationName(), version);
        EXPECT_EQ(step_version, version == current_version ? 1 : 0);
        const auto restored = roundTrip(step, header, version, step_version);
        EXPECT_EQ(restored->preservesInputOrder(), preserve_input_order && version == current_version);
        EXPECT_EQ(restored->alwaysReadTillEnd(), always_read_till_end);
        /// The restored step writes the same bytes, so the sort description of the order survives the round trip.
        EXPECT_EQ(
            serializeStep(*restored, version, step_version, /*for_cache_key=*/ false),
            serializeStep(step, version, step_version, /*for_cache_key=*/ false));
        if (version == pre_setting_version && preserve_input_order)
            EXPECT_EQ(
                serializeStep(step, version, step_version, /*for_cache_key=*/ false),
                serializeStep(makeStep(header, /*preserve_input_order=*/ false, always_read_till_end),
                    version, step_version, /*for_cache_key=*/ false));
    }
}

TEST(ExternalLimitByPlanSetting, InputOrderFlagIsNotPartOfTheHashTableCacheKey)
{
    /// The order of the rows does not change the hash tables built above the step, and the
    /// optimizer-derived part of the flag may differ between the single-node and the parallel-replicas
    /// plan builds, whose cache keys must match.
    const auto header = makeHeader();
    const auto with_flag = makeStep(header, /*preserve_input_order=*/ true);
    const auto without_flag = makeStep(header, /*preserve_input_order=*/ false);

    EXPECT_EQ(
        serializeStep(with_flag, current_version, /*step_version=*/ 1, /*for_cache_key=*/ true),
        serializeStep(without_flag, current_version, /*step_version=*/ 1, /*for_cache_key=*/ true));
    EXPECT_NE(
        serializeStep(with_flag, current_version, /*step_version=*/ 1, /*for_cache_key=*/ false),
        serializeStep(without_flag, current_version, /*step_version=*/ 1, /*for_cache_key=*/ false));
}

TEST(ExternalLimitByPlanSetting, SortedInputDoesNotRequireTemporaryStorage)
{
    /// Sorted-stream execution keeps only the current group's count, so an enabled spill threshold does
    /// not introduce a dependency on temporary storage, including when streams contain disjoint groups.
    const auto header = makeHeader();
    LimitByStep::Settings settings;
    settings.max_bytes_before_external_limit_by = 1;
    BuildQueryPipelineSettings build_settings(getContext().context);
    build_settings.temp_data_on_disk.reset();
    SortDescription sort_description;
    sort_description.emplace_back("k", 1, 1);

    for (const bool disjoint_groups : {false, true})
    {
        SCOPED_TRACE(disjoint_groups);
        QueryPipelineBuilder pipeline;
        pipeline.init(Pipe(std::make_shared<NullSource>(header)));
        LimitByStep step(header, settings, 2, 1, Names{"k"});
        step.applyOrder(sort_description);
        if (disjoint_groups)
            step.skipStreamMerging();

        EXPECT_NO_THROW(step.transformPipeline(pipeline, build_settings));
    }
}
