#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <Core/SettingsQuirks.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

namespace DB
{
void registerDistinctStep(QueryPlanStepRegistry & registry);

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsNonZeroUInt64 max_block_size;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_distinct;
    extern const QueryPlanSerializationSettingsDouble max_bytes_ratio_before_external_distinct;
    extern const QueryPlanSerializationSettingsNonZeroUInt64 temporary_files_buffer_size;
}
}

using namespace DB;

/// External `DISTINCT` thresholds are serialized only to peers whose plan version supports them.
/// `QueryPlanSerializationSettings::readBinary` rejects unknown setting names, so older peers receive
/// neither threshold and use in-memory execution with its memory requirements.
///
/// Step version 1 carries the input-order flag and is introduced at the same global version. Older
/// peers preserve input order through their in-memory execution; newer peers receive the requirement
/// for order restoration after spilling.
namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_setting_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXTERNAL_DISTINCT - 1;

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

QueryPlanSerializationSettings serializeDistinctStep(const DistinctStep::Settings & distinct_settings, UInt64 version)
{
    DistinctStep step(makeHeader(), distinct_settings, /*limit_hint_=*/0, Names{"k"}, /*pre_distinct_=*/false);

    QueryPlanSerializationSettings settings;
    step.serializeSettings(settings, version);
    return settings;
}

/// Serializes the step without its settings at the given version.
String serializeStep(const DistinctStep & step, UInt64 version, UInt64 step_version, bool for_cache_key)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry serialized_sets;
    serialized_sets.for_cache_key = for_cache_key;
    IQueryPlanStep::Serialization serialization{out, serialized_sets, for_cache_key, version, step_version};
    step.serialize(serialization);
    return out.str();
}

DistinctStep makeStep(const SharedHeader & header, bool preserve_input_order, bool pre_distinct = false)
{
    DistinctStep step(header, DistinctStep::Settings{}, /*limit_hint_=*/0, Names{"k"}, pre_distinct);
    if (preserve_input_order)
        step.preserveInputOrder();
    return step;
}

/// Round-trips a step through its own `serialize` and `deserialize` at the given version, the way a peer
/// at that version reads it.
bool inputOrderFlagAfterRoundTrip(const DistinctStep & step, const SharedHeader & header, UInt64 version, UInt64 step_version)
{
    const String bytes = serializeStep(step, version, step_version, /*for_cache_key=*/ false);
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry deserialized_sets;
    const SharedHeaders input_headers{header};
    const QueryPlanSerializationSettings settings;
    IQueryPlanStep::Deserialization deserialization{
        in, deserialized_sets, {}, getContext().context, input_headers, header, settings,
        /*max_type_complexity=*/ 0, version, step_version, /*skipping=*/ false};
    QueryPlanStepRegistry registry;
    registerDistinctStep(registry);
    registry.checkVersionReadable(step.getSerializationName(), step_version);
    const auto restored = registry.createStep(step.getSerializationName(), deserialization);
    EXPECT_TRUE(in.eof());
    return dynamic_cast<const DistinctStep &>(*restored).preservesInputOrder();
}

/// Checks whether the binary settings stream contains the given setting name.
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
    /// An assignment marks a plan setting as changed whatever the value, so a step with external `DISTINCT`
    /// disabled (e.g. the internal `DISTINCT` steps built with the default-constructed settings) ships
    /// explicit zeros to a peer at the current version: the initiator's decision reaches the receiver
    /// instead of being left to its defaults.
    const auto settings = serializeDistinctStep(DistinctStep::Settings{}, current_version);
    EXPECT_TRUE(wireCarries(settings, "max_bytes_before_external_distinct"));
    EXPECT_TRUE(wireCarries(settings, "max_bytes_ratio_before_external_distinct"));
}

TEST(ExternalDistinctPlanSetting, DefaultsToPreFeatureBehaviorWhenAbsent)
{
    /// A new worker reading a plan of an initiator that predates external `DISTINCT` does not receive the
    /// thresholds at all. Its defaults must preserve the behavior of that initiator (no spilling) instead
    /// of arming a mode the initiator could not have selected.
    QueryPlanSerializationSettings settings;
    EXPECT_EQ(settings[QueryPlanSerializationSetting::max_bytes_before_external_distinct], 0);
    EXPECT_EQ(settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_distinct], 0.);
}

TEST(ExternalDistinctPlanSetting, TemporaryFilesBufferSizeIsClampedOnDeserialization)
{
    /// The plan settings bypass the sanity clamp of the query settings, so the step clamps the buffer size
    /// itself, like the other consumers of temporary files: a serialized plan cannot make the receiver
    /// allocate an out-of-range temporary-file buffer.
    QueryPlanSerializationSettings plan_settings;
    plan_settings[QueryPlanSerializationSetting::temporary_files_buffer_size] = MAX_TEMPORARY_FILES_BUFFER_SIZE + 1;
    EXPECT_EQ(DistinctStep::Settings(plan_settings).temporary_files_buffer_size, MAX_TEMPORARY_FILES_BUFFER_SIZE);
}

TEST(ExternalDistinctPlanSetting, MaxBlockSizeRoundTripsOnDeserialization)
{
    /// The plan-level `max_block_size` is a non-zero setting like its query-level counterpart, so restoring
    /// the step only has to preserve the serialized value.
    for (const UInt64 max_block_size : {UInt64{1}, UInt64{DEFAULT_BLOCK_SIZE}})
    {
        SCOPED_TRACE(max_block_size);
        QueryPlanSerializationSettings plan_settings;
        plan_settings[QueryPlanSerializationSetting::max_block_size] = max_block_size;

        WriteBufferFromOwnString out;
        plan_settings.writeChangedBinary(out);
        ReadBufferFromString in(out.str());
        QueryPlanSerializationSettings restored_settings;
        restored_settings.readBinary(in);

        EXPECT_EQ(DistinctStep::Settings(restored_settings).max_block_size, max_block_size);
    }
}

TEST(ExternalDistinctPlanSetting, InputOrderFlagRoundTripsAtTheCurrentVersion)
{
    const auto header = makeHeader();
    QueryPlanStepRegistry registry;
    registerDistinctStep(registry);
    for (const bool preliminary : {false, true})
    {
        const auto step = makeStep(header, /*preserve_input_order=*/ true, preliminary);
        const auto step_version = registry.versionToWrite(step.getSerializationName(), current_version);
        EXPECT_EQ(step_version, 1);
        EXPECT_TRUE(inputOrderFlagAfterRoundTrip(step, header, current_version, step_version));
    }
}

TEST(ExternalDistinctPlanSetting, InputOrderFlagIsNotCarriedTowardsAnOlderPeer)
{
    /// The older peer reads the step in its own format, without the flag; it runs the in-memory `DISTINCT`,
    /// which keeps the input order by construction.
    const auto header = makeHeader();
    QueryPlanStepRegistry registry;
    registerDistinctStep(registry);
    for (const bool preliminary : {false, true})
    {
        const auto step = makeStep(header, /*preserve_input_order=*/ true, preliminary);
        const auto step_version = registry.versionToWrite(step.getSerializationName(), pre_setting_version);
        EXPECT_EQ(step_version, 0);
        EXPECT_FALSE(inputOrderFlagAfterRoundTrip(step, header, pre_setting_version, step_version));
    }
}

TEST(ExternalDistinctPlanSetting, VersionZeroIsReadableAtTheCurrentPlanVersion)
{
    /// Peers can share a global plan version while supporting different step versions. Version 0
    /// carries no input-order flag, even when the global version supports external `DISTINCT`.
    const auto header = makeHeader();
    for (const bool preliminary : {false, true})
        EXPECT_FALSE(inputOrderFlagAfterRoundTrip(
            makeStep(header, /*preserve_input_order=*/ true, preliminary), header, current_version, /*step_version=*/ 0));
}

TEST(ExternalDistinctPlanSetting, UnknownStepVersionIsRejected)
{
    QueryPlanStepRegistry registry;
    registerDistinctStep(registry);
    for (const String name : {"Distinct", "PreDistinct"})
        EXPECT_THROW(registry.checkVersionReadable(name, 2), Exception);
}

TEST(ExternalDistinctPlanSetting, InputOrderFlagIsNotPartOfTheHashTableCacheKey)
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

TEST(ExternalDistinctPlanSetting, SpillBlockByteTargetRoundTrips)
{
    for (const UInt64 bytes : {UInt64{0}, UInt64{1048576}})
    {
        DistinctStep::Settings original;
        original.prefer_external_sort_block_bytes = bytes;
        const auto settings = serializeDistinctStep(original, current_version);
        WriteBufferFromOwnString out;
        settings.writeChangedBinary(out);
        ReadBufferFromString in(out.str());
        QueryPlanSerializationSettings restored;
        restored.readBinary(in);
        EXPECT_EQ(DistinctStep::Settings(restored).prefer_external_sort_block_bytes, bytes);
    }
}
