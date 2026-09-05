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
#include <Processors/QueryPlan/Serialization.h>
#include <Common/tests/gtest_global_context.h>

namespace DB
{
namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_distinct;
    extern const QueryPlanSerializationSettingsDouble max_bytes_ratio_before_external_distinct;
    extern const QueryPlanSerializationSettingsNonZeroUInt64 temporary_files_buffer_size;
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
///
/// The input-order flag of the step (see DistinctStep::preserveInputOrder) is gated the same way: it is
/// written only towards a peer at the version that introduced it, and a peer below that version cannot
/// spill, so it keeps the input order anyway.
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

/// The bytes of the step's own `serialize` (without its settings) at the given version.
String serializeStep(const DistinctStep & step, UInt64 version, bool for_cache_key)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry serialized_sets;
    serialized_sets.for_cache_key = for_cache_key;
    IQueryPlanStep::Serialization serialization{out, serialized_sets, for_cache_key, version};
    step.serialize(serialization);
    return out.str();
}

DistinctStep makeStep(const SharedHeader & header, bool preserve_input_order)
{
    DistinctStep step(header, DistinctStep::Settings{}, /*limit_hint_=*/0, Names{"k"}, /*pre_distinct_=*/false);
    if (preserve_input_order)
        step.preserveInputOrder();
    return step;
}

/// Round-trips a step through its own `serialize` and `deserialize` at the given version, the way a peer
/// at that version reads it.
bool inputOrderFlagAfterRoundTrip(const DistinctStep & step, const SharedHeader & header, UInt64 version)
{
    const String bytes = serializeStep(step, version, /*for_cache_key=*/ false);
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry deserialized_sets;
    const SharedHeaders input_headers{header};
    const QueryPlanSerializationSettings settings;
    IQueryPlanStep::Deserialization deserialization{
        in, deserialized_sets, {}, getContext().context, input_headers, header, settings, /*max_type_complexity=*/ 0, version, /*skipping=*/ false};
    const auto restored = DistinctStep::deserializeNormal(deserialization);
    return dynamic_cast<const DistinctStep &>(*restored).preservesInputOrder();
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

TEST(ExternalDistinctPlanSetting, TemporaryFilesBufferSizeIsClampedOnDeserialization)
{
    /// The plan settings bypass the sanity clamp of the query settings, so the step clamps the buffer size
    /// itself, like the other consumers of temporary files: a serialized plan cannot make the receiver
    /// allocate an out-of-range temporary-file buffer.
    QueryPlanSerializationSettings plan_settings;
    plan_settings[QueryPlanSerializationSetting::temporary_files_buffer_size] = MAX_TEMPORARY_FILES_BUFFER_SIZE + 1;
    EXPECT_EQ(DistinctStep::Settings(plan_settings).temporary_files_buffer_size, MAX_TEMPORARY_FILES_BUFFER_SIZE);
}

TEST(ExternalDistinctPlanSetting, InputOrderFlagRoundTripsAtTheCurrentVersion)
{
    const auto header = makeHeader();
    EXPECT_TRUE(inputOrderFlagAfterRoundTrip(makeStep(header, /*preserve_input_order=*/ true), header, current_version));
}

TEST(ExternalDistinctPlanSetting, InputOrderFlagIsNotCarriedTowardsAnOlderPeer)
{
    /// The older peer reads the step in its own format, without the flag; it runs the in-memory
    /// DISTINCT, which keeps the input order by construction.
    const auto header = makeHeader();
    EXPECT_FALSE(inputOrderFlagAfterRoundTrip(makeStep(header, /*preserve_input_order=*/ true), header, pre_setting_version));
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
        serializeStep(with_flag, current_version, /*for_cache_key=*/ true),
        serializeStep(without_flag, current_version, /*for_cache_key=*/ true));
    EXPECT_NE(
        serializeStep(with_flag, current_version, /*for_cache_key=*/ false),
        serializeStep(without_flag, current_version, /*for_cache_key=*/ false));
}
