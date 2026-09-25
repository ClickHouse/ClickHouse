#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/typeid_cast.h>

namespace DB
{
void registerDistinctStep(QueryPlanStepRegistry & registry);
}

using namespace DB;

/// The preliminary `DISTINCT` of a set operation input gives up on a stream after its first chunk. The
/// window is part of the step from version 2 on, so that a plan fragment shipped to a peer runs the step
/// the way the initiator planned it.
namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_window_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SET_OPERATION_JOIN - 1;

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

DistinctStep makeStep(const SharedHeader & header, bool abandon_after_first_chunk)
{
    DistinctStep step(header, DistinctStep::Settings{}, /*limit_hint_=*/ 0, Names{"k"}, /*pre_distinct_=*/ true);
    if (abandon_after_first_chunk)
        step.abandonAfterFirstChunk();
    return step;
}

String serializeStep(const DistinctStep & step, UInt64 version, UInt64 step_version)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry serialized_sets;
    IQueryPlanStep::Serialization serialization{out, serialized_sets, /*for_cache_key=*/ false, version, step_version};
    step.serialize(serialization);
    return out.str();
}

bool abandonsAfterFirstChunkAfterRoundTrip(const DistinctStep & step, const SharedHeader & header, UInt64 version, UInt64 step_version)
{
    const String bytes = serializeStep(step, version, step_version);
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
    return dynamic_cast<const DistinctStep &>(*restored).abandonsAfterFirstChunk();
}

}

TEST(DistinctStepAbandonWindowSerialization, WindowRoundTripsAtTheCurrentVersion)
{
    const auto header = makeHeader();
    QueryPlanStepRegistry registry;
    registerDistinctStep(registry);
    for (const bool abandon_after_first_chunk : {false, true})
    {
        const auto step = makeStep(header, abandon_after_first_chunk);
        const auto step_version = registry.versionToWrite(step.getSerializationName(), current_version);
        EXPECT_EQ(step_version, 2);
        EXPECT_EQ(abandonsAfterFirstChunkAfterRoundTrip(step, header, current_version, step_version), abandon_after_first_chunk);
    }
}

TEST(DistinctStepAbandonWindowSerialization, WindowIsNotCarriedTowardsAnOlderPeer)
{
    /// The older peer reads the step in its own format and observes the default number of chunks.
    const auto header = makeHeader();
    QueryPlanStepRegistry registry;
    registerDistinctStep(registry);
    const auto step = makeStep(header, /*abandon_after_first_chunk=*/ true);
    const auto step_version = registry.versionToWrite(step.getSerializationName(), pre_window_version);
    EXPECT_EQ(step_version, 1);
    EXPECT_EQ(
        serializeStep(step, pre_window_version, step_version),
        serializeStep(makeStep(header, false), pre_window_version, step_version));
    EXPECT_FALSE(abandonsAfterFirstChunkAfterRoundTrip(step, header, pre_window_version, step_version));
}

TEST(DistinctStepAbandonWindowSerialization, CloneCarriesTheWindow)
{
    const auto header = makeHeader();
    const auto step = makeStep(header, /*abandon_after_first_chunk=*/ true);
    const auto cloned = step.clone();
    const auto * cloned_distinct = typeid_cast<const DistinctStep *>(cloned.get());
    ASSERT_TRUE(cloned_distinct);
    EXPECT_TRUE(cloned_distinct->abandonsAfterFirstChunk());
}
