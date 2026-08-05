#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <Core/ProtocolDefines.h>
#include <Interpreters/WindowDescription.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/WindowStep.h>

using namespace DB;

namespace
{

UInt64 readLeadingVersion(std::string_view serialized)
{
    ReadBufferFromString in(serialized);
    UInt64 version = 0;
    readVarUInt(version, in);
    return version;
}

QueryPlan makeTrivialPlan()
{
    auto type = std::make_shared<DataTypeUInt64>();
    ColumnWithTypeAndName column(type->createColumn(), type, "x");
    auto header = std::make_shared<const Block>(Block({column}));

    QueryPlan plan;
    plan.addStep(std::make_unique<ReadFromTableStep>(header, "test_table", TableExpressionModifiers{}));
    return plan;
}

}

/// A plan that was pre-serialized once at the initiator's version (parallel replicas cache the remote
/// plan via ensureSerialized) is sent to several connections that may have negotiated different query
/// plan serialization versions. serializeForReceiver must therefore reuse the cache only for a receiver
/// that understands the cached version, and re-serialize on the fly for an older receiver - otherwise a
/// not-yet-upgraded replica in a rolling upgrade would get a version-5 stream and reject it.
TEST(QueryPlanSerializeForReceiver, CachedPlanReSerializedForOlderReceiver)
{
    auto plan = makeTrivialPlan();

    /// Cache the serialization at version 5, as the parallel-replicas path does on a v5 initiator.
    plan.ensureSerialized(5);
    ASSERT_TRUE(plan.isSerialized());
    EXPECT_EQ(readLeadingVersion(plan.getSerializedData()), 5u);

    /// A version-5 receiver understands the cached stream, so the cache is reused verbatim.
    {
        WriteBufferFromOwnString out;
        plan.serializeForReceiver(out, 5);
        out.finalize();
        EXPECT_EQ(out.str(), std::string(plan.getSerializedData()));
        EXPECT_EQ(readLeadingVersion(out.str()), 5u);
    }

    /// A receiver older than version 5 (a pre-PR server) must NOT get the cached version-5 stream. The
    /// plan is re-serialized on the fly at the receiver's version, which omits the version-5 settings, so
    /// the stream differs from the cache and carries the older version header.
    {
        WriteBufferFromOwnString out;
        plan.serializeForReceiver(out, 4);
        out.finalize();
        EXPECT_EQ(readLeadingVersion(out.str()), 4u);
        EXPECT_NE(out.str(), std::string(plan.getSerializedData()));
    }
}

/// A step that cannot be written below its own minimum serialization version at all (WindowStep is
/// only registered under QueryPlanStepRegistry since version 4) must raise the plan's required
/// version, so that a serialization path without version negotiation (the stateless-worker task in
/// DistributedPlanExecutor, pinned to version 3) serializes such a plan at the version it needs
/// instead of hitting the fail-closed check in WindowStep::serialize.
TEST(QueryPlanRequiredSerializationVersion, WindowStepRaisesRequiredVersion)
{
    auto plan = makeTrivialPlan();
    EXPECT_EQ(plan.getRequiredSerializationVersion(), 1u);

    plan.addStep(std::make_unique<WindowStep>(
        plan.getCurrentHeader(), WindowDescription{}, std::vector<WindowFunctionDescription>{}, false));
    EXPECT_EQ(plan.getRequiredSerializationVersion(), UInt64(DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_WINDOW_STEP));
}

/// When the plan was never pre-serialized, serializeForReceiver serializes on the fly at the receiver's
/// version - this is the ordinary (non-cached) distributed path.
TEST(QueryPlanSerializeForReceiver, UncachedPlanSerializedAtReceiverVersion)
{
    auto plan = makeTrivialPlan();
    ASSERT_FALSE(plan.isSerialized());

    WriteBufferFromOwnString out_v1;
    plan.serializeForReceiver(out_v1, 1);
    out_v1.finalize();
    EXPECT_EQ(readLeadingVersion(out_v1.str()), 1u);

    WriteBufferFromOwnString out_v5;
    plan.serializeForReceiver(out_v5, 5);
    out_v5.finalize();
    EXPECT_EQ(readLeadingVersion(out_v5.str()), 5u);
}
