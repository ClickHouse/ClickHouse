#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>

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
/// not-yet-upgraded replica in a rolling upgrade would get a version-2 stream and reject it.
TEST(QueryPlanSerializeForReceiver, CachedPlanReSerializedForOlderReceiver)
{
    auto plan = makeTrivialPlan();

    /// Cache the serialization at version 2, as the parallel-replicas path does on a v2 initiator.
    plan.ensureSerialized(2);
    ASSERT_TRUE(plan.isSerialized());
    EXPECT_EQ(readLeadingVersion(plan.getSerializedData()), 2u);

    /// A version-2 receiver understands the cached stream, so the cache is reused verbatim.
    {
        WriteBufferFromOwnString out;
        plan.serializeForReceiver(out, 2);
        out.finalize();
        EXPECT_EQ(out.str(), std::string(plan.getSerializedData()));
        EXPECT_EQ(readLeadingVersion(out.str()), 2u);
    }

    /// A version-1 receiver (a pre-PR server) must NOT get the cached version-2 stream. The plan is
    /// re-serialized on the fly at version 1, which omits the version-2 settings, so the stream differs
    /// from the cache and carries the version-1 header.
    {
        WriteBufferFromOwnString out;
        plan.serializeForReceiver(out, 1);
        out.finalize();
        EXPECT_EQ(readLeadingVersion(out.str()), 1u);
        EXPECT_NE(out.str(), std::string(plan.getSerializedData()));
    }
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

    WriteBufferFromOwnString out_v2;
    plan.serializeForReceiver(out_v2, 2);
    out_v2.finalize();
    EXPECT_EQ(readLeadingVersion(out_v2.str()), 2u);
}
