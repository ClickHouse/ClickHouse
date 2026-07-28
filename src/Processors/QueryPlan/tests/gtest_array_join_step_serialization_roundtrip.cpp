#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{

/// A header with one Array(UInt64) column, which is the only shape ArrayJoinStep accepts:
/// its constructor runs ArrayJoinAction::prepare, which throws TYPE_MISMATCH for non-Array/Map.
SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "arr")}));
}

/// Serialize a step through the production path and return its byte stream.
String serializeStep(const IQueryPlanStep & step)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    step.serialize(ctx);
    return out.str();
}

/// Deserialize a byte stream through the production path.
///
/// The QueryPlanSerializationSettings object is left at its DECLARE defaults on purpose: that is
/// what QueryPlan::deserialize hands each step (it builds a fresh object per step and fills it only
/// from the names that step's serializeSettings wrote). Since ArrayJoinStep::serializeSettings
/// writes only max_block_size, a defaulted settings object is itself part of the assertion.
QueryPlanStepPtr deserializeStep(const String & bytes, const SharedHeader & header)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    SharedHeaders input_headers{header};
    ContextPtr context = getContext().context;

    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, input_headers, header, settings, 0, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, false};

    return ArrayJoinStep::deserialize(ctx);
}

struct RoundTrip
{
    String first;   /// bytes written by the original step
    String second;  /// bytes written by re-serializing the restored step
};

/// serialize -> deserialize -> serialize.
///
/// enable_lazy_columns_replication is private and deliberately has no getter, so the restored
/// step's value is observed by serializing it again: the second stream can carry the flag bit only
/// if the restored member is true. That also pins the whole wire format, not just one accessor.
RoundTrip roundTrip(bool is_left, bool is_unaligned, bool enable_lazy_columns_replication)
{
    auto header = makeHeader();
    ArrayJoinStep step(
        header,
        ArrayJoin{Names{"arr"}, is_left},
        is_unaligned,
        /*max_block_size_=*/65536,
        enable_lazy_columns_replication);

    String first = serializeStep(step);
    auto restored = deserializeStep(first, header);
    return {first, serializeStep(*restored)};
}

UInt8 flagsByte(const String & bytes)
{
    return static_cast<UInt8>(bytes.at(0));
}

}

/// Regression test for enable_lazy_columns_replication being lost when a query plan is serialized.
///
/// ArrayJoinStep::deserialize used to take the value from the per-step
/// QueryPlanSerializationSettings object, which ArrayJoinStep::serializeSettings never populates,
/// so a deserialized ArrayJoinStep always fell back to the plan DECLARE default (false) and the
/// executing node did eager column replication no matter what the initiator ran with. The value now
/// travels in the step's own flags byte.
TEST(ArrayJoinStepSerializationRoundTrip, LazyColumnsReplicationTrueSurvives)
{
    auto result = roundTrip(/*is_left=*/false, /*is_unaligned=*/false, /*enable_lazy_columns_replication=*/true);

    EXPECT_EQ(flagsByte(result.first) & 4, 4);
    EXPECT_EQ(result.first, result.second);
}

TEST(ArrayJoinStepSerializationRoundTrip, LazyColumnsReplicationFalseSurvives)
{
    auto result = roundTrip(/*is_left=*/false, /*is_unaligned=*/false, /*enable_lazy_columns_replication=*/false);

    EXPECT_EQ(flagsByte(result.first) & 4, 0);
    EXPECT_EQ(result.first, result.second);
}

/// The new bit must collide with neither of the two bits already in the byte.
TEST(ArrayJoinStepSerializationRoundTrip, FlagsBitsAreIndependent)
{
    for (bool is_left : {false, true})
    {
        for (bool is_unaligned : {false, true})
        {
            for (bool lazy : {false, true})
            {
                auto result = roundTrip(is_left, is_unaligned, lazy);
                const UInt8 expected = static_cast<UInt8>((is_left ? 1 : 0) + (is_unaligned ? 2 : 0) + (lazy ? 4 : 0));

                EXPECT_EQ(flagsByte(result.first), expected)
                    << "is_left=" << is_left << " is_unaligned=" << is_unaligned << " lazy=" << lazy;
                EXPECT_EQ(result.first, result.second)
                    << "is_left=" << is_left << " is_unaligned=" << is_unaligned << " lazy=" << lazy;
            }
        }
    }
}

/// Backward compatibility: a plan written by a server that predates the flag bit has bit 4 clear.
/// Such a stream must still deserialize, and must yield eager replication (false), which is exactly
/// what those servers do. This pins the graceful-degradation contract the whole approach rests on.
TEST(ArrayJoinStepSerializationRoundTrip, OldFormatByteWithoutBit4YieldsFalse)
{
    auto header = makeHeader();

    for (UInt8 old_flags : {UInt8(0), UInt8(1), UInt8(2), UInt8(3)})
    {
        /// The exact byte stream a pre-flag initiator emits: flags, then the column count, then the
        /// column names.
        WriteBufferFromOwnString out;
        writeIntBinary(old_flags, out);
        writeVarUInt(1, out);
        writeStringBinary(String("arr"), out);

        auto restored = deserializeStep(out.str(), header);
        EXPECT_EQ(flagsByte(serializeStep(*restored)) & 4, 0) << "old_flags=" << static_cast<int>(old_flags);
    }
}
