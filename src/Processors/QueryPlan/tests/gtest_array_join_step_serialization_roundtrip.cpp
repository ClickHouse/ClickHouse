#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

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

/// The ARRAY JOIN column `arr` plus a non-array `payload` column, which is the one lazy replication
/// can apply to (`arr` itself goes through the array-unnesting branch and is never replicated).
///
/// payload must be String. isLazyReplicationUseful declines any column that is fixed and contiguous
/// with a value size <= 8, which is exactly what ColumnVector reports, so a numeric payload would
/// take the eager branch in BOTH directions and the assertion below would be vacuous.
SharedHeader makeExecutionHeader()
{
    auto arr_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    auto payload_type = std::make_shared<DataTypeString>();
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(arr_type->createColumn(), arr_type, "arr"),
        ColumnWithTypeAndName(payload_type->createColumn(), payload_type, "payload")});
}

/// Two rows whose arrays hold several elements each, so replication really expands `payload`.
Chunk makeExecutionChunk(const Block & header)
{
    auto arr_column = header.getByName("arr").type->createColumn();
    arr_column->insert(Array{UInt64(10), UInt64(11), UInt64(12)});
    arr_column->insert(Array{UInt64(20), UInt64(21)});

    auto payload_column = header.getByName("payload").type->createColumn();
    payload_column->insert("first payload value");
    payload_column->insert("second payload value");

    return Chunk(Columns{std::move(arr_column), std::move(payload_column)}, 2);
}

/// Round-trip a step through the wire, then actually run the restored step and return what it
/// produced. The object under test is deliberately the DESERIALIZED step, because the whole point is
/// what the node executing a remote plan fragment does.
Block roundTripAndExecute(bool enable_lazy_columns_replication, const ContextPtr & context)
{
    auto header = makeExecutionHeader();

    ArrayJoinStep step(
        header,
        ArrayJoin{Names{"arr"}, /*is_left=*/false},
        /*is_unaligned_=*/false,
        /*max_block_size_=*/65536,
        enable_lazy_columns_replication);

    auto restored = deserializeStep(serializeStep(step), header);

    QueryPipelineBuilder builder;
    builder.init(Pipe(std::make_shared<SourceFromSingleChunk>(header, makeExecutionChunk(*header))));

    BuildQueryPipelineSettings settings(context);
    /// assert_cast compares typeid exactly, so the concrete type is the only thing it accepts here;
    /// that also pins that the registry gave back an ArrayJoinStep.
    assert_cast<ArrayJoinStep &>(*restored).transformPipeline(builder, settings);

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PullingPipelineExecutor executor(pipeline);

    Block block;
    while (executor.pull(block))
    {
        if (block.rows() != 0)
            return block;
    }
    return block;
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

/// The user-visible half of the contract: a deserialized step must actually replicate lazily.
///
/// The assertions above all inspect bytes, so they would stay green if the restored value stopped
/// reaching ArrayJoinAction (for example if ArrayJoinStep::transformPipeline passed a literal false).
/// This case runs the restored step and looks at the column it produced, so it is the one that fails
/// in that situation.
///
/// Both arms must return identical data: the flag selects a column representation, never a result.
TEST(ArrayJoinStepSerializationRoundTrip, DeserializedStepPerformsLazyReplication)
{
    MainThreadStatus::getInstance();
    tryRegisterFunctions();

    auto context = Context::createCopy(getContext().context);

    Block lazy_block = roundTripAndExecute(/*enable_lazy_columns_replication=*/true, context);
    Block eager_block = roundTripAndExecute(/*enable_lazy_columns_replication=*/false, context);

    const auto & lazy_payload = lazy_block.getByName("payload").column;
    const auto & eager_payload = eager_block.getByName("payload").column;

    EXPECT_TRUE(lazy_payload->isReplicated());
    EXPECT_FALSE(eager_payload->isReplicated());

    /// 3 + 2 array elements, so the ARRAY JOIN produces 5 rows and each payload value repeats.
    ASSERT_EQ(lazy_block.rows(), 5u);
    ASSERT_EQ(eager_block.rows(), 5u);

    auto lazy_payload_full = lazy_payload->convertToFullColumnIfReplicated();
    ASSERT_EQ(lazy_payload_full->size(), eager_payload->size());
    for (size_t row = 0; row < eager_payload->size(); ++row)
        EXPECT_EQ((*lazy_payload_full)[row], (*eager_payload)[row]) << "row=" << row;

    /// The ARRAY JOIN column itself is unnested, not replicated, in either direction.
    EXPECT_FALSE(lazy_block.getByName("arr").column->isReplicated());
    EXPECT_FALSE(eager_block.getByName("arr").column->isReplicated());
    for (size_t row = 0; row < eager_block.rows(); ++row)
        EXPECT_EQ((*lazy_block.getByName("arr").column)[row], (*eager_block.getByName("arr").column)[row]) << "row=" << row;
}
