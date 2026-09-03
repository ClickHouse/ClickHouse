#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ArrayJoin.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/Serialization.h>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "arr")}));
}

/// Serializes the step with the given negotiated version and returns the leading flags byte.
UInt8 serializeAndGetFlags(const ArrayJoinStep & step, UInt64 version)
{
    String buffer;
    WriteBufferFromString out(buffer);
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = version;
    step.serialize(ctx);
    out.finalize();
    EXPECT_FALSE(buffer.empty());
    return static_cast<UInt8>(buffer[0]);
}

}

/// `array_join_use_nulls` changes semantics only for `LEFT ARRAY JOIN`, but both planner paths hand
/// `ArrayJoinStep` the raw setting value. The flag must therefore be normalized to
/// `is_left && array_join_use_nulls`, otherwise a plain `ARRAY JOIN arr SETTINGS array_join_use_nulls = 1`
/// would set the serialized bit and get rejected by the version gate against an older peer even though
/// its semantics did not change at all.
TEST(ArrayJoinStep, UseNullsFlagIsNormalizedToLeftArrayJoin)
{
    auto header = makeHeader();

    /// Regular `ARRAY JOIN`: the flag is dropped, so the bit is not set and no version gate applies.
    {
        ArrayJoinStep step(
            header,
            ArrayJoin{Names{"arr"}, /*is_left=*/false, /*array_join_use_nulls=*/true},
            /*is_unaligned=*/false,
            /*max_block_size=*/65536,
            /*enable_lazy_columns_replication=*/false);

        /// The output header keeps the plain, non-`Nullable` element type.
        EXPECT_EQ(step.getOutputHeader()->getByName("arr").type->getName(), "UInt64");

        UInt8 flags = serializeAndGetFlags(step, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
        EXPECT_EQ(flags & 32, 0);

        /// An older peer must still accept it.
        EXPECT_NO_THROW(serializeAndGetFlags(step, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ARRAY_JOIN_USE_NULLS - 1));
    }

    /// `LEFT ARRAY JOIN`: the flag is kept, the bit is set, and an older peer is rejected.
    {
        ArrayJoinStep step(
            header,
            ArrayJoin{Names{"arr"}, /*is_left=*/true, /*array_join_use_nulls=*/true},
            /*is_unaligned=*/false,
            /*max_block_size=*/65536,
            /*enable_lazy_columns_replication=*/false);

        EXPECT_EQ(step.getOutputHeader()->getByName("arr").type->getName(), "Nullable(UInt64)");

        UInt8 flags = serializeAndGetFlags(step, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
        EXPECT_EQ(flags & 32, 32);

        EXPECT_THROW(
            serializeAndGetFlags(step, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ARRAY_JOIN_USE_NULLS - 1),
            Exception);
    }
}
