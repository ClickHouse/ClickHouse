#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{

constexpr UInt64 legacy_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_LIMIT_BY_ALWAYS_READ_TILL_END - 1;
constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "number")}));
}

std::unique_ptr<LimitByStep> makeStep(const SharedHeader & header, bool always_read_till_end)
{
    return std::make_unique<LimitByStep>(header, 2, 1, Names{"number"}, always_read_till_end);
}

String serializeStep(const IQueryPlanStep & step, UInt64 version)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = version;
    step.serialize(ctx);
    return out.str();
}

std::unique_ptr<LimitByStep> deserializeStep(const String & bytes, const SharedHeader & header, UInt64 version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    ContextPtr context = getContext().context;

    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, SharedHeaders{header}, header, settings, 0, version, false};

    auto step = LimitByStep::deserialize(ctx);
    EXPECT_TRUE(in.eof());

    return std::unique_ptr<LimitByStep>(static_cast<LimitByStep *>(step.release()));
}

}

TEST(LimitByStepSerialization, LegacyFormatPreservesReadAllBehavior)
{
    auto header = makeHeader();
    auto early_stop_step = makeStep(header, false);
    auto read_all_step = makeStep(header, true);

    const String early_stop_bytes = serializeStep(*early_stop_step, legacy_version);
    const String read_all_bytes = serializeStep(*read_all_step, legacy_version);

    /// The legacy format has no flag, so both values must produce the same bytes.
    EXPECT_EQ(early_stop_bytes, read_all_bytes);

    auto restored = deserializeStep(early_stop_bytes, header, legacy_version);
    EXPECT_TRUE(restored->alwaysReadTillEnd());
    EXPECT_EQ(early_stop_bytes, serializeStep(*restored, legacy_version));
}

TEST(LimitByStepSerialization, FlagRoundTripsAtCurrentVersion)
{
    auto header = makeHeader();

    for (bool always_read_till_end : {false, true})
    {
        auto step = makeStep(header, always_read_till_end);
        const String bytes = serializeStep(*step, current_version);
        auto restored = deserializeStep(bytes, header, current_version);

        EXPECT_EQ(restored->alwaysReadTillEnd(), always_read_till_end);
        EXPECT_EQ(bytes, serializeStep(*restored, current_version));
    }
}

}
