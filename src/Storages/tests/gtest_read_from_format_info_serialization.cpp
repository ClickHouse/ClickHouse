#include <gtest/gtest.h>

#include <list>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

namespace DB::ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

using namespace DB;

namespace
{

String serializeInfo(const ReadFromFormatInfo & info, UInt64 step_version, bool for_cache_key = false)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.for_cache_key = for_cache_key;
    ctx.step_version = step_version;
    info.serialize(ctx);
    return out.str();
}

ReadFromFormatInfo deserializeInfo(const String & bytes, UInt64 step_version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    SharedHeaders input_headers;
    SharedHeader output_header = std::make_shared<const Block>();
    ContextPtr context = getContext().context;
    IQueryPlanStep::Deserialization ctx{in, registry, {}, context, input_headers, output_header, settings, 0, 0, step_version, false};
    auto info = ReadFromFormatInfo::deserialize(ctx);
    EXPECT_TRUE(in.eof());
    return info;
}

}

TEST(ReadFromFormatInfoSerialization, StringSizesColumnsRoundTrip)
{
    ReadFromFormatInfo info;
    info.string_sizes_columns = {"b.size", "a.size"};
    auto restored = deserializeInfo(serializeInfo(info, 1), 1);
    EXPECT_EQ(restored.string_sizes_columns, info.string_sizes_columns);
}

TEST(ReadFromFormatInfoSerialization, Version0KeepsTheOldLayout)
{
    ReadFromFormatInfo info;
    EXPECT_EQ(serializeInfo(info, 0).size() + 1, serializeInfo(info, 1).size());
    EXPECT_TRUE(deserializeInfo(serializeInfo(info, 0), 0).string_sizes_columns.empty());
}

TEST(ReadFromFormatInfoSerialization, Version0RefusesStringSizesColumns)
{
    ReadFromFormatInfo info;
    info.string_sizes_columns = {"s.size"};
    try
    {
        serializeInfo(info, 0);
        FAIL() << "Serializing string_sizes_columns at step version 0 must throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SUPPORT_IS_DISABLED);
    }
    EXPECT_NO_THROW(serializeInfo(info, 0, /*for_cache_key=*/ true));
}
