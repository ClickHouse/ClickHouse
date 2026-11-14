#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{

constexpr UInt64 legacy_step_version = ReadFromFormatInfo::SERIALIZATION_VERSION_WITH_COLUMNS_ADDED_AFTER_FORMAT - 1;
constexpr UInt64 current_step_version = ReadFromFormatInfo::SERIALIZATION_VERSION_WITH_COLUMNS_ADDED_AFTER_FORMAT;

ReadFromFormatInfo makeInfo(NameSet columns_added_after_format)
{
    ReadFromFormatInfo info;
    NamesAndTypesList columns{{"id", std::make_shared<DataTypeInt64>()}, {"_change_type", std::make_shared<DataTypeString>()}};
    for (const auto & column : columns)
    {
        info.source_header.insert({column.type->createColumn(), column.type, column.name});
        info.format_header.insert({column.type->createColumn(), column.type, column.name});
    }
    info.columns_description = ColumnsDescription(columns);
    info.requested_columns = columns;
    info.columns_added_after_format = std::move(columns_added_after_format);
    return info;
}

String serialize(const ReadFromFormatInfo & info, UInt64 step_version)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    QueryPlanStepSerialization ctx{out, registry};
    ctx.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    ctx.step_version = step_version;
    info.serialize(ctx);
    return out.str();
}

ReadFromFormatInfo deserialize(const String & bytes, UInt64 step_version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    ContextPtr context = getContext().context;
    SharedHeaders input_headers;
    SharedHeader output_header;

    QueryPlanStepDeserialization ctx{
        in, registry, {}, context, input_headers, output_header, settings, 0, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, step_version, false};
    auto info = ReadFromFormatInfo::deserialize(ctx);
    EXPECT_TRUE(in.eof());
    return info;
}

}

/// At the step version from before `columns_added_after_format`, the bytes are those of the old format, which the peer
/// of an older release reads, and the set is read as empty.
TEST(ReadFromFormatInfoSerialization, LegacyStepVersionKeepsOldFormat)
{
    const String without_columns = serialize(makeInfo({}), legacy_step_version);
    const String with_columns = serialize(makeInfo({"_change_type"}), legacy_step_version);
    EXPECT_EQ(without_columns, with_columns);

    auto restored = deserialize(with_columns, legacy_step_version);
    EXPECT_TRUE(restored.columns_added_after_format.empty());
    EXPECT_EQ(with_columns, serialize(restored, legacy_step_version));
}

TEST(ReadFromFormatInfoSerialization, CurrentStepVersionRoundTrips)
{
    for (const auto & columns : {NameSet{}, NameSet{"_change_type", "id"}})
    {
        const String bytes = serialize(makeInfo(columns), current_step_version);
        auto restored = deserialize(bytes, current_step_version);
        EXPECT_EQ(columns, restored.columns_added_after_format);
        EXPECT_EQ(bytes, serialize(restored, current_step_version));
    }

    EXPECT_NE(serialize(makeInfo({}), legacy_step_version), serialize(makeInfo({}), current_step_version));
}
