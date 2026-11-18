#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
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

/// The read of a Delta Lake table with column mapping `k_old` -> physical `k`, `k` -> physical `v`.
ReadFromFormatInfo makeInfo(NameSet columns_read_under_other_names)
{
    auto type = std::make_shared<DataTypeInt64>();
    NamesAndTypesList physical{{"v", type}, {"k", type}};

    ReadFromFormatInfo info;
    info.source_header = Block({{type, "k"}, {type, "k_old"}});
    info.format_header = Block({{type, "v"}, {type, "k"}});
    info.columns_description = ColumnsDescription(physical);
    info.requested_columns = physical;
    info.columns_read_under_other_names = std::move(columns_read_under_other_names);
    return info;
}

String serialize(const ReadFromFormatInfo & info)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    info.serialize(ctx);
    return out.str();
}

ReadFromFormatInfo deserialize(const String & bytes)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    ContextPtr context = getContext().context;
    SharedHeaders input_headers;
    SharedHeader output_header;
    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, input_headers, output_header, settings, 0, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, 0, false};

    auto info = ReadFromFormatInfo::deserialize(ctx);
    EXPECT_TRUE(in.eof());
    return info;
}

}

TEST(ReadFromFormatInfoSerialization, KeepsColumnsReadUnderOtherNames)
{
    /// Ten names: the size of the set is written as the byte '\n'.
    NameSet ten_names;
    for (size_t i = 0; i < 10; ++i)
        ten_names.insert("c" + std::to_string(i));

    for (const NameSet & names : {NameSet{"k", "k_old"}, ten_names})
        EXPECT_EQ(deserialize(serialize(makeInfo(names))).columns_read_under_other_names, names);
}

TEST(ReadFromFormatInfoSerialization, NoTrailerWithoutColumnsReadUnderOtherNames)
{
    const String without = serialize(makeInfo({}));
    const String with = serialize(makeInfo({"k", "k_old"}));

    EXPECT_TRUE(deserialize(without).columns_read_under_other_names.empty());
    ASSERT_GT(with.size(), without.size());
    EXPECT_TRUE(with.starts_with(without.substr(0, without.size() - 1)));
    EXPECT_TRUE(without.ends_with('\n'));
    EXPECT_TRUE(with.ends_with('\n'));
}
