#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#include <gtest/gtest.h>

#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/GlueCatalog.h>
#include <Poco/JSON/Parser.h>

using namespace DataLake;

namespace
{

Poco::JSON::Object::Ptr parseObject(const std::string & json)
{
    Poco::JSON::Parser parser;
    return parser.parse(json).extract<Poco::JSON::Object::Ptr>();
}

Poco::Dynamic::Var parseType(const std::string & json)
{
    /// Parse primitive and complex types through the same JSON wrapper.
    return parseObject("{\"type\": " + json + "}")->get("type");
}

}

TEST(GlueSchemaColumns, PrimitiveTypes)
{
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"boolean\"")), "boolean");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"int\"")), "int");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"long\"")), "bigint");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"float\"")), "float");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"double\"")), "double");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"date\"")), "date");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"time\"")), "string");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"timestamp\"")), "timestamp");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"timestamptz\"")), "timestamp");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"timestamp_ns\"")), "timestamp");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"timestamptz_ns\"")), "timestamp");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"string\"")), "string");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"uuid\"")), "string");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"binary\"")), "binary");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"fixed[16]\"")), "binary");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"decimal(9, 2)\"")), "decimal(9,2)");
    EXPECT_EQ(icebergTypeToGlueType(parseType("\"decimal(38,0)\"")), "decimal(38,0)");
}

TEST(GlueSchemaColumns, ComplexTypes)
{
    EXPECT_EQ(
        icebergTypeToGlueType(parseType(R"json({"type":"list","element-id":3,"element":"string","element-required":false})json")),
        "array<string>");
    EXPECT_EQ(
        icebergTypeToGlueType(parseType(R"json({"type":"map","key-id":4,"key":"string","value-id":5,"value":"decimal(9, 2)","value-required":false})json")),
        "map<string,decimal(9,2)>");
    EXPECT_EQ(
        icebergTypeToGlueType(parseType(
            R"json({"type":"struct","fields":[
                {"id":6,"name":"created_by","required":false,"type":"string"},
                {"id":7,"name":"tags","required":true,"type":{"type":"list","element-id":8,"element":"long","element-required":true}}
            ]})json")),
        "struct<created_by:string,tags:array<bigint>>");
}

TEST(GlueSchemaColumns, ColumnsFromSchema)
{
    auto schema = parseObject(R"json({
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "message", "required": false, "type": "string"},
            {"id": 3, "name": "details", "required": false, "type": {"type": "struct", "fields": [
                {"id": 4, "name": "created_by", "required": false, "type": "string"}]}}
        ]})json");

    auto columns = GlueCatalog::glueColumnsFromIcebergSchema(schema);
    ASSERT_EQ(columns.size(), 3u);

    EXPECT_EQ(columns[0].GetName(), "id");
    EXPECT_EQ(columns[0].GetType(), "bigint");
    EXPECT_EQ(columns[0].GetParameters().at("iceberg.field.id"), "1");
    EXPECT_EQ(columns[0].GetParameters().at("iceberg.field.optional"), "false");
    EXPECT_EQ(columns[0].GetParameters().at("iceberg.field.current"), "true");

    EXPECT_EQ(columns[1].GetName(), "message");
    EXPECT_EQ(columns[1].GetType(), "string");
    EXPECT_EQ(columns[1].GetParameters().at("iceberg.field.id"), "2");
    EXPECT_EQ(columns[1].GetParameters().at("iceberg.field.optional"), "true");

    EXPECT_EQ(columns[2].GetName(), "details");
    EXPECT_EQ(columns[2].GetType(), "struct<created_by:string>");
    EXPECT_EQ(columns[2].GetParameters().at("iceberg.field.id"), "3");
}

TEST(GlueSchemaColumns, CurrentSchemaSelection)
{
    auto metadata = parseObject(R"json({
        "format-version": 2,
        "current-schema-id": 1,
        "schemas": [
            {"schema-id": 0, "fields": [{"id": 1, "name": "a", "required": false, "type": "int"}]},
            {"schema-id": 1, "fields": [{"id": 1, "name": "a", "required": false, "type": "int"},
                                        {"id": 2, "name": "b", "required": false, "type": "string"}]}
        ]})json");

    auto columns = GlueCatalog::glueColumnsFromIcebergSchema(GlueCatalog::getCurrentSchema(metadata));
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[1].GetName(), "b");

    /// In v1, the top-level `schema` takes precedence over `schemas`.
    auto v1_metadata = parseObject(R"json({"format-version": 1, "schema": {"schema-id": 0, "fields": [{"id": 1, "name": "x", "required": false, "type": "double"}]},
        "current-schema-id": 1, "schemas": [{"schema-id": 1, "fields": [{"id": 2, "name": "stale", "required": false, "type": "int"}]}]})json");
    auto v1_columns = GlueCatalog::glueColumnsFromIcebergSchema(GlueCatalog::getCurrentSchema(v1_metadata));
    ASSERT_EQ(v1_columns.size(), 1u);
    EXPECT_EQ(v1_columns[0].GetName(), "x");
    EXPECT_EQ(v1_columns[0].GetType(), "double");

    EXPECT_TRUE(GlueCatalog::glueColumnsFromIcebergSchema(nullptr).empty());
    EXPECT_TRUE(GlueCatalog::getCurrentSchema(nullptr).isNull());
}

/// Without a metadata location, the Glue reader can still parse ordinary nested types.
TEST(GlueSchemaColumns, NestedTypesParseableByGlueFallback)
{
    const auto glue_type = icebergTypeToGlueType(parseType(
        R"json({"type":"struct","fields":[
            {"id":1,"name":"a","required":false,"type":{"type":"map","key-id":2,"key":"string","value-id":3,"value":"decimal(9, 2)","value-required":false}},
            {"id":4,"name":"b","required":false,"type":{"type":"list","element-id":5,"element":"long","element-required":false}}
        ]})json"));
    EXPECT_EQ(glue_type, "struct<a:map<string,decimal(9,2)>,b:array<bigint>>");

    auto parsed = getType(glue_type, /*nullable=*/true);
    EXPECT_EQ(parsed->getName(), "Tuple(a Map(String, Nullable(Decimal(9, 2))), b Array(Nullable(Int64)))");
}

#endif
