#include <gtest/gtest.h>

#include <config.h>
#if USE_ORC

#    include <Columns/ColumnsNumber.h>
#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeMap.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeTuple.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <DataTypes/DataTypeString.h>
#    include <IO/WriteBufferFromFile.h>
#    include <Processors/Executors/CompletedPipelineExecutor.h>
#    include <Processors/Formats/Impl/ORCBlockOutputFormat.h>
#    include <Processors/Sources/SourceFromChunks.h>
#    include <QueryPipeline/QueryPipelineBuilder.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

#    include <orc/OrcFile.hh>

#    include <Poco/JSON/Object.h>
#    include <Poco/JSON/Parser.h>

using namespace DB;

namespace
{

// Iceberg schema exercising each optional/required shape (expected iceberg.required per path is
// asserted in the test below).
const char * kIcebergSchema = R"JSON(
{
  "type": "struct",
  "schema-id": 0,
  "fields": [
    { "id": 1, "name": "id", "required": true, "type": "long" },
    { "id": 2, "name": "arr", "required": false,
      "type": { "type": "list", "element-id": 10, "element": "int", "element-required": false } },
    { "id": 3, "name": "arr_req", "required": true,
      "type": { "type": "list", "element-id": 11, "element": "int", "element-required": true } },
    { "id": 4, "name": "st", "required": false,
      "type": { "type": "struct", "fields": [
        { "id": 12, "name": "a", "required": false, "type": "int" },
        { "id": 13, "name": "b", "required": true, "type": "int" }
      ] } },
    { "id": 5, "name": "m", "required": false,
      "type": { "type": "map", "key-id": 14, "key": "string",
                "value-id": 15, "value": "int", "value-required": false } },
    { "id": 6, "name": "nested", "required": false,
      "type": { "type": "list", "element-id": 16, "element-required": true,
                "element": { "type": "struct", "fields": [
                  { "id": 17, "name": "x", "required": true, "type": "int" },
                  { "id": 18, "name": "y", "required": false, "type": "int" },
                  { "id": 19, "name": "inner", "required": false,
                    "type": { "type": "struct", "fields": [
                      { "id": 20, "name": "p", "required": true, "type": "int" }
                    ] } }
                ] } } }
  ]
}
)JSON";

/// The ClickHouse header the Iceberg reader produces for the schema above: optionality lives on the
/// leaves (scalars get Nullable), containers are never Nullable. This is exactly what would reach
/// the ORC writer on an INSERT into such a table.
Block makeHeader()
{
    auto nint = makeNullable(std::make_shared<DataTypeInt32>());
    auto nlong = makeNullable(std::make_shared<DataTypeInt64>());

    Block header;
    header.insert(ColumnWithTypeAndName(nlong, "id"));
    header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeArray>(nint), "arr"));
    header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()), "arr_req"));
    header.insert(ColumnWithTypeAndName(
        std::make_shared<DataTypeTuple>(DataTypes{nint, std::make_shared<DataTypeInt32>()}, Names{"a", "b"}), "st"));
    header.insert(ColumnWithTypeAndName(
        std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), nint), "m"));
    // nested: Array(Tuple(x Int32, y Nullable(Int32), inner Tuple(p Int32))) — optional list of
    // required structs, where `inner` is an OPTIONAL struct nested inside. `inner` is a complex node
    // whose optionality cannot come from Nullable-ness (Tuple is never Nullable), so it only reads
    // false if the collector descended into the nested struct and applied its own `required` bit.
    auto inner_tuple = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeInt32>()}, Names{"p"});
    header.insert(ColumnWithTypeAndName(
        std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(
                DataTypes{std::make_shared<DataTypeInt32>(), nint, inner_tuple}, Names{"x", "y", "inner"})),
        "nested"));
    return header;
}

ColumnMapperPtr makeIcebergMapper()
{
    Poco::JSON::Parser parser;
    auto schema_object = parser.parse(kIcebergSchema).extract<Poco::JSON::Object::Ptr>();
    // Use the same builder the MultipleFileWriter INSERT path uses, so this test covers the
    // production mapper wiring (dropping the wiring there would fail this test).
    return Iceberg::createColumnMapperFromFields(schema_object->getArray("fields"));
}

void writeOneEmptyBlock(const Block & header, ColumnMapperPtr mapper, const String & path)
{
    Chunk chunk(header.cloneEmptyColumns(), 0);
    Chunks chunks;
    chunks.push_back(std::move(chunk));
    auto source = std::make_shared<SourceFromChunks>(std::make_shared<const Block>(header), std::move(chunks));

    QueryPipelineBuilder builder;
    builder.init(Pipe(source));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));

    WriteBufferFromFile write_buffer(path);
    FormatSettings format_settings;
    auto output = std::make_shared<ORCBlockOutputFormat>(write_buffer, pipeline.getSharedHeader(), format_settings, mapper);
    pipeline.complete(output);
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
    output->finalize();
    write_buffer.finalize();
}

/// Walk the ORC footer type tree, recording iceberg.required for every path (dotted convention:
/// t.field, arr.element, m.key, m.value) that carries the attribute.
void collectRequired(const orc::Type & type, const String & path, std::map<String, String> & out)
{
    if (type.hasAttributeKey("iceberg.required"))
        out[path] = type.getAttributeValue("iceberg.required");

    switch (type.getKind())
    {
        case orc::TypeKind::STRUCT:
            for (uint64_t i = 0; i < type.getSubtypeCount(); ++i)
            {
                const String child = path.empty() ? type.getFieldName(i) : path + "." + type.getFieldName(i);
                collectRequired(*type.getSubtype(i), child, out);
            }
            break;
        case orc::TypeKind::LIST:
            collectRequired(*type.getSubtype(0), path + ".element", out);
            break;
        case orc::TypeKind::MAP:
            collectRequired(*type.getSubtype(0), path + ".key", out);
            collectRequired(*type.getSubtype(1), path + ".value", out);
            break;
        default:
            break;
    }
}

std::map<String, String> readIcebergRequired(const String & path)
{
    orc::ReaderOptions options;
    auto reader = orc::createReader(orc::readLocalFile(path), options);
    std::map<String, String> out;
    // The root is the file struct; descend into its top-level columns with an empty prefix.
    collectRequired(reader->getType(), "", out);
    return out;
}

}

/// Regression for the bot finding on #109994 (follow-up): the ORC Iceberg writer must derive
/// `iceberg.required` from the source Iceberg schema, not from ClickHouse Nullable-ness, because a
/// complex container (list/map/struct) is never Nullable in the ClickHouse type. Without the fix,
/// every optional complex field (and its optional children) is wrongly emitted as required=true.
TEST(ORCIcebergRequired, OptionalComplexFieldsAreNotRequired)
{
    const String path = "/tmp/test_orc_iceberg_required.orc";
    writeOneEmptyBlock(makeHeader(), makeIcebergMapper(), path);
    auto req = readIcebergRequired(path);

    // Required scalar and required list stay required.
    EXPECT_EQ(req["id"], "true") << "id is a required long";
    EXPECT_EQ(req["arr_req"], "true") << "arr_req is a required list";
    EXPECT_EQ(req["arr_req.element"], "true") << "arr_req element-required=true";

    // Optional complex containers must be optional (this is the bug).
    EXPECT_EQ(req["arr"], "false") << "arr is an optional list";
    EXPECT_EQ(req["arr.element"], "false") << "arr element-required=false";
    EXPECT_EQ(req["st"], "false") << "st is an optional struct";
    EXPECT_EQ(req["m"], "false") << "m is an optional map";

    // Optionality of nested fields follows their own Iceberg `required` bit.
    EXPECT_EQ(req["st.a"], "false") << "st.a required=false";
    EXPECT_EQ(req["st.b"], "true") << "st.b required=true";
    EXPECT_EQ(req["m.key"], "true") << "map key is always required per the Iceberg spec";
    EXPECT_EQ(req["m.value"], "false") << "m value-required=false";

    // Nested complex (list of struct): the recursion must descend through the list into the struct
    // and apply each level's own optionality.
    EXPECT_EQ(req["nested"], "false") << "nested is an optional list";
    EXPECT_EQ(req["nested.element"], "true") << "nested element-required=true";
    EXPECT_EQ(req["nested.element.x"], "true") << "nested.element.x required=true";
    EXPECT_EQ(req["nested.element.y"], "false") << "nested.element.y required=false";
    // An OPTIONAL complex node nested inside another complex node: only correct if the collector
    // recursed into the nested struct (a Tuple is never Nullable, so the fallback cannot yield false).
    EXPECT_EQ(req["nested.element.inner"], "false") << "nested.element.inner is an optional struct";
    EXPECT_EQ(req["nested.element.inner.p"], "true") << "nested.element.inner.p required=true";
}

/// A field-id-only mapper (no Iceberg required info) must keep the pre-fix behaviour: derive
/// `required` from ClickHouse Nullable-ness. This guards the has_iceberg_required_info fallback so
/// non-Iceberg-schema writes are unchanged.
TEST(ORCIcebergRequired, NoInfoFallsBackToNullable)
{
    auto mapper = std::make_shared<ColumnMapper>();
    // Only field ids, no optional-paths => hasIcebergRequiredInfo() stays false.
    mapper->setStorageColumnEncoding({{"id", 1}, {"arr", 2}});
    ASSERT_FALSE(mapper->hasIcebergRequiredInfo());

    Block header;
    header.insert(ColumnWithTypeAndName(makeNullable(std::make_shared<DataTypeInt64>()), "id"));
    header.insert(ColumnWithTypeAndName(
        std::make_shared<DataTypeArray>(makeNullable(std::make_shared<DataTypeInt32>())), "arr"));

    const String path = "/tmp/test_orc_iceberg_required_fallback.orc";
    writeOneEmptyBlock(header, mapper, path);
    auto req = readIcebergRequired(path);

    // Nullable(Int64) -> required=false; the non-Nullable Array container -> required=true
    // (the pre-fix behaviour, correct only when no Iceberg schema info is available).
    EXPECT_EQ(req["id"], "false");
    EXPECT_EQ(req["arr"], "true");
}

/// A schema missing a mandatory member (a required flag, or a field's `type`) must be rejected, not
/// silently treated as required. collectOptionalPaths reads mandatory members unconditionally, the
/// same as the schema parser, so building the mapper throws.
TEST(ORCIcebergRequired, MalformedSchemaThrows)
{
    Poco::JSON::Parser parser;

    // A field with no top-level `required` key.
    const char * missing_required = R"JSON(
    { "type": "struct", "schema-id": 0, "fields": [
        { "id": 1, "name": "arr",
          "type": { "type": "list", "element-id": 10, "element": "int", "element-required": false } }
    ] }
    )JSON";
    auto obj1 = parser.parse(missing_required).extract<Poco::JSON::Object::Ptr>();
    EXPECT_ANY_THROW(Iceberg::createColumnMapperFromFields(obj1->getArray("fields")));

    // A field with `required` but no `type` (the mandatory member is skipped nowhere).
    const char * missing_type = R"JSON(
    { "type": "struct", "schema-id": 0, "fields": [
        { "id": 1, "name": "x", "required": false }
    ] }
    )JSON";
    auto obj2 = parser.parse(missing_type).extract<Poco::JSON::Object::Ptr>();
    EXPECT_ANY_THROW(Iceberg::createColumnMapperFromFields(obj2->getArray("fields")));
}
#endif
