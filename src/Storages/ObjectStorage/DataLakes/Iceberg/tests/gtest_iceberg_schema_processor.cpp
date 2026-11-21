#include <gtest/gtest.h>

#include <DataTypes/IDataType.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Common/Exception.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

using namespace DB::Iceberg;

namespace
{
Poco::JSON::Object::Ptr parseSchema(const std::string & json)
{
    Poco::JSON::Parser parser;
    return parser.parse(json).extract<Poco::JSON::Object::Ptr>();
}
}

TEST(IcebergSchemaProcessor, GetSimpleTypeBoolean)
{
    auto type = IcebergSchemaProcessor::getSimpleType("boolean");
    EXPECT_EQ(type->getName(), "Bool");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeInt)
{
    auto type = IcebergSchemaProcessor::getSimpleType("int");
    EXPECT_EQ(type->getName(), "Int32");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeLong)
{
    auto type = IcebergSchemaProcessor::getSimpleType("long");
    EXPECT_EQ(type->getName(), "Int64");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeBigint)
{
    auto type = IcebergSchemaProcessor::getSimpleType("bigint");
    EXPECT_EQ(type->getName(), "Int64");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeFloat)
{
    auto type = IcebergSchemaProcessor::getSimpleType("float");
    EXPECT_EQ(type->getName(), "Float32");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeDouble)
{
    auto type = IcebergSchemaProcessor::getSimpleType("double");
    EXPECT_EQ(type->getName(), "Float64");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeDate)
{
    auto type = IcebergSchemaProcessor::getSimpleType("date");
    EXPECT_EQ(type->getName(), "Date32");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeTime)
{
    auto type = IcebergSchemaProcessor::getSimpleType("time");
    EXPECT_EQ(type->getName(), "Int64");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeTimestamp)
{
    auto type = IcebergSchemaProcessor::getSimpleType("timestamp");
    EXPECT_EQ(type->getName(), "DateTime64(6)");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeTimestamptz)
{
    auto type = IcebergSchemaProcessor::getSimpleType("timestamptz");
    EXPECT_EQ(type->getName(), "DateTime64(6, 'UTC')");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeTimestampNs)
{
    auto type = IcebergSchemaProcessor::getSimpleType("timestamp_ns");
    EXPECT_EQ(type->getName(), "DateTime64(9)");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeTimestamptzNs)
{
    auto type = IcebergSchemaProcessor::getSimpleType("timestamptz_ns");
    EXPECT_EQ(type->getName(), "DateTime64(9, 'UTC')");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeString)
{
    auto type = IcebergSchemaProcessor::getSimpleType("string");
    EXPECT_EQ(type->getName(), "String");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeBinary)
{
    auto type = IcebergSchemaProcessor::getSimpleType("binary");
    EXPECT_EQ(type->getName(), "String");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeUuid)
{
    auto type = IcebergSchemaProcessor::getSimpleType("uuid");
    EXPECT_EQ(type->getName(), "UUID");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeFixed)
{
    auto type = IcebergSchemaProcessor::getSimpleType("fixed[16]");
    EXPECT_EQ(type->getName(), "FixedString(16)");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeDecimal)
{
    auto type = IcebergSchemaProcessor::getSimpleType("decimal(10, 2)");
    EXPECT_EQ(type->getName(), "Decimal(10, 2)");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeUnknownThrows)
{
    EXPECT_THROW(IcebergSchemaProcessor::getSimpleType("unknown_type"), DB::Exception);
}

/// The Iceberg primitive type grammar is a closed set: scalars, decimal(P, S) and fixed[N] whose
/// only parameters are integers, geography/geometry whose parameters are bare identifiers, and the
/// list/map/struct wrappers. None of them carries a quoted string literal. A spelling that embeds
/// one (e.g. "MyType('Hello ( world )')") matches no branch of getSimpleType and is rejected before
/// any comparison runs, so canonicalizeTypeSpacing never sees whitespace inside a quoted literal.
TEST(IcebergSchemaProcessor, GetSimpleTypeWithStringLiteralArgumentThrows)
{
    EXPECT_THROW(IcebergSchemaProcessor::getSimpleType("MyType('Hello ( world )')"), DB::Exception);
}

/// The same string-literal-bearing spelling must be rejected as an initial schema type, i.e. the
/// parser guards the entry point so a quoted literal never reaches the whitespace canonicalization.
TEST(IcebergSchemaProcessor, InitialSchemaTypeWithStringLiteralArgumentThrows)
{
    auto schema = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"MyType('Hello ( world )')"}]})json");
    IcebergSchemaProcessor processor;
    EXPECT_THROW(processor.addIcebergTableSchema(schema), DB::Exception);
}

/// The primitive parser must accept the same inner-whitespace spellings that the
/// whitespace-insensitive comparison treats as equivalent. Without canonicalizing the type string
/// before parsing, readIntText does not skip the leading space, so "decimal( 20, 0 )" and
/// "fixed[ 16 ]" fail to parse even though they denote decimal(20, 0) / fixed[16].
TEST(IcebergSchemaProcessor, GetSimpleTypeDecimalInnerWhitespace)
{
    auto type = IcebergSchemaProcessor::getSimpleType("decimal( 20, 0 )");
    EXPECT_EQ(type->getName(), "Decimal(20, 0)");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeFixedInnerWhitespace)
{
    auto type = IcebergSchemaProcessor::getSimpleType("fixed[ 16 ]");
    EXPECT_EQ(type->getName(), "FixedString(16)");
}

/// Regression test for https://github.com/ClickHouse/ClickHouse/issues/109642
/// The same schema-id can be serialized by different Iceberg writers with different
/// whitespace in parameterized primitive type strings, e.g. the table metadata JSON
/// emits "decimal(20,0)" while the manifest Avro metadata emits "decimal(20, 0)".
/// Both denote the identical type per the Iceberg spec, so re-adding the schema-id
/// must NOT be rejected as a rebinding to a different schema.
TEST(IcebergSchemaProcessor, DecimalTypeWhitespaceIsInsensitive)
{
    auto first = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"decimal(20,0)"}]})json");
    auto second = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"decimal(20, 0)"}]})json");
    IcebergSchemaProcessor processor;
    processor.addIcebergTableSchema(first);
    EXPECT_NO_THROW(processor.addIcebergTableSchema(second));
}

/// A genuinely different type bound to the same schema-id must still be rejected.
TEST(IcebergSchemaProcessor, RebindingSchemaIdToDifferentTypeStillRejected)
{
    auto first = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"decimal(20,0)"}]})json");
    auto second = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"decimal(20,2)"}]})json");
    IcebergSchemaProcessor processor;
    processor.addIcebergTableSchema(first);
    EXPECT_THROW(processor.addIcebergTableSchema(second), DB::Exception);
}

/// A renamed field bound to the same schema-id must still be rejected (issue #107316).
TEST(IcebergSchemaProcessor, RebindingSchemaIdToRenamedFieldStillRejected)
{
    auto first = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"long"}]})json");
    auto second = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c9","required":false,"type":"long"}]})json");
    IcebergSchemaProcessor processor;
    processor.addIcebergTableSchema(first);
    EXPECT_THROW(processor.addIcebergTableSchema(second), DB::Exception);
}

/// The whitespace-insensitive comparison must reach into list/map wrappers: the nested
/// element/key/value primitive types (here list<decimal>) can also be serialized with
/// different spacing across metadata files.
TEST(IcebergSchemaProcessor, ListElementDecimalWhitespaceIsInsensitive)
{
    auto first = parseSchema(
        R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":{"type":"list","element-id":2,"element-required":false,"element":"decimal(20,0)"}}]})json");
    auto second = parseSchema(
        R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":{"type":"list","element-id":2,"element-required":false,"element":"decimal(20, 0)"}}]})json");
    IcebergSchemaProcessor processor;
    processor.addIcebergTableSchema(first);
    EXPECT_NO_THROW(processor.addIcebergTableSchema(second));
}

/// Same for map key/value primitive types (here map<decimal, decimal>).
TEST(IcebergSchemaProcessor, MapKeyValueDecimalWhitespaceIsInsensitive)
{
    auto first = parseSchema(
        R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":{"type":"map","key-id":2,"key":"decimal(20,0)","value-id":3,"value-required":false,"value":"decimal(10,2)"}}]})json");
    auto second = parseSchema(
        R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":{"type":"map","key-id":2,"key":"decimal(20, 0)","value-id":3,"value-required":false,"value":"decimal(10, 2)"}}]})json");
    IcebergSchemaProcessor processor;
    processor.addIcebergTableSchema(first);
    EXPECT_NO_THROW(processor.addIcebergTableSchema(second));
}

/// The Iceberg geography/geometry primitives carry parameters too, e.g.
/// "geography(crs, algorithm)", so their serialization can also differ by whitespace
/// across metadata files. With the geo parser enabled, re-adding the same schema-id with
/// different spacing must not be rejected.
TEST(IcebergSchemaProcessor, GeographyTypeWhitespaceIsInsensitive)
{
    auto first = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"geography(C,A)"}]})json");
    auto second = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"geography(C, A)"}]})json");
    IcebergSchemaProcessor processor(/*allow_geo_parser_=*/true);
    processor.addIcebergTableSchema(first);
    EXPECT_NO_THROW(processor.addIcebergTableSchema(second));
}

/// A geo type string carrying leading/trailing whitespace must map to its alias just like the
/// space-free spelling. The alias prefix match (geography -> binary) runs on the canonicalized
/// spelling, so " geography(C,A)" and "geography(C, A)" under the same schema-id compare equal
/// instead of one skipping aliasing (staying "geography") and the other becoming "binary".
TEST(IcebergSchemaProcessor, GeographyTypeEdgeWhitespaceIsInsensitive)
{
    auto first = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":" geography(C,A) "}]})json");
    auto second = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"geography(C, A)"}]})json");
    IcebergSchemaProcessor processor(/*allow_geo_parser_=*/true);
    processor.addIcebergTableSchema(first);
    EXPECT_NO_THROW(processor.addIcebergTableSchema(second));
}

/// Schema-evolution path: renaming a geo field across two schema-ids while only changing the
/// whitespace of its parameterized type string must resolve to a rename, so the transform DAG
/// exposes the NEW column name. Without whitespace-insensitive comparison the old node is kept
/// unchanged and the DAG would still expose the old name.
TEST(IcebergSchemaProcessor, RenameGeoFieldAcrossSchemaIdsWithWhitespaceIsRename)
{
    auto old_schema = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"a","required":false,"type":"geography(C,A)"}]})json");
    auto new_schema = parseSchema(R"json({"schema-id":1,"fields":[{"id":1,"name":"b","required":false,"type":"geography(C, A)"}]})json");
    IcebergSchemaProcessor processor(/*allow_geo_parser_=*/true);
    processor.addIcebergTableSchema(old_schema);
    processor.addIcebergTableSchema(new_schema);

    auto dag = processor.getSchemaTransformationDagByIds(0, 1);
    ASSERT_TRUE(dag);
    const auto & outputs = dag->getOutputs();
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs[0]->result_name, "b");
}

/// A whitespace-heavy type string must be accepted in the INITIAL/current schema (not just the
/// repeated-same-schema-id path): the parser runs before any comparison, so it has to tolerate the
/// same spellings on its own.
TEST(IcebergSchemaProcessor, InitialSchemaDecimalInnerWhitespaceAccepted)
{
    auto schema = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"decimal( 20, 0 )"}]})json");
    IcebergSchemaProcessor processor;
    EXPECT_NO_THROW(processor.addIcebergTableSchema(schema));
}

/// Schema-evolution across two schema-ids where a decimal widens (allowed conversion) while its
/// type string also carries inner whitespace. allowPrimitiveTypeConversion must canonicalize the
/// spacing so the widening is still recognized and the DAG casts to the new type under the new name.
TEST(IcebergSchemaProcessor, WidenDecimalAcrossSchemaIdsWithInnerWhitespace)
{
    auto old_schema = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"decimal(10,2)"}]})json");
    auto new_schema = parseSchema(R"json({"schema-id":1,"fields":[{"id":1,"name":"c0","required":false,"type":"decimal( 20, 2 )"}]})json");
    IcebergSchemaProcessor processor;
    processor.addIcebergTableSchema(old_schema);
    processor.addIcebergTableSchema(new_schema);

    auto dag = processor.getSchemaTransformationDagByIds(0, 1);
    ASSERT_TRUE(dag);
    const auto & outputs = dag->getOutputs();
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs[0]->result_type->getName(), "Nullable(Decimal(20, 2))");
}

/// A genuinely different nested type inside a list wrapper must still be rejected.
TEST(IcebergSchemaProcessor, RebindingListElementToDifferentTypeStillRejected)
{
    auto first = parseSchema(
        R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":{"type":"list","element-id":2,"element-required":false,"element":"decimal(20,0)"}}]})json");
    auto second = parseSchema(
        R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":{"type":"list","element-id":2,"element-required":false,"element":"decimal(20,2)"}}]})json");
    IcebergSchemaProcessor processor;
    processor.addIcebergTableSchema(first);
    EXPECT_THROW(processor.addIcebergTableSchema(second), DB::Exception);
}

/// Spacing normalization only removes whitespace adjacent to the delimiters '(', ')', '[', ']', ','.
/// Whitespace embedded inside a numeric token is not formatting, so malformed spellings such as
/// "decimal(2 0,0)" or "fixed[1 6]" must NOT canonicalize to a valid type and must still be rejected.
TEST(IcebergSchemaProcessor, GetSimpleTypeDecimalMalformedInnerTokenWhitespaceThrows)
{
    EXPECT_THROW(IcebergSchemaProcessor::getSimpleType("decimal(2 0,0)"), DB::Exception);
}

TEST(IcebergSchemaProcessor, GetSimpleTypeFixedMalformedInnerTokenWhitespaceThrows)
{
    EXPECT_THROW(IcebergSchemaProcessor::getSimpleType("fixed[1 6]"), DB::Exception);
}

/// The same malformed spelling must be rejected when it appears as an initial schema type, i.e. the
/// broadened normalization must not let invalid metadata pass through addIcebergTableSchema.
TEST(IcebergSchemaProcessor, InitialSchemaDecimalMalformedInnerTokenWhitespaceThrows)
{
    auto schema = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"decimal(2 0,0)"}]})json");
    IcebergSchemaProcessor processor;
    EXPECT_THROW(processor.addIcebergTableSchema(schema), DB::Exception);
}

/// Trailing garbage after the scale token must be rejected. Canonicalizing spacing does not remove
/// whitespace between two digits, so "decimal(20,0 0)" keeps the embedded space; the parser must not
/// stop after reading the scale and silently ignore the rest. This mirrors the fixed[N] handling.
TEST(IcebergSchemaProcessor, GetSimpleTypeDecimalTrailingGarbageInScaleThrows)
{
    EXPECT_THROW(IcebergSchemaProcessor::getSimpleType("decimal(20,0 0)"), DB::Exception);
}

/// The same malformed scale spelling must be rejected as an initial schema type.
TEST(IcebergSchemaProcessor, InitialSchemaDecimalTrailingGarbageInScaleThrows)
{
    auto schema = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"decimal(20,0 0)"}]})json");
    IcebergSchemaProcessor processor;
    EXPECT_THROW(processor.addIcebergTableSchema(schema), DB::Exception);
}

/// A new schema-id introduced during evolution is parsed at add time (getSimpleType runs on every
/// field), so a malformed scale in the new schema is rejected when the new schema is added and never
/// reaches the evolution DAG. The old, valid schema-id remains added.
TEST(IcebergSchemaProcessor, SchemaEvolutionDecimalTrailingGarbageInScaleThrows)
{
    auto old_schema = parseSchema(R"json({"schema-id":0,"fields":[{"id":1,"name":"c0","required":false,"type":"decimal(10,2)"}]})json");
    auto new_schema = parseSchema(R"json({"schema-id":1,"fields":[{"id":1,"name":"c0","required":false,"type":"decimal(20,2 2)"}]})json");
    IcebergSchemaProcessor processor;
    processor.addIcebergTableSchema(old_schema);
    EXPECT_THROW(processor.addIcebergTableSchema(new_schema), DB::Exception);
}

/// A missing scale ("decimal(20,)") or a sign-only scale ("decimal(20,+)") is malformed metadata and
/// must be rejected, not silently read as scale 0. The scale is parsed with readIntText, which throws
/// at end of buffer or on a non-digit, matching how the precision is parsed.
TEST(IcebergSchemaProcessor, GetSimpleTypeDecimalEmptyScaleThrows)
{
    EXPECT_THROW(IcebergSchemaProcessor::getSimpleType("decimal(20,)"), DB::Exception);
}

TEST(IcebergSchemaProcessor, GetSimpleTypeDecimalSignOnlyScaleThrows)
{
    EXPECT_THROW(IcebergSchemaProcessor::getSimpleType("decimal(20,+)"), DB::Exception);
}
