#include <gtest/gtest.h>

#include <Databases/DataLake/IcebergRestLabels.h>

#include <Poco/JSON/Parser.h>

#include <string>
#include <vector>

using namespace DataLake;

namespace
{

TableLabels parse(const std::string & load_table_result)
{
    Poco::JSON::Parser parser;
    return parseIcebergRestTableLabels(parser.parse(load_table_result).extract<Poco::JSON::Object::Ptr>());
}

/// Field labels as `id:name:key=value`, with `-` for a field which has no name.
std::vector<std::string> flattenFieldLabels(const TableLabels & labels)
{
    std::vector<std::string> result;
    for (const auto & field : labels.field_labels)
        for (const auto & [key, value] : field.labels)
            result.push_back(std::to_string(field.field_id) + ":" + field.field_name.value_or("-") + ":" + key + "=" + value);
    return result;
}

using Labels = TableLabels::Labels;
using Flattened = std::vector<std::string>;

const std::string METADATA = R"(
    "metadata": {
        "format-version": 2,
        "current-schema-id": 1,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "name", "required": false, "type": "string"},
                    {"id": 3, "name": "legacy_code", "required": false, "type": "string"}
                ]
            },
            {
                "type": "struct",
                "schema-id": 1,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "full_name", "required": false, "type": "string"},
                    {"id": 4, "name": "address", "required": false, "type": {
                        "type": "struct",
                        "fields": [
                            {"id": 5, "name": "city", "required": false, "type": "string"},
                            {"id": 6, "name": "geo", "required": false, "type": {
                                "type": "struct",
                                "fields": [{"id": 7, "name": "lat", "required": false, "type": "double"}]
                            }}
                        ]
                    }},
                    {"id": 8, "name": "tags", "required": false, "type": {
                        "type": "list", "element-id": 9, "element-required": false, "element": "string"
                    }},
                    {"id": 10, "name": "attrs", "required": false, "type": {
                        "type": "map", "key-id": 11, "key": "string", "value-id": 12, "value-required": false, "value": {
                            "type": "struct",
                            "fields": [{"id": 13, "name": "score", "required": false, "type": "int"}]
                        }
                    }}
                ]
            }
        ]
    }
)";

}

TEST(IcebergRestLabels, NoLabels)
{
    EXPECT_TRUE(parseIcebergRestTableLabels(Poco::JSON::Object::Ptr{}).empty());
    EXPECT_TRUE(parse("{}").empty());
    EXPECT_TRUE(parse("{" + METADATA + "}").empty());
    EXPECT_TRUE(parse(R"({"labels": {}})").empty());
    EXPECT_TRUE(parse(R"({"labels": {"object-labels": {}, "fields": []}})").empty());
    EXPECT_TRUE(parse(R"({"labels": null})").empty());
}

TEST(IcebergRestLabels, ObjectLabels)
{
    const auto labels = parse(R"({"labels": {"object-labels": {"owner": "team-a", "classification": "restricted", "empty": ""}}})");

    EXPECT_EQ(labels.object_labels, (Labels{{"classification", "restricted"}, {"empty", ""}, {"owner", "team-a"}}));
    EXPECT_TRUE(labels.field_labels.empty());
    EXPECT_FALSE(labels.empty());
}

TEST(IcebergRestLabels, FieldLabels)
{
    const auto labels = parse("{" + METADATA + R"(,
        "labels": {
            "fields": [
                {"field-id": 2, "labels": {"pii.type": "name", "pii": "true"}},
                {"field-id": 1, "labels": {"key": "primary"}}
            ]
        }})");

    EXPECT_TRUE(labels.object_labels.empty());
    /// Ordered by field id, then by key.
    EXPECT_EQ(flattenFieldLabels(labels), (Flattened{"1:id:key=primary", "2:full_name:pii=true", "2:full_name:pii.type=name"}));
}

TEST(IcebergRestLabels, FieldNamesAreOfCurrentSchema)
{
    const auto labels = parse("{" + METADATA + R"(,
        "labels": {
            "fields": [
                {"field-id": 2, "labels": {"a": "renamed"}},
                {"field-id": 3, "labels": {"a": "dropped"}},
                {"field-id": 100, "labels": {"a": "unknown"}}
            ]
        }})");

    /// The field 2 was renamed, the field 3 exists only in the previous schema.
    EXPECT_EQ(flattenFieldLabels(labels), (Flattened{"2:full_name:a=renamed", "3:-:a=dropped", "100:-:a=unknown"}));
}

TEST(IcebergRestLabels, NestedFieldNames)
{
    const auto labels = parse("{" + METADATA + R"(,
        "labels": {
            "fields": [
                {"field-id": 4, "labels": {"a": "1"}},
                {"field-id": 5, "labels": {"a": "1"}},
                {"field-id": 7, "labels": {"a": "1"}},
                {"field-id": 9, "labels": {"a": "1"}},
                {"field-id": 11, "labels": {"a": "1"}},
                {"field-id": 12, "labels": {"a": "1"}},
                {"field-id": 13, "labels": {"a": "1"}}
            ]
        }})");

    EXPECT_EQ(flattenFieldLabels(labels), (Flattened{
        "4:address:a=1",
        "5:address.city:a=1",
        "7:address.geo.lat:a=1",
        "9:tags.element:a=1",
        "11:attrs.key:a=1",
        "12:attrs.value:a=1",
        "13:attrs.value.score:a=1"}));
}

TEST(IcebergRestLabels, SchemaOfFormatVersion1)
{
    const auto labels = parse(R"({
        "metadata": {
            "format-version": 1,
            "schema": {"type": "struct", "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}
        },
        "labels": {"fields": [{"field-id": 1, "labels": {"a": "1"}}]}})");

    EXPECT_EQ(flattenFieldLabels(labels), (Flattened{"1:id:a=1"}));
}

TEST(IcebergRestLabels, NoMetadata)
{
    const auto labels = parse(R"({"labels": {"fields": [{"field-id": 1, "labels": {"a": "1"}}]}})");
    EXPECT_EQ(flattenFieldLabels(labels), (Flattened{"1:-:a=1"}));
}

/// Labels are optional enrichment: a response which does not follow the specification must not fail the load of the table.
TEST(IcebergRestLabels, Lenient)
{
    EXPECT_TRUE(parse(R"({"labels": "oops"})").empty());
    EXPECT_TRUE(parse(R"({"labels": ["oops"]})").empty());
    EXPECT_TRUE(parse(R"({"labels": {"object-labels": ["oops"], "fields": {"oops": 1}}})").empty());
    EXPECT_TRUE(parse(R"({"metadata": "oops", "labels": {"fields": "oops"}})").empty());

    const auto labels = parse("{" + METADATA + R"(,
        "labels": {
            "object-labels": {"string": "s", "number": 365, "bool": true, "null": null, "object": {"a": "b"}, "array": ["a"]},
            "fields": [
                "oops",
                {"labels": {"a": "no field id"}},
                {"field-id": "1", "labels": {"a": "field id is not a number"}},
                {"field-id": 99999999999, "labels": {"a": "field id is too big"}},
                {"field-id": 1},
                {"field-id": 1, "labels": "oops"},
                {"field-id": 1, "labels": {}},
                {"field-id": 2, "labels": {"a": "1", "b": "1"}},
                {"field-id": 2, "labels": {"b": "2", "c": 3}}
            ]
        }})");

    /// Scalars are kept as text, the rest is skipped.
    EXPECT_EQ(labels.object_labels, (Labels{{"bool", "true"}, {"number", "365"}, {"string", "s"}}));
    /// A field without labels is not listed. Several entries of one field are merged.
    EXPECT_EQ(flattenFieldLabels(labels), (Flattened{"2:full_name:a=1", "2:full_name:b=2", "2:full_name:c=3"}));
}
