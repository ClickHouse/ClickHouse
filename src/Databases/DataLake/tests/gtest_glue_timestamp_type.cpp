#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#include <gtest/gtest.h>

#include <Databases/DataLake/GlueCatalog.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

using namespace DataLake;

namespace
{

/// Builds a minimal Iceberg metadata JSON object with a single schema containing
/// the given column name and type. `schema_id` is used for both `current-schema-id`
/// and the schema's `schema-id` entry.
Poco::JSON::Object::Ptr buildMetadata(const std::string & column_name, const std::string & column_type, Int64 schema_id = 0)
{
    Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
    field->set("name", column_name);
    field->set("type", column_type);

    Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
    fields->add(field);

    Poco::JSON::Object::Ptr schema = new Poco::JSON::Object;
    schema->set("schema-id", schema_id);
    schema->set("fields", fields);

    Poco::JSON::Array::Ptr schemas = new Poco::JSON::Array;
    schemas->add(schema);

    Poco::JSON::Object::Ptr metadata = new Poco::JSON::Object;
    metadata->set("current-schema-id", schema_id);
    metadata->set("schemas", schemas);

    return metadata;
}

}

TEST(GlueCatalog, ResolveTimestampTypeFoundTimestamptz)
{
    auto metadata = buildMetadata("ts_col", "timestamptz");
    auto result = GlueCatalog::resolveTimestampTypeFromMetadata(metadata, "ts_col", "timestamp");
    EXPECT_EQ(result, "timestamptz");
}

TEST(GlueCatalog, ResolveTimestampTypeFoundTimestamptzNs)
{
    auto metadata = buildMetadata("ts_col", "timestamptz_ns");
    auto result = GlueCatalog::resolveTimestampTypeFromMetadata(metadata, "ts_col", "timestamp_nano");
    EXPECT_EQ(result, "timestamptz_ns");
}

TEST(GlueCatalog, ResolveTimestampTypeFoundTimestamp)
{
    auto metadata = buildMetadata("ts_col", "timestamp");
    auto result = GlueCatalog::resolveTimestampTypeFromMetadata(metadata, "ts_col", "timestamp");
    EXPECT_EQ(result, "timestamp");
}

TEST(GlueCatalog, ResolveTimestampTypeFoundTimestampNs)
{
    auto metadata = buildMetadata("ts_col", "timestamp_ns");
    auto result = GlueCatalog::resolveTimestampTypeFromMetadata(metadata, "ts_col", "timestamp_nano");
    EXPECT_EQ(result, "timestamp_ns");
}

TEST(GlueCatalog, ResolveTimestampTypeNotFoundFallbackTimestamp)
{
    auto metadata = buildMetadata("other_col", "timestamptz");
    // Column not found — glue_column_type "timestamp" falls back to "timestamp"
    auto result = GlueCatalog::resolveTimestampTypeFromMetadata(metadata, "ts_col", "timestamp");
    EXPECT_EQ(result, "timestamp");
}

TEST(GlueCatalog, ResolveTimestampTypeNotFoundFallbackTimestampNano)
{
    auto metadata = buildMetadata("other_col", "timestamptz_ns");
    // Column not found — glue_column_type "timestamp_nano" falls back to "timestamp_ns"
    auto result = GlueCatalog::resolveTimestampTypeFromMetadata(metadata, "ts_col", "timestamp_nano");
    EXPECT_EQ(result, "timestamp_ns");
}

#endif
