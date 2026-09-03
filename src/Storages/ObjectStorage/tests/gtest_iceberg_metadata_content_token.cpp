#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>
#include <Poco/JSON/Parser.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

using namespace DB::Iceberg;

namespace
{

Poco::JSON::Object::Ptr parse(const String & json)
{
    Poco::JSON::Parser parser;
    return parser.parse(json).extract<Poco::JSON::Object::Ptr>();
}

/// A format-version 1 table that omits `table-uuid`, which is exactly the table whose replacement
/// no UUID comparison can see.
const String v1_metadata = R"({
    "format-version": 1,
    "location": "/data/table",
    "last-updated-ms": 1000,
    "last-column-id": 2,
    "current-schema-id": 0,
    "current-snapshot-id": 7,
    "default-sort-order-id": 0
})";

}

/// The same file read twice is the same file: the token must not depend on anything but the
/// content, or every pinned reopen would be refused.
TEST(IcebergMetadataContentToken, IsStableForTheSameContent)
{
    EXPECT_EQ(computeMetadataContentToken(parse(v1_metadata)), computeMetadataContentToken(parse(v1_metadata)));
}

/// A table recreated at the same root writes its own metadata file. Even when it reuses the path,
/// the version number and the schema id - which it restarts from the beginning - its own commit
/// timestamp and snapshot id are its own.
TEST(IcebergMetadataContentToken, ChangesWhenAnotherTableTookThePathOver)
{
    const auto pinned = computeMetadataContentToken(parse(v1_metadata));

    EXPECT_NE(pinned, computeMetadataContentToken(parse(R"({
        "format-version": 1,
        "location": "/data/table",
        "last-updated-ms": 2000,
        "last-column-id": 2,
        "current-schema-id": 0,
        "current-snapshot-id": 7,
        "default-sort-order-id": 0
    })")));

    EXPECT_NE(pinned, computeMetadataContentToken(parse(R"({
        "format-version": 1,
        "location": "/data/table",
        "last-updated-ms": 1000,
        "last-column-id": 2,
        "current-schema-id": 0,
        "current-snapshot-id": 9,
        "default-sort-order-id": 0
    })")));
}

/// An empty table has no `current-snapshot-id` at all, and the `snapshot_id == null` branch of the
/// pinned reopen reads such a table as empty - so a replacement that only drops the snapshot must
/// still be seen.
TEST(IcebergMetadataContentToken, ChangesWhenAFieldIsRemovedOrNulled)
{
    const auto pinned = computeMetadataContentToken(parse(v1_metadata));

    EXPECT_NE(pinned, computeMetadataContentToken(parse(R"({
        "format-version": 1,
        "location": "/data/table",
        "last-updated-ms": 1000,
        "last-column-id": 2,
        "current-schema-id": 0,
        "default-sort-order-id": 0
    })")));

    EXPECT_NE(pinned, computeMetadataContentToken(parse(R"({
        "format-version": 1,
        "location": "/data/table",
        "last-updated-ms": 1000,
        "last-column-id": 2,
        "current-schema-id": 0,
        "current-snapshot-id": null,
        "default-sort-order-id": 0
    })")));
}

/// The values are hashed under their own field names, so a value moving from one field to another
/// is not the same file.
TEST(IcebergMetadataContentToken, DoesNotConfuseTwoFieldsCarryingTheSameValue)
{
    EXPECT_NE(
        computeMetadataContentToken(parse(R"({"format-version": 1, "last-column-id": 2, "current-schema-id": 0})")),
        computeMetadataContentToken(parse(R"({"format-version": 1, "last-column-id": 0, "current-schema-id": 2})")));
}

/// A format-version 2 table is settled by its `table-uuid`, and the token has to see that too, so
/// that the pinned reopen of such a table is not weaker than the one it already had.
TEST(IcebergMetadataContentToken, ChangesWithTheTableUuid)
{
    EXPECT_NE(
        computeMetadataContentToken(parse(R"({"format-version": 2, "table-uuid": "11111111-1111-1111-1111-111111111111"})")),
        computeMetadataContentToken(parse(R"({"format-version": 2, "table-uuid": "22222222-2222-2222-2222-222222222222"})")));
}


namespace
{

Poco::JSON::Object::Ptr metadataWithLog(const std::vector<String> & previous_files)
{
    Poco::JSON::Object::Ptr metadata = new Poco::JSON::Object;
    Poco::JSON::Array::Ptr metadata_log = new Poco::JSON::Array;
    for (const auto & file : previous_files)
    {
        Poco::JSON::Object::Ptr entry = new Poco::JSON::Object;
        entry->set(DB::Iceberg::f_metadata_file, file);
        metadata_log->add(entry);
    }
    metadata->set(DB::Iceberg::f_metadata_log, metadata_log);
    return metadata;
}

}

/// The file that was validated is trivially its own ancestor, whichever way its path is spelled.
TEST(IcebergMetadataAncestry, TheValidatedFileItself)
{
    auto metadata = metadataWithLog({});
    EXPECT_TRUE(DB::Iceberg::metadataDescendsFromValidatedFile(metadata, "metadata/v1.metadata.json", "metadata/v1.metadata.json"));
    EXPECT_TRUE(DB::Iceberg::metadataDescendsFromValidatedFile(
        metadata, "s3://bucket/table/metadata/v1.metadata.json", "metadata/v1.metadata.json"));
}

/// A commit of the validated table records the file it supersedes, so the file now current
/// descends from the validated one.
TEST(IcebergMetadataAncestry, ACommitOfTheValidatedTable)
{
    auto metadata = metadataWithLog({"s3://bucket/table/metadata/v0.metadata.json", "s3://bucket/table/metadata/v1.metadata.json"});
    EXPECT_TRUE(DB::Iceberg::metadataDescendsFromValidatedFile(metadata, "metadata/v2.metadata.json", "metadata/v1.metadata.json"));
}

/// A table recreated at the same root starts its own log, so the file it commits at a higher
/// version does not descend from the validated file - the case a version comparison cannot see.
TEST(IcebergMetadataAncestry, ATableRecreatedAtTheSameRoot)
{
    EXPECT_FALSE(DB::Iceberg::metadataDescendsFromValidatedFile(
        metadataWithLog({}), "metadata/v2.metadata.json", "metadata/v1.metadata.json"));
    EXPECT_FALSE(DB::Iceberg::metadataDescendsFromValidatedFile(
        metadataWithLog({"s3://bucket/table/metadata/00000-abcdef.metadata.json"}),
        "metadata/00001-fedcba.metadata.json",
        "metadata/v1.metadata.json"));
}

/// A metadata file with no log at all ties nothing to the validated file.
TEST(IcebergMetadataAncestry, NoLogAtAll)
{
    Poco::JSON::Object::Ptr metadata = new Poco::JSON::Object;
    EXPECT_FALSE(DB::Iceberg::metadataDescendsFromValidatedFile(metadata, "metadata/v2.metadata.json", "metadata/v1.metadata.json"));
}

#endif
