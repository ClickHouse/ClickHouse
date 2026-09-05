#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>
#include <Poco/JSON/Parser.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

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

/// A statement validated against a table that carried a `table-uuid` must not be let through by a
/// metadata file that carries none: `table-uuid` is required from `format-version` 2 on, and a live
/// table never loses it, so a file without one at the same path is a different table.
TEST(IcebergMetadataBelongsToValidatedTable, AMissingUuidFailsClosed)
{
    auto trusted = std::make_shared<TrustedTableUuid>(normalizeUuid("11111111-1111-1111-1111-111111111111"));
    PersistentTableComponents components{
        .shared_schema_processor = std::make_shared<SharedSchemaProcessor>(false),
        .metadata_cache = nullptr,
        .format_version = 2,
        .table_location = "/data/table",
        .metadata_compression_method = DB::CompressionMethod::None,
        .table_path = "/data/table",
        .trusted_table_uuid = trusted,
        .path_resolver = IcebergPathResolver("/data/table", "/data/table"),
        .table_root_was_derived = false,
    };

    const auto incarnation = trusted->getIncarnation();
    EXPECT_THROW(
        components.checkMetadataBelongsToValidatedTable(parse(v1_metadata), incarnation, "the write"), DB::Exception);

    /// The same file with the validated `table-uuid` is the validated table, and passes.
    auto with_uuid = parse(v1_metadata);
    with_uuid->set(f_table_uuid, "11111111-1111-1111-1111-111111111111");
    EXPECT_NO_THROW(components.checkMetadataBelongsToValidatedTable(with_uuid, incarnation, "the write"));
}

/// A table that never carried a `table-uuid` has nothing to compare, and is not refused here.
TEST(IcebergMetadataBelongsToValidatedTable, NoValidatedUuidIsNotARefusal)
{
    auto trusted = std::make_shared<TrustedTableUuid>(std::nullopt);
    PersistentTableComponents components{
        .shared_schema_processor = std::make_shared<SharedSchemaProcessor>(false),
        .metadata_cache = nullptr,
        .format_version = 1,
        .table_location = "/data/table",
        .metadata_compression_method = DB::CompressionMethod::None,
        .table_path = "/data/table",
        .trusted_table_uuid = trusted,
        .path_resolver = IcebergPathResolver("/data/table", "/data/table"),
        .table_root_was_derived = false,
    };

    EXPECT_NO_THROW(
        components.checkMetadataBelongsToValidatedTable(parse(v1_metadata), trusted->getIncarnation(), "the write"));
}

#endif
