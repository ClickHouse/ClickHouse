#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>

#if USE_AVRO

#include <filesystem>

#include <Common/Exception.h>
#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace
{

/// Minimal V2 table metadata with all spec-optional arrays present, modelled on
/// what createEmptyMetadataFile writes for a ClickHouse-created empty table.
Poco::JSON::Object::Ptr makeMinimalV2Metadata()
{
    Poco::JSON::Object::Ptr metadata = new Poco::JSON::Object;
    metadata->set(Iceberg::f_format_version, 2);
    metadata->set(Iceberg::f_current_schema_id, 0);
    metadata->set(Iceberg::f_last_sequence_number, 0);
    metadata->set(Iceberg::f_current_snapshot_id, -1);

    metadata->set(Iceberg::f_refs, Poco::JSON::Object::Ptr(new Poco::JSON::Object));

    metadata->set(Iceberg::f_snapshots, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(Iceberg::f_snapshot_log, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(Iceberg::f_metadata_log, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    return metadata;
}

void appendSnapshot(Poco::JSON::Object::Ptr metadata, Int64 parent_snapshot_id = -1)
{
    FileNamesGenerator generator("s3://bucket/table", /*use_uuid_in_metadata=*/ false, CompressionMethod::None, "Parquet");
    generator.setVersion(1);
    auto metadata_info = generator.generateMetadataPathWithInfo();
    MetadataGenerator(metadata).generateNextMetadata(
        generator,
        metadata_info.path,
        parent_snapshot_id,
        /*added_files=*/ 1,
        /*added_records=*/ 1,
        /*added_files_size=*/ 100,
        /*num_partitions=*/ 1,
        /*added_delete_files=*/ 0,
        /*num_deleted_rows=*/ 0);
}

/// Generate one snapshot with explicit delete-file / operation parameters and return its `operation` summary field.
String snapshotOperation(
    Poco::JSON::Object::Ptr metadata,
    Int64 added_delete_files,
    Int64 num_deleted_rows,
    MetadataGenerator::SnapshotOperation operation = MetadataGenerator::SnapshotOperation::Append)
{
    FileNamesGenerator generator("s3://bucket/table", /*use_uuid_in_metadata=*/ false, CompressionMethod::None, "Parquet");
    generator.setVersion(1);
    auto metadata_info = generator.generateMetadataPathWithInfo();
    auto result = MetadataGenerator(metadata).generateNextMetadata(
        generator,
        metadata_info.path,
        /*parent_snapshot_id=*/ -1,
        /*added_files=*/ 1,
        /*added_records=*/ 1,
        /*added_files_size=*/ 100,
        /*num_partitions=*/ 1,
        added_delete_files,
        num_deleted_rows,
        /*user_defined_snapshot_id=*/ std::nullopt,
        /*user_defined_timestamp=*/ std::nullopt,
        operation);
    return result.snapshot->getObject(Iceberg::f_summary)->getValue<String>(Iceberg::f_operation);
}

void callManifestOnlySnapshot(Poco::JSON::Object::Ptr metadata, Int64 parent_snapshot_id)
{
    FileNamesGenerator generator("s3://bucket/table", /*use_uuid_in_metadata=*/ false, CompressionMethod::None, "Parquet");
    generator.setVersion(1);
    auto metadata_info = generator.generateMetadataPathWithInfo();
    MetadataGenerator(metadata).generateManifestOnlySnapshot(generator, metadata_info.path, parent_snapshot_id);
}

}

/// A plain data append (no deleted rows) must be labelled `append`.
TEST(IcebergMetadataGenerator, AppendOperationForInsert)
{
    auto metadata = makeMinimalV2Metadata();
    EXPECT_EQ(snapshotOperation(metadata, /*added_delete_files=*/ 0, /*num_deleted_rows=*/ 0), Iceberg::f_append);
}

/// A merge-on-read DELETE writes position-delete files, so its snapshot must be labelled `overwrite`
/// (Iceberg spec). This is what system.iceberg_history reports and what expire_snapshots relies on.
TEST(IcebergMetadataGenerator, OverwriteOperationForDelete)
{
    auto metadata = makeMinimalV2Metadata();
    EXPECT_EQ(snapshotOperation(metadata, /*added_delete_files=*/ 1, /*num_deleted_rows=*/ 5), Iceberg::f_overwrite);
}

/// An explicit `Replace` (manifest rewrite / compaction) stays `replace` regardless of delete counters.
TEST(IcebergMetadataGenerator, ReplaceOperationIsPreserved)
{
    auto metadata = makeMinimalV2Metadata();
    EXPECT_EQ(
        snapshotOperation(
            metadata, /*added_delete_files=*/ 1, /*num_deleted_rows=*/ 5, MetadataGenerator::SnapshotOperation::Replace),
        Iceberg::f_replace);
}

/// snapshots / metadata-log / snapshot-log are optional per the Iceberg spec, so external
/// engines may produce empty-table metadata that omits any of them. generateNextMetadata must
/// seed them rather than abort: a missing snapshots throws Poco::InvalidAccessException (empty
/// Var extracted in getParentSnapshot), a missing log array dereferences a null Array::Ptr
/// (Poco::NullPointerException, "Null pointer").
///
/// The exception is a missing `snapshots` combined with a live current snapshot: the commit
/// preserves previous table contents by locating the parent snapshot's manifest list in
/// `snapshots`, so seeding an empty list there would silently drop all previous data. Such
/// self-contradictory metadata must keep failing, with a spec-violation error.

TEST(IcebergMetadataGenerator, AppendsSnapshotWhenAllOptionalArraysPresent)
{
    auto metadata = makeMinimalV2Metadata();
    EXPECT_NO_THROW(appendSnapshot(metadata));
    EXPECT_EQ(metadata->getArray(Iceberg::f_snapshots)->size(), 1u);
}

TEST(IcebergMetadataGenerator, AppendsSnapshotWhenSnapshotsArrayMissing)
{
    auto metadata = makeMinimalV2Metadata();
    metadata->remove(Iceberg::f_snapshots);
    EXPECT_NO_THROW(appendSnapshot(metadata));
    EXPECT_EQ(metadata->getArray(Iceberg::f_snapshots)->size(), 1u);
}

TEST(IcebergMetadataGenerator, AppendsSnapshotWhenMetadataLogMissing)
{
    auto metadata = makeMinimalV2Metadata();
    metadata->remove(Iceberg::f_metadata_log);
    EXPECT_NO_THROW(appendSnapshot(metadata));
    EXPECT_EQ(metadata->getArray(Iceberg::f_metadata_log)->size(), 1u);
}

TEST(IcebergMetadataGenerator, AppendsSnapshotWhenSnapshotLogMissing)
{
    auto metadata = makeMinimalV2Metadata();
    metadata->remove(Iceberg::f_snapshot_log);
    EXPECT_NO_THROW(appendSnapshot(metadata));
    EXPECT_EQ(metadata->getArray(Iceberg::f_snapshot_log)->size(), 1u);
}

TEST(IcebergMetadataGenerator, AppendsSnapshotWhenAllOptionalArraysMissing)
{
    auto metadata = makeMinimalV2Metadata();
    metadata->remove(Iceberg::f_snapshots);
    metadata->remove(Iceberg::f_metadata_log);
    metadata->remove(Iceberg::f_snapshot_log);
    EXPECT_NO_THROW(appendSnapshot(metadata));
}

TEST(IcebergMetadataGenerator, ThrowsWhenSnapshotsArrayMissingButParentSnapshotExists)
{
    auto metadata = makeMinimalV2Metadata();
    metadata->set(Iceberg::f_current_snapshot_id, 42);
    metadata->remove(Iceberg::f_snapshots);
    try
    {
        appendSnapshot(metadata, /*parent_snapshot_id=*/ 42);
        FAIL() << "Expected ICEBERG_SPECIFICATION_VIOLATION";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION);
    }
    EXPECT_FALSE(metadata->has(Iceberg::f_snapshots));
}

/// `refs` is optional per the Iceberg spec too, so a manifest-only rewrite (OPTIMIZE ... MANIFEST)
/// must seed it. isObject is asserted, not just the absence of a throw: it matches on the same
/// typeid(Object::Ptr) as getObject, so it also rejects seeding a raw Poco::JSON::Object pointer.
TEST(IcebergMetadataGenerator, ManifestOnlySnapshotWhenRefsMissing)
{
    auto metadata = makeMinimalV2Metadata();
    ASSERT_NO_THROW(appendSnapshot(metadata));
    ASSERT_EQ(metadata->getArray(Iceberg::f_snapshots)->size(), 1u);
    auto parent_snapshot_id
        = metadata->getArray(Iceberg::f_snapshots)->getObject(0)->getValue<Int64>(Iceberg::f_metadata_snapshot_id);

    metadata->remove(Iceberg::f_refs);
    EXPECT_NO_THROW(callManifestOnlySnapshot(metadata, parent_snapshot_id));

    EXPECT_TRUE(metadata->isObject(Iceberg::f_refs));
    ASSERT_FALSE(metadata->getObject(Iceberg::f_refs).isNull());
    ASSERT_TRUE(metadata->getObject(Iceberg::f_refs)->has(Iceberg::f_main));
    EXPECT_EQ(
        metadata->getObject(Iceberg::f_refs)->getObject(Iceberg::f_main)->getValue<Int64>(Iceberg::f_metadata_snapshot_id),
        metadata->getValue<Int64>(Iceberg::f_current_snapshot_id));
}

TEST(IcebergFileNamesGenerator, ContinuesCurrentMetadataNaming)
{
    auto next_name = [](const String & current)
    {
        FileNamesGenerator generator("s3://bucket/table", /*use_uuid_in_metadata=*/ false, CompressionMethod::None, "Parquet");
        generator.continueMetadataChain("s3://bucket/table/metadata/" + current, 4);
        return String(std::filesystem::path(generator.generateMetadataPathWithInfo().path.serialize()).filename());
    };
    EXPECT_EQ(next_name("v4.metadata.json"), "v5.metadata.json");
    const String numbered = next_name("00004-3f2b6c1e-8a43-4d5e-9b7f-0c1d2e3f4a5b.metadata.json");
    EXPECT_TRUE(numbered.starts_with("00005-") && numbered.ends_with(".metadata.json") && numbered.size() == 56) << numbered;
    EXPECT_FALSE(Iceberg::isVersionNumberedCommitScheme(numbered));
    /// Without a catalog the name is the commit's compare-and-set: every writer of version 5 must aim at it.
    EXPECT_EQ(numbered, next_name("00004-0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d.metadata.json"));
    const String versioned = next_name("v4-3f2b6c1e-8a43-4d5e-9b7f-0c1d2e3f4a5b.metadata.json");
    EXPECT_TRUE(versioned.starts_with("v5-") && versioned.ends_with(".metadata.json") && versioned.size() == 53) << versioned;
    EXPECT_FALSE(Iceberg::isVersionNumberedCommitScheme(versioned));
    EXPECT_EQ(versioned, next_name("v4-0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d.metadata.json"));
}

#endif
