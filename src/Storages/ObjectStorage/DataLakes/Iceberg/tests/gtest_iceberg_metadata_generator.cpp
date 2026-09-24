#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
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

    Poco::JSON::Object::Ptr main_branch = new Poco::JSON::Object;
    main_branch->set(Iceberg::f_metadata_snapshot_id, -1);
    main_branch->set(Iceberg::f_type, Iceberg::f_branch);
    Poco::JSON::Object::Ptr refs = new Poco::JSON::Object;
    refs->set(Iceberg::f_main, main_branch);
    metadata->set(Iceberg::f_refs, refs);

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

#endif
