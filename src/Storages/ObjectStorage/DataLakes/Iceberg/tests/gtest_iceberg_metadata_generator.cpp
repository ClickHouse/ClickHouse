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

void appendSnapshot(Poco::JSON::Object::Ptr metadata, Int64 parent_snapshot_id = -1, bool tolerate_missing_parent_snapshot = false)
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
        /*num_deleted_rows=*/ 0,
        /*user_defined_snapshot_id=*/ std::nullopt,
        /*user_defined_timestamp=*/ std::nullopt,
        MetadataGenerator::SnapshotOperation::Append,
        tolerate_missing_parent_snapshot);
}

void expectAppendRejectsUnresolvableParent(Poco::JSON::Object::Ptr metadata, Int64 parent_snapshot_id)
{
    try
    {
        appendSnapshot(metadata, parent_snapshot_id);
        FAIL() << "Expected ICEBERG_SPECIFICATION_VIOLATION";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION);
    }
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
/// The exception is a live current snapshot that cannot be resolved in `snapshots` (the whole
/// array missing, empty, or its entry pruned): the commit preserves previous table contents by
/// locating the parent snapshot's manifest list in `snapshots`, so proceeding would silently
/// drop all previous data. Such self-contradictory metadata must keep failing, with a
/// spec-violation error.

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

TEST(IcebergMetadataGenerator, AppendsOnTopOfExistingParentSnapshot)
{
    auto metadata = makeMinimalV2Metadata();
    EXPECT_NO_THROW(appendSnapshot(metadata));
    const auto first_snapshot_id = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);
    EXPECT_NO_THROW(appendSnapshot(metadata, first_snapshot_id));
    EXPECT_EQ(metadata->getArray(Iceberg::f_snapshots)->size(), 2u);
}

TEST(IcebergMetadataGenerator, ThrowsWhenSnapshotsArrayMissingButParentSnapshotExists)
{
    auto metadata = makeMinimalV2Metadata();
    metadata->set(Iceberg::f_current_snapshot_id, 42);
    metadata->remove(Iceberg::f_snapshots);
    expectAppendRejectsUnresolvableParent(metadata, /*parent_snapshot_id=*/ 42);
}

TEST(IcebergMetadataGenerator, ThrowsWhenParentSnapshotMissingFromEmptySnapshotsArray)
{
    auto metadata = makeMinimalV2Metadata();
    metadata->set(Iceberg::f_current_snapshot_id, 42);
    expectAppendRejectsUnresolvableParent(metadata, /*parent_snapshot_id=*/ 42);
}

TEST(IcebergMetadataGenerator, ThrowsWhenParentSnapshotEntryPrunedFromSnapshotsArray)
{
    auto metadata = makeMinimalV2Metadata();
    EXPECT_NO_THROW(appendSnapshot(metadata));
    /// Make the metadata genuinely self-contradictory: the array stays non-empty (one entry
    /// from the append above), but the live current snapshot points at an id absent from it.
    /// Pick a fixed id guaranteed to differ from the appended one (snapshot ids span the whole
    /// Int64 range, so `first_snapshot_id + 1` could signed-overflow).
    const auto first_snapshot_id = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);
    const Int64 missing_snapshot_id = first_snapshot_id == 0 ? 1 : 0;
    metadata->set(Iceberg::f_current_snapshot_id, missing_snapshot_id);
    expectAppendRejectsUnresolvableParent(metadata, missing_snapshot_id);
}

/// The up-front check the write paths run before uploading any data files. It enforces the same
/// invariant as the commit, so a self-contradictory table fails without orphaning objects.
TEST(IcebergMetadataGenerator, ValidateParentSnapshotResolvable)
{
    auto metadata = makeMinimalV2Metadata();
    EXPECT_NO_THROW(appendSnapshot(metadata));
    const auto present_id = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);

    /// A live parent that resolves in `snapshots` is fine; so is "no live parent" (-1).
    EXPECT_NO_THROW(MetadataGenerator::validateParentSnapshotResolvable(metadata, present_id));
    EXPECT_NO_THROW(MetadataGenerator::validateParentSnapshotResolvable(metadata, -1));

    /// A live parent absent from `snapshots` is rejected.
    const Int64 missing_id = present_id == 0 ? 1 : 0;
    try
    {
        MetadataGenerator::validateParentSnapshotResolvable(metadata, missing_id);
        FAIL() << "Expected ICEBERG_SPECIFICATION_VIOLATION";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION);
    }
}

/// Compaction replays a filtered history where a record's parent may be a legitimately
/// skipped snapshot; `tolerate_missing_parent_snapshot` keeps such appends committing,
/// with the summary totals restarting from the appended deltas.
TEST(IcebergMetadataGenerator, ToleratesUnresolvableParentWhenRequested)
{
    auto metadata = makeMinimalV2Metadata();
    metadata->set(Iceberg::f_current_snapshot_id, 42);
    EXPECT_NO_THROW(appendSnapshot(metadata, /*parent_snapshot_id=*/ 42, /*tolerate_missing_parent_snapshot=*/ true));

    const auto snapshot = metadata->getArray(Iceberg::f_snapshots)->getObject(0);
    EXPECT_EQ(snapshot->getValue<Int64>(Iceberg::f_parent_snapshot_id), 42);
    EXPECT_EQ(snapshot->getObject(Iceberg::f_summary)->getValue<String>(Iceberg::f_total_records), "1");
}

#endif
