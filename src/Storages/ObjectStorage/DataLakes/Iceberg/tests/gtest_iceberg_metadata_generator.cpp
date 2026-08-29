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
    extern const int BAD_ARGUMENTS;
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

/// Returns the summary of the generated manifest-only snapshot.
Poco::JSON::Object::Ptr callManifestOnlySnapshot(Poco::JSON::Object::Ptr metadata, Int64 parent_snapshot_id)
{
    FileNamesGenerator generator("s3://bucket/table", /*use_uuid_in_metadata=*/ false, CompressionMethod::None, "Parquet");
    generator.setVersion(1);
    auto metadata_info = generator.generateMetadataPathWithInfo();
    auto result = MetadataGenerator(metadata).generateManifestOnlySnapshot(generator, metadata_info.path, parent_snapshot_id);
    return result.snapshot->getObject(Iceberg::f_summary);
}

/// Append one snapshot, then drop `field_name` from its summary so it stands in for a parent
/// written by an engine that omits that optional counter. Returns the parent snapshot id.
Int64 appendParentWithoutSummaryField(Poco::JSON::Object::Ptr metadata, const char * field_name)
{
    appendSnapshot(metadata);
    auto parent = metadata->getArray(Iceberg::f_snapshots)->getObject(0);
    parent->getObject(Iceberg::f_summary)->remove(field_name);
    return parent->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
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

/// The `total-*` metrics are optional per the Iceberg spec, so a parent snapshot written by another
/// engine may omit them. A manifest-only rewrite adds no data, so every such total is unchanged and
/// an absent one stays absent; refusing the rewrite would leave OPTIMIZE ... MANIFEST permanently
/// unavailable on a table that reads correctly. Totals the parent does carry are still forwarded.
TEST(IcebergMetadataGenerator, ManifestOnlySnapshotWhenParentTotalRecordsMissing)
{
    auto metadata = makeMinimalV2Metadata();
    auto parent_snapshot_id = appendParentWithoutSummaryField(metadata, Iceberg::f_total_records);

    Poco::JSON::Object::Ptr summary;
    ASSERT_NO_THROW(summary = callManifestOnlySnapshot(metadata, parent_snapshot_id));

    /// The parent's own summary would satisfy the total-* assertions below, so pin the new snapshot
    /// first: `replace` separates it from the parent's `append`, and it must be attached and current.
    EXPECT_EQ(summary->getValue<String>(Iceberg::f_operation), Iceberg::f_replace);
    EXPECT_EQ(metadata->getArray(Iceberg::f_snapshots)->size(), 2u);
    EXPECT_NE(metadata->getValue<Int64>(Iceberg::f_current_snapshot_id), parent_snapshot_id);

    EXPECT_FALSE(summary->has(Iceberg::f_total_records));
    EXPECT_EQ(summary->getValue<String>(Iceberg::f_total_files_size), "100");
    EXPECT_EQ(summary->getValue<String>(Iceberg::f_total_data_files), "1");
}

/// The same absent counter must still be rejected when the commit moves it: a delta cannot be added
/// to an unknown total, and writing the delta alone would report a total missing the parent's rows.
TEST(IcebergMetadataGenerator, ThrowsWhenParentTotalRecordsMissingAndCommitAddsRecords)
{
    auto metadata = makeMinimalV2Metadata();
    auto parent_snapshot_id = appendParentWithoutSummaryField(metadata, Iceberg::f_total_records);
    try
    {
        appendSnapshot(metadata, parent_snapshot_id);
        FAIL() << "Expected BAD_ARGUMENTS";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::BAD_ARGUMENTS);
    }
}

#endif
