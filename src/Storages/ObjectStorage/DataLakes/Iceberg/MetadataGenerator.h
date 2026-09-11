#pragma once

#include <pcg_random.hpp>
#include "config.h"

#include <DataTypes/IDataType.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Poco/JSON/Object.h>


namespace DB
{

#if USE_AVRO

class MetadataGenerator
{
public:
    explicit MetadataGenerator(Poco::JSON::Object::Ptr metadata_object_);

    struct NextMetadataResult
    {
        Poco::JSON::Object::Ptr snapshot = nullptr;
        /// Metadata path for the manifest list file; resolve for I/O, serialize for writing into Iceberg metadata.
        Iceberg::IcebergPathFromMetadata manifest_list_path;
    };

    enum class SnapshotOperation
    {
        Append,
        Replace,
    };

    /// Fails close on a live parent snapshot that cannot be found in `snapshots`,
    /// except when `tolerate_missing_parent_snapshot` is set: compaction replays a
    /// filtered history where a record's parent may be a legitimately skipped snapshot.
    NextMetadataResult generateNextMetadata(
        FileNamesGenerator & generator,
        const Iceberg::IcebergPathFromMetadata & metadata_file_path,
        Int64 parent_snapshot_id,
        Int64 added_files,
        Int64 added_records,
        Int64 added_files_size,
        Int64 num_partitions,
        Int64 added_delete_files,
        Int64 num_deleted_rows,
        std::optional<Int64> user_defined_snapshot_id = std::nullopt,
        std::optional<Int64> user_defined_timestamp = std::nullopt,
        SnapshotOperation operation = SnapshotOperation::Append,
        bool tolerate_missing_parent_snapshot = false);

    /// Throws if `parent_snapshot_id` is a live snapshot (>= 0) absent from the metadata's
    /// `snapshots` list. `generateNextMetadata` enforces the same invariant, but only at commit
    /// time, after a write has already uploaded its data files; call this up front (before any
    /// data is written) so a self-contradictory table fails without leaving orphaned objects.
    static void validateParentSnapshotResolvable(const Poco::JSON::Object::Ptr & metadata_object, Int64 parent_snapshot_id);

    /// Create a manifest-only rewrite snapshot (`replace` operation) carrying `total-*` counters forward so `OPTIMIZE ... MANIFEST` is idempotent.
    NextMetadataResult generateManifestOnlySnapshot(
        FileNamesGenerator & generator,
        const Iceberg::IcebergPathFromMetadata & metadata_file_path,
        Int64 parent_snapshot_id);

    void generateAddColumnMetadata(const String & column_name, DataTypePtr type);
    void generateDropColumnMetadata(const String & column_name);
    void generateModifyColumnMetadata(const String & column_name, DataTypePtr type);
    void generateRenameColumnMetadata(const String & column_name, const String & new_column_name);

private:
    Poco::JSON::Object::Ptr metadata_object;

    pcg64_fast gen;
    std::uniform_int_distribution<Int64> dis;

    Int64 getMaxSequenceNumber();
    Poco::JSON::Object::Ptr getParentSnapshot(Int64 parent_snapshot_id);
};

#endif

}
