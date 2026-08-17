#pragma once

#include <pcg_random.hpp>
#include "config.h"

#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
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
        /// Metadata path for the manifest list file (e.g. "wasb://container@account/table/metadata/snap-xxx.avro").
        /// Use IcebergPathResolver::resolve to get storage path for I/O.
        /// Use .serialize() to get the path for writing into Iceberg metadata.
        Iceberg::IcebergPathFromMetadata manifest_list_path;
    };

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
        bool is_truncate = false);

    void generateAddColumnMetadata(const String & column_name, DataTypePtr type);
    void generateDropColumnMetadata(const String & column_name);
    /// Returns false when the column already has the requested type (no metadata change).
    /// `context` supplies the settings used to map the stored Iceberg type back to a ClickHouse
    /// type (the timestamptz timezone and whether geo types are allowed).
    bool generateModifyColumnMetadata(const String & column_name, DataTypePtr type, ContextPtr context);
    void generateRenameColumnMetadata(const String & column_name, const String & new_column_name);

    /// A commit attempt can land in the catalog even when the client observes a failure
    /// (the Iceberg "commit state unknown" case, e.g. a proxy returning 5xx after the catalog
    /// applied the update). These predicates let a retry detect that the requested change is
    /// already present instead of applying it a second time and failing.
    bool isAddColumnApplied(const String & column_name, DataTypePtr type) const;
    bool isDropColumnApplied(const String & column_name) const;
    bool isRenameColumnApplied(const String & column_name, const String & new_column_name) const;
    bool isModifyColumnApplied(const String & column_name, DataTypePtr type) const;

private:
    Poco::JSON::Object::Ptr metadata_object;

    pcg64_fast gen;
    std::uniform_int_distribution<Int64> dis;

    Int64 getMaxSequenceNumber();
    Poco::JSON::Object::Ptr getParentSnapshot(Int64 parent_snapshot_id);

    /// Returns the schema referenced by `current-schema-id`, or nullptr when it is absent.
    Poco::JSON::Object::Ptr findCurrentSchema() const;
    /// Returns the schema referenced by `current-schema-id`, throwing when it is absent.
    Poco::JSON::Object::Ptr getCurrentSchema() const;
};

#endif

}
