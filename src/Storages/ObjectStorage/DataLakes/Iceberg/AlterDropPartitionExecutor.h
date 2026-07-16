#pragma once

#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotSummary.h>
#include "config.h"

#if USE_AVRO

#include <Core/Field.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/PartitionCommands.h>
#include <Interpreters/StorageID.h>
#include <Common/Logger.h>

#include <Poco/JSON/Object.h>

#include <optional>
#include <unordered_set>
#include <vector>

namespace DataLake
{
class ICatalog;
}

namespace DB
{

class FileNamesGenerator;
class IcebergMetadata;

namespace Iceberg
{

class AlterDropPartitionExecutor
{
public:
    AlterDropPartitionExecutor(
        const PartitionCommand & command_,
        const IcebergMetadata & metadata_,
        ContextPtr context_,
        ObjectStoragePtr object_storage_,
        const PersistentTableComponents & components_,
        const DataLakeStorageSettings & data_lake_settings_,
        String write_format_,
        LoggerPtr log_,
        std::shared_ptr<DataLake::ICatalog> catalog_,
        StorageID storage_id_);

    void run();

private:
    struct SnapshotState
    {
        IcebergDataSnapshotPtr snapshot;
        TableStateSnapshot table_state;
        Poco::JSON::Object::Ptr metadata_object;
        Poco::JSON::Object::Ptr partition_spec;
        Int64 partition_spec_id = 0;
        Int32 schema_id = 0;
        std::vector<String> partition_columns;
        DataTypes partition_types;
    };

    struct TargetManifest
    {
        ManifestFileCacheKey manifest_key;
    };

    struct TargetManifests
    {
        std::vector<TargetManifest> fully_matched;
        std::vector<TargetManifest> partially_matched;

        bool empty() const { return fully_matched.empty() && partially_matched.empty(); }
    };

    struct DropPlan
    {
        TargetManifests target_manifests;
        Iceberg::SnapshotSummaryUpdateDelete snapshot_summary_update;
    };

    struct TargetFilePaths
    {
        std::unordered_set<String> data;
        std::unordered_set<String> position_delete;

        bool empty() const { return data.empty() && position_delete.empty(); }
    };

    struct ReplacementManifestWrite
    {
        IcebergPathFromMetadata path;
        Int64 length = 0;
        Int64 min_sequence_number = 0;
        Int64 existing_rows_count = 0;
        Int64 existing_files_count = 0;
        FileContentType content_type = FileContentType::DATA;
    };

    std::optional<SnapshotState> fetchSnapshotState();
    std::pair<IcebergDataSnapshotPtr, TableStateSnapshot> fetchLatestState() const;

    TargetFilePaths discoverTargetFilePaths(const SnapshotState & state, const Row & target_partition) const;
    DropPlan buildDropPlan(const SnapshotState & state, const TargetFilePaths & targets, bool require_all_targets) const;
    bool tryCommit(SnapshotState & state, const TargetFilePaths & targets, DropPlan plan);

    std::vector<ReplacementManifestWrite> writeReplacementManifests(
        const SnapshotState & state, const TargetFilePaths & targets, const DropPlan & plan,
        FileNamesGenerator & filename_generator,
        std::vector<String> & files_for_cleanup);

    struct ManifestListWriteResult
    {
        GeneratedMetadataFileWithInfo metadata_info;
        /// The newly committed snapshot, needed for the catalog commit of catalog-backed tables.
        Poco::JSON::Object::Ptr new_snapshot;
    };

    ManifestListWriteResult writeManifestList(
        SnapshotState & state,
        const DropPlan & plan,
        const std::vector<ReplacementManifestWrite> & replacements,
        FileNamesGenerator & filename_generator,
        std::vector<String> & files_for_cleanup);

    bool commitMetadataJSON(
        SnapshotState & state,
        FileNamesGenerator & filename_generator,
        const GeneratedMetadataFileWithInfo & metadata_info,
        const Poco::JSON::Object::Ptr & new_snapshot);

    void cleanupNotCommited(std::vector<std::string> files);

    const PartitionCommand & command;
    const IcebergMetadata & metadata;
    ContextPtr context;
    ObjectStoragePtr object_storage;
    const PersistentTableComponents & components;
    const DataLakeStorageSettings & data_lake_settings;
    String write_format;
    LoggerPtr log;
    /// Set for catalog-backed (DatabaseDataLake) tables; the new metadata location must also be
    /// committed to the catalog (the shared source of truth), like the INSERT/DELETE write paths do.
    std::shared_ptr<DataLake::ICatalog> catalog;
    StorageID storage_id;
};

}
}

#endif
