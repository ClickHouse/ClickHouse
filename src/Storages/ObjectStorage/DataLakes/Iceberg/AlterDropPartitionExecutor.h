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
#include <Common/Logger.h>

#include <Poco/JSON/Object.h>

#include <optional>
#include <unordered_set>
#include <vector>

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
        LoggerPtr log_);

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

    using TargetFilePaths = std::unordered_set<String>;

    std::optional<SnapshotState> fetchSnapshotState();

    TargetFilePaths discoverTargetFilePaths(const SnapshotState & state, const Row & target_partition) const;
    DropPlan buildDropPlan(const SnapshotState & state, const TargetFilePaths & targets) const;
    bool tryCommit(SnapshotState & state, const DropPlan & plan);

    struct ManifestListWriteResult
    {
        GeneratedMetadataFileWithInfo metadata_info;
    };

    ManifestListWriteResult writeManifestList(
        SnapshotState & state, const DropPlan & plan, FileNamesGenerator & filename_generator, std::vector<String> & files_for_cleanup);

    bool
    commitMetadataJSON(SnapshotState & state, FileNamesGenerator & filename_generator, const GeneratedMetadataFileWithInfo & metadata_info);

    void cleanupNotCommited(std::vector<std::string> files);

    const PartitionCommand & command;
    const IcebergMetadata & metadata;
    ContextPtr context;
    ObjectStoragePtr object_storage;
    const PersistentTableComponents & components;
    const DataLakeStorageSettings & data_lake_settings;
    String write_format;
    LoggerPtr log;
};

}
}

#endif
