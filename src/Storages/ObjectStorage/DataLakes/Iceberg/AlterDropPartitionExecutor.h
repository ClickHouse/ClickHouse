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

#include <functional>
#include <optional>
#include <unordered_set>
#include <vector>

namespace DB
{

class FileNamesGenerator;

namespace Iceberg
{

class AlterDropPartitionExecutor
{
public:
    AlterDropPartitionExecutor(
        const PartitionCommand & command_,
        ContextPtr context_,
        ObjectStoragePtr object_storage_,
        const PersistentTableComponents & components_,
        const DataLakeStorageSettings & data_lake_settings_,
        String write_format_,
        LoggerPtr log_,
        std::function<std::pair<IcebergDataSnapshotPtr, TableStateSnapshot>()> fetch_latest_state_);

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
        std::vector<DataTypePtr> partition_types;
    };

    struct TargetManifest
    {
        IcebergPathFromMetadata manifest_path;
        ManifestFileContentType manifest_content_type = ManifestFileContentType::DATA;
        std::vector<ProcessedManifestFileEntryPtr> entries_to_keep;     // re-emitted as EXISTING
        std::vector<ProcessedManifestFileEntryPtr> entries_to_remove;   // dropped
    };

    struct TargetManifests
    {
        std::vector<TargetManifest> fully_matched;     // entries_to_keep is empty
        std::vector<TargetManifest> partially_matched; // entries_to_keep is non-empty
    };

    struct DropPlan
    {
        TargetManifests target_manifests;
        Iceberg::SnapshotSummaryUpdateDelete snapshot_summary_update;

        explicit DropPlan(TargetManifests && target_manifests_);
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
        Int32 existing_rows_count = 0;
        Int32 existing_files_count = 0;
        FileContentType content_type = FileContentType::DATA;
    };

    std::optional<SnapshotState> fetchSnapshotState();

    TargetFilePaths discoverTargetFilePaths(const SnapshotState & state, const Row & target_partition) const;
    TargetManifests findTargetManifests(const SnapshotState & state, const TargetFilePaths & targets) const;

    static void matchEntries(
        const std::vector<ProcessedManifestFileEntryPtr> & entries,
        const std::unordered_set<String> & target_paths,
        const IcebergPathResolver & path_resolver,
        TargetManifest & out);

    bool tryCommit(SnapshotState & state, DropPlan plan);

    std::vector<ReplacementManifestWrite> writeReplacementManifests(
        const SnapshotState & state,
        const DropPlan & plan,
        FileNamesGenerator & filename_generator,
        std::vector<String> & files_for_cleanup);

    struct ManifestListWriteResult
    {
        Poco::JSON::Object::Ptr new_snapshot;
        GeneratedMetadataFileWithInfo metadata_info;
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
        const GeneratedMetadataFileWithInfo & metadata_info);

    const PartitionCommand & command;
    ContextPtr context;
    ObjectStoragePtr object_storage;
    const PersistentTableComponents & components;
    const DataLakeStorageSettings & data_lake_settings;
    String write_format;
    LoggerPtr log;
    std::function<std::pair<IcebergDataSnapshotPtr, TableStateSnapshot>()> fetch_latest_state;
};

}
}

#endif
