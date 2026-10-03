#pragma once

#include "config.h"

#if USE_AVRO

#include <unordered_set>

#include <Common/Logger_fwd.h>
#include <Core/Types.h>
#include <Databases/DataLake/ICatalog.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

namespace DB::Iceberg
{

/// Paths collected from Iceberg snapshot metadata, stored as opaque
/// IcebergPathFromMetadata values that must be resolved through
/// IcebergPathResolver before I/O operations.
struct SnapshotReferencedFiles
{
    std::unordered_set<IcebergPathFromMetadata> manifest_list_paths;
    std::unordered_set<IcebergPathFromMetadata> manifest_paths;
    std::unordered_set<IcebergPathFromMetadata> data_file_paths;
};

SnapshotReferencedFiles collectSnapshotReferencedFiles(
    const Poco::JSON::Array::Ptr & snapshots,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    Int32 current_schema_id);

struct ReachableFilesRoot
{
    Int32 metadata_version;
    /// Resolved storage path of the metadata file the traversal was rooted at. Two distinct
    /// files can share a version number, so identity of the root is this path, not the number.
    String metadata_path;
    Poco::JSON::Object::Ptr metadata;
};

ReachableFilesRoot resolveReachableFilesRoot(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    const DataLakeStorageSettings & data_lake_settings,
    ContextPtr context,
    LoggerPtr log,
    const std::shared_ptr<DataLake::ICatalog> & catalog = nullptr,
    const String & table_identifier = {});

/// Collect all files reachable through the metadata graph.
///
/// Traverses: metadata JSON files (from metadata-log), manifest lists (from snapshots),
/// manifest files (from manifest lists), data/delete files (from manifest files),
/// and statistics files. All returned paths are resolved storage paths.
std::unordered_set<String> collectReachableFiles(
    const ReachableFilesRoot & root,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log);

}

#endif
