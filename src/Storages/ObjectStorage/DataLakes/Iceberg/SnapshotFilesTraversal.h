#pragma once

#include "config.h"

#if USE_AVRO

#include <unordered_set>
#include <utility>
#include <vector>

#include <Common/Logger_fwd.h>
#include <Core/Types.h>
#include <Databases/DataLake/ICatalog.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Array.h>

#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExternalPathResolver.h>

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
    Int32 current_schema_id,
    ExternalStorageCache & external_storages);

struct ReachableFilesResult
{
    std::unordered_set<String> files;
    Int32 metadata_version;
    /// Resolved storage path of the metadata file the traversal was rooted at. Two distinct
    /// files can share a version number, so identity of the root is this path, not the number.
    String metadata_path;
    std::vector<std::pair<ObjectStoragePtr, String>> external_files;
};

/// Use the latest head for `remove_orphan_files`, but the configured head for `drop`.
/// `scan_metadata_log_history` adds surviving external references without making expired in-table files reachable.
/// The returned root identifies the traversed state for TOCTOU checks.
ReachableFilesResult collectReachableFiles(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    const DataLakeStorageSettings & data_lake_settings,
    ContextPtr context,
    LoggerPtr log,
    ExternalStorageCache & external_storages,
    const std::shared_ptr<DataLake::ICatalog> & catalog,
    const String & table_identifier,
    bool scan_metadata_log_history,
    bool ignore_explicit_metadata_file_path);

}

#endif
