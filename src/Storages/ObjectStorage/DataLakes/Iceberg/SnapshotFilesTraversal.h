#pragma once

#include "config.h"

#if USE_AVRO

#include <unordered_set>
#include <utility>
#include <vector>

#include <Common/Logger_fwd.h>
#include <Core/Types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Array.h>

#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/Utils.h>

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
    SecondaryStorages & secondary_storages);

struct ReachableFilesResult
{
    /// Base-storage keys of reachable files inside `table_path`, for matching against a base-storage listing.
    std::unordered_set<String> files;
    Int32 metadata_version;
    /// Reachable files a base-storage listing of `table_path` cannot see: on a secondary storage, or on
    /// the base storage but outside `table_path`. Deduplicated and paired with their storage.
    std::vector<std::pair<ObjectStoragePtr, String>> external_files;
};

/// Collect all files reachable through the current metadata graph.
///
/// Traverses: metadata JSON files (from metadata-log), manifest lists (from snapshots),
/// manifest files (from manifest lists), data/delete files (from manifest files),
/// and statistics files. Base-storage files inside `table_path` go to `files` (as keys); everything
/// else goes to `external_files` (as resolved (storage, key) pairs). Also returns the metadata version
/// used, for TOCTOU detection.
ReachableFilesResult collectReachableFiles(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    const DataLakeStorageSettings & data_lake_settings,
    ContextPtr context,
    LoggerPtr log,
    SecondaryStorages & secondary_storages);

}

#endif
