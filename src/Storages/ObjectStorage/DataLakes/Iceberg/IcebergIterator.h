#pragma once
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include "config.h"

#if USE_AVRO

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Core/Types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>

#include <condition_variable>
#include <mutex>
#include <vector>
#include <base/defines.h>

#include <Core/BackgroundSchedulePool.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestListPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>

namespace DB
{

namespace Iceberg
{
class ManifestFilesPrefetcher;
}

class IcebergIterator : public IObjectIterator
{
public:
    explicit IcebergIterator(
        ObjectStoragePtr object_storage_,
        ContextPtr local_context_,
        const ActionsDAG * filter_dag_,
        IDataLakeMetadata::FileProgressCallback callback_,
        Iceberg::TableStateSnapshotPtr table_snapshot_,
        Iceberg::IcebergDataSnapshotPtr data_snapshot_,
        Iceberg::PersistentTableComponents persistent_components);

    ObjectInfoPtr next(size_t) override;

    size_t estimatedKeysCount() override;
    ~IcebergIterator() override;

private:
    void ensureDeletesReady();
    Iceberg::ProcessedManifestFileEntryPtr nextDataFileEntry();
    Iceberg::ManifestIteratorPtr createManifestIterator(
        const ManifestFileCacheKey & manifest_list_entry, const Iceberg::ManifestFileCacheableInfo & manifest_file) const;

    LoggerPtr logger;
    ObjectStoragePtr object_storage;
    ContextPtr local_context;
    const Iceberg::TableStateSnapshotPtr table_state_snapshot;
    Iceberg::IcebergDataSnapshotPtr data_snapshot;
    Iceberg::PersistentTableComponents persistent_components;
    /// Shared read-only by the threads parsing data and delete manifests concurrently.
    std::shared_ptr<const ActionsDAG> manifest_filter_dag;
    IDataLakeMetadata::FileProgressCallback callback;

    /// Filled under `deletes_mutex` and never mutated after `deletes_ready`, so `next` may read them
    /// unguarded once it has gone through `ensureDeletesReady`.
    std::vector<Iceberg::ProcessedManifestFileEntryPtr> position_deletes_files;
    std::vector<Iceberg::ProcessedManifestFileEntryPtr> equality_deletes_files;
    std::mutex deletes_mutex;
    std::condition_variable delete_manifest_parsed;
    /// Delete manifests not yet taken.
    size_t delete_manifests_left TSA_GUARDED_BY(deletes_mutex) = 0;
    size_t delete_manifests_in_progress TSA_GUARDED_BY(deletes_mutex) = 0;
    bool deletes_ready TSA_GUARDED_BY(deletes_mutex) = false;
    std::exception_ptr deletes_exception TSA_GUARDED_BY(deletes_mutex);

    std::mutex data_files_mutex;
    std::condition_variable data_manifest_released;
    /// The delete manifests, then the data manifests: one read-ahead window for both.
    std::unique_ptr<Iceberg::ManifestFilesPrefetcher> manifests TSA_GUARDED_BY(data_files_mutex);
    /// Partially read manifests, each used by one thread at a time.
    std::vector<Iceberg::ManifestIteratorPtr> idle_data_manifests TSA_GUARDED_BY(data_files_mutex);
    size_t data_manifests_in_use TSA_GUARDED_BY(data_files_mutex) = 0;
    std::exception_ptr data_files_exception TSA_GUARDED_BY(data_files_mutex);
};
}


#endif
