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

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool_fwd.h>

#include <mutex>
#include <base/defines.h>

#include <Core/BackgroundSchedulePool.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>

namespace DB
{

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
    void decodeDeleteManifests();
    void decodeDataManifests();
    Iceberg::ManifestIteratorPtr createManifestIterator(const ManifestFileCacheKey & manifest_list_entry) const;
    std::vector<Iceberg::ProcessedManifestFileEntryPtr> decodeManifest(const ManifestFileCacheKey & manifest_list_entry) const;

    LoggerPtr logger;
    ObjectStoragePtr object_storage;
    ContextPtr local_context;
    const Iceberg::TableStateSnapshotPtr table_state_snapshot;
    Iceberg::IcebergDataSnapshotPtr data_snapshot;
    Iceberg::PersistentTableComponents persistent_components;
    /// Shared read-only by the concurrent data- and delete-manifest decode tasks.
    std::shared_ptr<const ActionsDAG> manifest_filter_dag;
    ConcurrentBoundedQueue<Iceberg::ProcessedManifestFileEntryPtr> blocking_queue;
    std::unique_ptr<ThreadFromGlobalPool> producer_task;
    IDataLakeMetadata::FileProgressCallback callback;
    /// Filled once under `deletes_mutex` and never mutated afterwards, so `next` may read them
    /// unguarded once it has gone through `ensureDeletesReady`.
    std::vector<Iceberg::ProcessedManifestFileEntryPtr> position_deletes_files;
    std::vector<Iceberg::ProcessedManifestFileEntryPtr> equality_deletes_files;
    std::mutex deletes_mutex;
    bool deletes_ready TSA_GUARDED_BY(deletes_mutex) = false;
    std::exception_ptr deletes_exception TSA_GUARDED_BY(deletes_mutex);
    std::exception_ptr exception;
    std::mutex exception_mutex;
};
}


#endif
