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
#include <Common/ThreadPool.h>
#include <Common/threadPoolCallbackRunner.h>
#include <optional>
#include <future>
#include <vector>
#include <base/defines.h>

#include <Core/BackgroundSchedulePool.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>

namespace DB
{

namespace Iceberg
{

class SingleThreadIcebergKeysIterator
{
public:
    SingleThreadIcebergKeysIterator(
        ObjectStoragePtr object_storage_,
        ContextPtr local_context_,
        Iceberg::ManifestFileContentType manifest_file_content_type_,
        const ActionsDAG * filter_dag_,
        TableStateSnapshotPtr table_snapshot_,
        IcebergDataSnapshotPtr data_snapshot_,
        PersistentTableComponents persistent_components);

    std::optional<DB::Iceberg::ProcessedManifestFileEntryPtr> next();

    ~SingleThreadIcebergKeysIterator();

private:
    void initParallelPrefetch();

    ObjectStoragePtr object_storage;
    std::shared_ptr<const ActionsDAG> filter_dag;
    ContextPtr local_context;
    Iceberg::TableStateSnapshotPtr table_snapshot;
    Iceberg::IcebergDataSnapshotPtr data_snapshot;
    PersistentTableComponents persistent_components;
    LoggerPtr log;

    /// Serial iteration state (used when parallel_loading_threads == 1)
    size_t manifest_file_index = 0;
    Iceberg::ManifestIteratorPtr current_manifest_file_iterator;

    const Iceberg::ManifestFileContentType manifest_file_content_type;

    /// Parallel prefetch state (used when parallel_loading_threads > 1)
    UInt64 parallel_loading_threads = 1;

    using ManifestFetchRunner = ThreadPoolCallbackRunnerLocal<Iceberg::ManifestFileCacheableInfo>;

    /// Dedicated K-sized pool + runner for parallel manifest prefetch; concurrency is bounded
    /// by the pool size K (the codebase convention, see BlobCopierThread).
    /// enqueueAndGiveOwnership tasks are NOT tracked by the runner, so its destructor does not
    /// wait for them, and the running task wrapper dereferences the runner (this->thread_name).
    /// The destructor therefore MUST drain the give-ownership tasks (while both runner and pool
    /// are still alive) before any member tears down. After that drain nothing is in flight, so
    /// the relative destruction order of these members is immaterial.
    std::optional<ThreadPool> prefetch_pool;
    std::optional<ManifestFetchRunner> prefetch_runner;
    /// Task handles and their manifest_list indices, both in submission (manifest_list) order.
    std::vector<size_t> prefetch_manifest_indices;
    std::vector<std::shared_ptr<ManifestFetchRunner::Task>> prefetch_tasks;
    size_t prefetch_consume_pos = 0;
    bool prefetch_initialized = false;
};

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
    LoggerPtr logger;
    std::shared_ptr<ActionsDAG> filter_dag;
    ObjectStoragePtr object_storage;
    const Iceberg::TableStateSnapshotPtr table_state_snapshot;
    Iceberg::PersistentTableComponents persistent_components;
    Iceberg::SingleThreadIcebergKeysIterator data_files_iterator;
    Iceberg::SingleThreadIcebergKeysIterator deletes_iterator;
    ConcurrentBoundedQueue<Iceberg::ProcessedManifestFileEntryPtr> blocking_queue;
    std::unique_ptr<ThreadFromGlobalPool> producer_task;
    IDataLakeMetadata::FileProgressCallback callback;
    std::vector<Iceberg::ProcessedManifestFileEntryPtr> position_deletes_files;
    std::vector<Iceberg::ProcessedManifestFileEntryPtr> equality_deletes_files;
    std::exception_ptr exception;
    std::mutex exception_mutex;
};
}


#endif
