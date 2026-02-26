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
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>

#include <Common/ConcurrentBoundedQueue.h>

#include <optional>
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
template <typename HeavyCPUProcessingFunction>
class HomogeneousIcebergKeysIterator
{
    using FilesGenerator = std::function<std::vector<ManifestFileEntryPtr>(const Iceberg::ManifestFilePtr & manifest_file)>;

    using Result = std::invoke_result_t<HeavyCPUProcessingFunction, const Iceberg::ManifestFileEntryPtr &>;

    friend class ManifestFilesAsyncronousIterator;

public:
    HomogeneousIcebergKeysIterator(
        ObjectStoragePtr object_storage_,
        ContextPtr local_context_,
        Iceberg::ManifestFileContentType manifest_file_content_type_,
        const ActionsDAG * filter_dag_,
        TableStateSnapshotPtr table_snapshot_,
        IcebergDataSnapshotPtr data_snapshot_,
        PersistentTableComponents persistent_components,
        HeavyCPUProcessingFunction processing_function_);

    std::optional<Result> next();

private:
    ObjectStoragePtr object_storage;
    std::shared_ptr<const ActionsDAG> filter_dag;
    ContextPtr local_context;
    Iceberg::TableStateSnapshotPtr table_snapshot;
    Iceberg::IcebergDataSnapshotPtr data_snapshot;
    bool use_partition_pruning;
    PersistentTableComponents persistent_components;
    FilesGenerator files_generator;
    LoggerPtr log;
    const Iceberg::ManifestFileContentType manifest_file_content_type;
    ManifestFilePtr current_manifest_file_iterator;
    std::mutex current_manifest_file_mutex;
    HeavyCPUProcessingFunction processing_function;

    struct ManifestFileWeightFunction
    {
        size_t operator()(const Iceberg::ManifestFilePtr & manifest_file) const;
    };

    struct ManifestFilesAsyncronousIterator
    {
        ConcurrentBoundedQueue<Iceberg::ManifestFilePtr, ManifestFileWeightFunction> blocking_queue;
        std::atomic<size_t> index;
        const Iceberg::ManifestFileContentType manifest_file_content_type;
        const HomogeneousIcebergKeysIterator & parent;
        const size_t number_of_workers;
        std::deque<ThreadFromGlobalPool> workers;

        Iceberg::ManifestFilePtr next();

        Iceberg::ManifestFilePtr task();

        ManifestFilesAsyncronousIterator(
            Iceberg::ManifestFileContentType manifest_file_content_type_,
            const HomogeneousIcebergKeysIterator & parent_,
            size_t max_sum_size_of_manifest_files_in_queue_,
            size_t number_of_workers_);
    } manifest_files_asyncronous_iterator;
};

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
    std::optional<Iceberg::HomogeneousIcebergKeysIterator<std::function<ObjectInfoPtr(const Iceberg::ManifestFileEntryPtr & manifest_file)>>>
        data_files_iterator;
    Iceberg::HomogeneousIcebergKeysIterator<std::function<Iceberg::ManifestFileEntryPtr(const Iceberg::ManifestFileEntryPtr & manifest_file)>>
        deletes_iterator;
    IDataLakeMetadata::FileProgressCallback callback;
    std::vector<Iceberg::ManifestFileEntryPtr> position_deletes_files;
    std::vector<Iceberg::ManifestFileEntryPtr> equality_deletes_files;
    std::exception_ptr exception;
    std::mutex exception_mutex;

    ObjectInfoPtr convertToObjectInfo(const Iceberg::ManifestFileEntryPtr & manifest_file_entry);
};
}

}


#endif
