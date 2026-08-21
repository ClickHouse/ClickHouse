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

class SingleThreadIcebergKeysIterator
{
    using FilesGenerator = std::function<std::vector<ManifestFileEntryPtr>(const Iceberg::ManifestFilePtr & manifest_file)>;
public:
    SingleThreadIcebergKeysIterator(
        ObjectStoragePtr object_storage_,
        ContextPtr local_context_,
        FilesGenerator files_generator_,
        Iceberg::ManifestFileContentType manifest_file_content_type_,
        const ActionsDAG * filter_dag_,
        TableStateSnapshotPtr table_snapshot_,
        IcebergDataSnapshotPtr data_snapshot_,
        PersistentTableComponents persistent_components);

    std::optional<DB::Iceberg::ManifestFileEntryPtr> next();

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
    std::vector<ManifestFileEntryPtr> files;


    // By Iceberg design it is difficult to avoid storing position deletes in memory.
    size_t manifest_file_index = 0;
    size_t internal_data_index = 0;
    Iceberg::ManifestFilePtr current_manifest_file_content;
    Int32 previous_entry_schema = -1;
    std::optional<Iceberg::ManifestFilesPruner> current_pruner;

    const Iceberg::ManifestFileContentType manifest_file_content_type;
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
    std::unique_ptr<ActionsDAG> filter_dag;
    ObjectStoragePtr object_storage;
    const Iceberg::TableStateSnapshotPtr table_state_snapshot;
    Iceberg::PersistentTableComponents persistent_components;
    Iceberg::SingleThreadIcebergKeysIterator data_files_iterator;
    Iceberg::SingleThreadIcebergKeysIterator deletes_iterator;
    ConcurrentBoundedQueue<Iceberg::ManifestFileEntryPtr> blocking_queue;
    std::optional<ThreadFromGlobalPool> producer_task;
    IDataLakeMetadata::FileProgressCallback callback;
    std::vector<Iceberg::ManifestFileEntryPtr> position_deletes_files;
    std::vector<Iceberg::ManifestFileEntryPtr> equality_deletes_files;
    std::exception_ptr exception;
    std::mutex exception_mutex;
};
}


#endif
