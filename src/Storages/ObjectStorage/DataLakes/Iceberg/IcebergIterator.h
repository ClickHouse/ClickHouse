#pragma once
#include "config.h"

#if USE_AVRO

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Core/Types.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>

#include <Common/ConcurrentBoundedQueue.h>

#include <optional>
#include <base/defines.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>

namespace CurrentMetrics
{
    extern const Metric StorageObjectStorageThreads;
    extern const Metric StorageObjectStorageThreadsActive;
    extern const Metric StorageObjectStorageThreadsScheduled;
}
namespace DB {

namespace Iceberg
{

class SingleThreadIcebergKeysIterator
{
public:
    SingleThreadIcebergKeysIterator(
        ObjectStoragePtr object_storage_,
        ContextPtr local_context_,
        Iceberg::FileContentType content_type_,
        StorageObjectStorageConfigurationWeakPtr configuration_,
        const ActionsDAG * filter_dag_,
        IcebergTableStateSnapshotPtr table_snapshot_,
        IcebergDataSnapshotPtr data_snapshot_,
        IcebergMetadataFilesCachePtr iceberg_metadata_cache_,
        IcebergSchemaProcessorPtr schema_processor_,
        Int32 format_version_,
        String table_location_);

    std::optional<DB::Iceberg::ManifestFileEntry> next();

    ~SingleThreadIcebergKeysIterator();

private:
    ObjectStoragePtr object_storage;
    std::shared_ptr<const ActionsDAG> filter_dag;
    ContextPtr local_context;
    Iceberg::IcebergTableStateSnapshotPtr table_snapshot;
    Iceberg::IcebergDataSnapshotPtr data_snapshot;
    IcebergMetadataFilesCachePtr iceberg_metadata_cache;
    IcebergSchemaProcessorPtr schema_processor;
    StorageObjectStorageConfigurationWeakPtr configuration;
    bool use_partition_pruning;
    const Int32 format_version;
    const String table_location;
    LoggerPtr log;


    // By Iceberg design it is difficult to avoid storing position deletes in memory.
    size_t manifest_file_index = 0;
    size_t internal_data_index = 0;
    Iceberg::ManifestFilePtr current_manifest_file_content;
    Int32 previous_entry_schema = -1;
    std::optional<Iceberg::ManifestFilesPruner> current_pruner;

    const Iceberg::FileContentType content_type;
    const Iceberg::ManifestFileContentType manifest_file_content_type;

    size_t min_max_index_pruned_files = 0;
    size_t partition_pruned_files = 0;
};

}

class IcebergIterator : public IObjectIterator 
{
public:
    template <typename... Args>
    explicit IcebergIterator(
        ObjectStoragePtr object_storage_,
        ContextPtr local_context_,
        StorageObjectStorageConfigurationWeakPtr configuration_,
        const ActionsDAG * filter_dag_,
        IDataLakeMetadata::FileProgressCallback callback_,
        Args &&... args)
        : filter_dag(filter_dag_ ? std::make_unique<ActionsDAG>(filter_dag_->clone()) : nullptr)
        , object_storage(std::move(object_storage_))
        , data_files_iterator(
              object_storage, local_context_, Iceberg::FileContentType::DATA, configuration_, filter_dag.get(), std::forward<Args>(args)...)
        , position_deletes_iterator(
              object_storage,
              local_context_,
              Iceberg::FileContentType::POSITION_DELETE,
              configuration_,
              filter_dag.get(),
              std::forward<Args>(args)...)
        , blocking_queue(100)
        , producer_task(local_context_->getSchedulePool().createTask(
              "IcebergMetaReaderThread",
              [this]
              {
                  while (!blocking_queue.isFinished())
                  {
                      auto info = data_files_iterator.next();
                      if (!info.has_value())
                          break;
                      [[maybe_unused]] auto pushed = blocking_queue.push(std::move(info.value()));
                  }
                  blocking_queue.finish();
              }))
        , callback(std::move(callback_))
        , format(configuration_.lock()->format)
        , compression_method(configuration_.lock()->compression_method)
        , position_deletes_files(
              [this]()
              {
                  producer_task->activateAndSchedule();

                  std::vector<Iceberg::ManifestFileEntry> position_deletes_files_tmp;
                  auto position_delete = position_deletes_iterator.next();
                  while (position_delete.has_value())
                  {
                      position_deletes_files_tmp.push_back(std::move(position_delete.value()));
                      position_delete = position_deletes_iterator.next();
                  }
                  std::sort(position_deletes_files_tmp.begin(), position_deletes_files_tmp.end());
                  return position_deletes_files_tmp;
              }())
    {
    }

    ObjectInfoPtr next(size_t) override;

    size_t estimatedKeysCount() override;
    ~IcebergIterator() override;

    std::shared_ptr<ISimpleTransform> getPositionDeleteTransformer(
        const ObjectInfoPtr & object_info,
        const SharedHeader & header,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context_) const override;

private:
    std::unique_ptr<ActionsDAG> filter_dag;
    ObjectStoragePtr object_storage;
    Iceberg::SingleThreadIcebergKeysIterator data_files_iterator;
    Iceberg::SingleThreadIcebergKeysIterator position_deletes_iterator;
    ConcurrentBoundedQueue<Iceberg::ManifestFileEntry> blocking_queue;
    BackgroundSchedulePool::TaskHolder producer_task;
    IDataLakeMetadata::FileProgressCallback callback;
    const String format;
    const String compression_method;
    const std::vector<Iceberg::ManifestFileEntry> position_deletes_files;
};
}


#endif
