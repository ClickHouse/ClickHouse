#pragma once
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
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

#include <Common/SharedMutex.h>
#include <tuple>
#include <optional>
#include <base/defines.h>

#    include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#    include <Storages/ObjectStorage/StorageObjectStorage.h>

#    include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>

#include <mutex>

namespace CurrentMetrics
{
    extern const Metric StorageObjectStorageThreads;
    extern const Metric StorageObjectStorageThreadsActive;
    extern const Metric StorageObjectStorageThreadsScheduled;
}
namespace DB {

namespace Setting
{
extern const SettingsBool use_iceberg_partition_pruning;
}

class SingleThreadIcebergKeysIterator
{
public:
    SingleThreadIcebergKeysIterator(
        ObjectStoragePtr object_storage_,
        ContextPtr local_context_,
        Iceberg::FileContentType content_type_,
        StorageObjectStorageConfigurationWeakPtr configuration_,
        const ActionsDAG * filter_dag_,
        Iceberg::IcebergTableStateSnapshotPtr table_snapshot_,
        Iceberg::IcebergDataSnapshotPtr data_snapshot_,
        IcebergMetadataFilesCachePtr iceberg_metadata_cache_,
        IcebergSchemaProcessorPtr schema_processor_,
        Int32 format_version_,
        String table_location_)
        : object_storage(object_storage_)
        , filter_dag(filter_dag_ ? std::make_shared<ActionsDAG>(filter_dag_->clone()) : nullptr)
        , local_context(local_context_)
        , table_snapshot(table_snapshot_)
        , data_snapshot(data_snapshot_)
        , iceberg_metadata_cache(iceberg_metadata_cache_)
        , schema_processor(schema_processor_)
        , configuration(std::move(configuration_))
        , use_partition_pruning(
              [this]()
              {
                  if (!local_context && filter_dag)
                  {
                      throw DB::Exception(
                          DB::ErrorCodes::LOGICAL_ERROR,
                          "Context is required with non-empty filter_dag to implement "
                          "partition pruning for Iceberg table");
                  }
                  return filter_dag && local_context->getSettingsRef()[Setting::use_iceberg_partition_pruning].value;
              }())
        , format_version(format_version_)
        , table_location(std::move(table_location_))
        , log(getLogger("IcebergIterator"))
        , content_type(content_type_)
        , manifest_file_content_type(
              content_type_ == Iceberg::FileContentType::DATA ? Iceberg::ManifestFileContentType::DATA
                                                              : Iceberg::ManifestFileContentType::DELETE)
    {
    }

    std::optional<Iceberg::ManifestFileEntry> next();

    ~SingleThreadIcebergKeysIterator();

    std::shared_ptr<const ActionsDAG> getFilterDag() const { return filter_dag; }

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
    Int32 format_version;
    String table_location;
    LoggerPtr log;


    // By Iceberg design it is difficult to avoid storing position deletes in memory.
    size_t manifest_file_index = 0;
    size_t internal_data_index = 0;
    Iceberg::ManifestFilePtr current_manifest_file_content;
    Int32 previous_entry_schema = -1;
    std::optional<Iceberg::ManifestFilesPruner> current_pruner;

    Iceberg::FileContentType content_type;
    Iceberg::ManifestFileContentType manifest_file_content_type;

    std::optional<String> getRelevantManifestList(const Poco::JSON::Object::Ptr & metadata);
    Iceberg::ManifestFilePtr tryGetManifestFile(const String & filename) const;

    size_t min_max_index_pruned_files = 0;
    size_t partition_pruned_files = 0;
};

template <typename T>
class BlockingMCSP
{
public:
    explicit BlockingMCSP(size_t max_capacity_)
        : max_capacity(max_capacity_)
    {
    }

    void close()
    {
        std::unique_lock lock(mutex);
        closed = true;
        consumer_cv.notify_all();
    }

    void push(T value)
    {
        std::unique_lock lock(mutex);
        chassert(!closed, "Cannot push to closed BlockingMCSP");
        producer_cv.wait(lock, [this]() { return queue.size() < max_capacity || closed; });
        if (closed)
            return;
        bool was_empty = queue.empty();
        queue.push(std::move(value));
        if (was_empty)
        {
            consumer_cv.notify_one();
        }
    }

    std::optional<T> pop()
    {
        std::unique_lock lock(mutex);
        consumer_cv.wait(lock, [this]() { return !queue.empty() || closed; });
        if (closed && queue.empty())
        {
            return std::nullopt;
        }
        bool was_full = queue.size() == max_capacity;
        auto value = std::move(queue.front());
        queue.pop();
        if (was_full)
        {
            producer_cv.notify_one();
        }
        return value;
    }

    bool opened()
    {
        std::lock_guard lock(mutex);
        return !closed;
    }

private:
    std::queue<T> queue;
    size_t max_capacity;
    bool closed{false};
    std::mutex mutex;
    std::condition_variable consumer_cv;
    std::condition_variable producer_cv;
};


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
                  auto filtered_dag = data_files_iterator.getFilterDag();
                  while (blocking_queue.opened())
                  {
                      auto info = data_files_iterator.next();
                      if (!info.has_value())
                          break;
                      blocking_queue.push(std::move(info.value()));
                  }
                  blocking_queue.close();
              }))
        , format(configuration_.lock()->format)
        , compression_method(configuration_.lock()->compression_method)
        , callback(std::move(callback_))
    {
        producer_task->activateAndSchedule();
        auto position_delete = position_deletes_iterator.next();
        while (position_delete.has_value())
        {
            position_deletes_files.push_back(std::move(position_delete.value()));
            position_delete = position_deletes_iterator.next();
        }
        std::sort(position_deletes_files.begin(), position_deletes_files.end());
        LOG_DEBUG(
            &Poco::Logger::get("IcebergIterator"), "Iceberg iterator created with {} position delete files", position_deletes_files.size());
    }

    ObjectInfoPtr next(size_t) override
    {
        auto manifest_file_entry = blocking_queue.pop();
        if (manifest_file_entry)
        {
            LOG_DEBUG(
                &Poco::Logger::get("IcebergIterator"),
                "Iceberg iterator next file key: {}, path: {}",
                manifest_file_entry->file_path_key,
                manifest_file_entry->file_path);
            return std::make_shared<IcebergDataObjectInfo>(*manifest_file_entry, position_deletes_files, format);
        }
        return nullptr;
    }

    size_t estimatedKeysCount() override
    {
        return std::numeric_limits<size_t>::max();
    }

    ~IcebergIterator() override
    {
        blocking_queue.close();
        producer_task->deactivate();
    }

    std::shared_ptr<ISimpleTransform> getPositionDeleteTransformer(
        const ObjectInfoPtr & object_info,
        const SharedHeader & header,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context_) const override
    {
        auto iceberg_object_info = std::dynamic_pointer_cast<IcebergDataObjectInfo>(object_info);
        if (!iceberg_object_info)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "The object with path '{}' info is not IcebergDataObjectInfo", object_info->getPath());

        return std::make_shared<IcebergBitmapPositionDeleteTransform>(
            header, iceberg_object_info, object_storage, format_settings, context_, format, compression_method, position_deletes_files);
    }

private:
    std::unique_ptr<ActionsDAG> filter_dag;
    ObjectStoragePtr object_storage;
    SingleThreadIcebergKeysIterator data_files_iterator;
    SingleThreadIcebergKeysIterator position_deletes_iterator;
    std::vector<Iceberg::ManifestFileEntry> position_deletes_files;
    BlockingMCSP<Iceberg::ManifestFileEntry> blocking_queue;
    BackgroundSchedulePool::TaskHolder producer_task;
    String format;
    String compression_method;
    IDataLakeMetadata::FileProgressCallback callback;
};
}


#endif
