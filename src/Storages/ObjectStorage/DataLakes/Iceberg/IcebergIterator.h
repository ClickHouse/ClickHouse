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
        ActionsDAG * filter_dag_,
        ContextPtr local_context_,
        Iceberg::IcebergTableStateSnapshotPtr table_snapshot_,
        Iceberg::IcebergDataSnapshotPtr data_snapshot_,
        IDataLakeMetadata::FileProgressCallback callback_)
        : object_storage(object_storage_)
        , filter_dag(filter_dag_)
        , local_context(local_context_)
        , table_snapshot(table_snapshot_)
        , data_snapshot(data_snapshot_)
        , callback(std::move(callback_))
        , use_partition_pruning(
              [this]()
              {
                  if (!local_context && filter_dag)
                  {
                      throw DB::Exception(
                          DB::ErrorCodes::LOGICAL_ERROR,
                          "Context is required with non-empty filter_dag to implement partition pruning for Iceberg table");
                  }
                  return filter_dag && local_context->getSettingsRef()[Setting::use_iceberg_partition_pruning].value;
              }())
    {
    }

    IcebergDataObjectInfoPtr next();

private:
    ObjectStoragePtr object_storage;
    const ActionsDAG * filter_dag = nullptr;
    ContextPtr local_context;
    Iceberg::IcebergTableStateSnapshotPtr table_snapshot;
    Iceberg::IcebergDataSnapshotPtr data_snapshot;
    IcebergMetadataFilesCachePtr iceberg_metadata_cache;
    IDataLakeMetadata::FileProgressCallback callback;
    bool use_partition_pruning;


    // By Iceberg design it is difficult to avoid storing position deletes in memory.
    std::vector<Iceberg::ManifestFileEntry> position_deletes_files;
    size_t manifest_file_index = 0;
    size_t internal_data_index = 0;
    Iceberg::ManifestFilePtr current_manifest_file_content;
    Int32 previous_entry_schema = -1;
    std::optional<Iceberg::ManifestFilesPruner> current_pruner;


    StorageObjectStorageConfigurationWeakPtr configuration;
    std::shared_ptr<IcebergSchemaProcessor> schema_processor;

    std::optional<String> getRelevantManifestList(const Poco::JSON::Object::Ptr & metadata);
    Iceberg::ManifestFilePtr tryGetManifestFile(const String & filename) const;
    std::vector<Iceberg::ManifestFileEntry> getPositionDeletes() const;
};

template<typename T>
class BlockingMCSP {
public:
    explicit BlockingMCSP(size_t max_capacity_) : max_capacity(max_capacity_) {}

    void close() {
        std::unique_lock lock(mutex);
        closed = true;
        consumer_cv.notify_all();
    }

    void push(T value)
    {
        std::unique_lock lock(mutex);
        chassert(!closed, "Cannot push to closed BlockingMCSP");
        producer_cv.wait(lock, [this]() { return queue.size() < max_capacity; });
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
        if (closed && queue.empty()) {
            return std::nullopt;
        }
        bool was_full = queue.size() == max_capacity;
        auto value = std::move(queue.front());
        queue.pop();
        if (was_full) {
            producer_cv.notify_one();
        }
        return value;
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
    IcebergIterator(
        ObjectStoragePtr object_storage_,
        ActionsDAG * filter_dag_,
        ContextPtr local_context_,
        Iceberg::IcebergTableStateSnapshotPtr table_snapshot_,
        Iceberg::IcebergDataSnapshotPtr data_snapshot_,
        IDataLakeMetadata::FileProgressCallback callback_)
        : single_thread_iterator(
            object_storage_, filter_dag_, local_context_, table_snapshot_, data_snapshot_, std::move(callback_)),
            blocking_queue(100),
            producer_task(local_context_->getSchedulePool().createTask("IcebergMetaReaderThread", [this]{ 
                while (true)
                {
                    auto info = single_thread_iterator.next();
                    if (!info)
                        break;
                    blocking_queue.push(std::move(info));
                }
                blocking_queue.close();
             }))
    {
        producer_task->activateAndSchedule();
    }

    ObjectInfoPtr next(size_t) override
    {
        auto info = blocking_queue.pop();
        if (info)
        {
            return info.value();
        }
        return nullptr;
    }

    size_t estimatedKeysCount() override
    {
        return std::numeric_limits<size_t>::max();
    }

    ~IcebergIterator() override
    {
        producer_task->deactivate();
    }
private:
    SingleThreadIcebergKeysIterator single_thread_iterator;
    BlockingMCSP<IcebergDataObjectInfoPtr> blocking_queue;
    BackgroundSchedulePool::TaskHolder producer_task;

};
} // namespace DB


#endif
