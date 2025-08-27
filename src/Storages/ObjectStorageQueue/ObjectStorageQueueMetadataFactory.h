#pragma once
#include <Common/logger_useful.h>
#include <Interpreters/StorageID.h>
#include <boost/noncopyable.hpp>
#include <unordered_set>
#include <mutex>

namespace DB
{
class ObjectStorageQueueMetadata;
using ObjectStorageQueueMetadataPtr = std::unique_ptr<ObjectStorageQueueMetadata>;

/**
 * A class to keep track of all S3(Azure/etc)Queue storages.
 * Its main purpose is to be able to shutdown such tables
 * before shutting down all other tables,
 * to avoid "Table is shutting down" exceptions during processing.
 */
class ObjectStorageQueueFactory final : private boost::noncopyable
{
public:
    static ObjectStorageQueueFactory & instance();

    void registerTable(const StorageID & storage);

    void unregisterTable(const StorageID & storage, bool if_exists = false);

    void renameTable(const StorageID & from, const StorageID & to);

    void shutdown();

private:
    bool shutdown_called = false;
    std::mutex mutex;
    std::unordered_set<StorageID, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual> storages;
};

class ObjectStorageQueueMetadataFactory final : private boost::noncopyable
{
public:
    using FilesMetadataPtr = std::shared_ptr<ObjectStorageQueueMetadata>;

    static ObjectStorageQueueMetadataFactory & instance();

    FilesMetadataPtr getOrCreate(
        const std::string & zookeeper_path,
        ObjectStorageQueueMetadataPtr metadata,
        const StorageID & storage_id);

    void remove(const std::string & zookeeper_path, const StorageID & storage_id, bool remove_metadata_if_no_registered);

    std::unordered_map<std::string, FilesMetadataPtr> getAll();

private:
    struct MetadataWithRefCount
    {
        explicit MetadataWithRefCount(std::shared_ptr<ObjectStorageQueueMetadata> metadata_) : metadata(metadata_) {}
        std::shared_ptr<ObjectStorageQueueMetadata> metadata;
        std::unique_ptr<std::atomic<size_t>> ref_count = std::make_unique<std::atomic<size_t>>(0);
    };
    using MetadataByPath = std::unordered_map<std::string, MetadataWithRefCount>;

    MetadataByPath metadata_by_path;
    std::mutex mutex;
    LoggerPtr log = getLogger("QueueMetadataFactory");
};

}
