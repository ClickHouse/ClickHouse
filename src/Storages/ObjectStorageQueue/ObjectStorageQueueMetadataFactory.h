#pragma once
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <boost/noncopyable.hpp>

namespace DB
{

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
