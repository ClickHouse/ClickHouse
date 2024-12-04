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

    void remove(const std::string & zookeeper_path, const StorageID & storage_id);

    std::unordered_map<std::string, FilesMetadataPtr> getAll();

private:
    using MetadataByPath = std::unordered_map<std::string, std::shared_ptr<ObjectStorageQueueMetadata>>;

    MetadataByPath metadata_by_path;
    std::mutex mutex;
    LoggerPtr log = getLogger("QueueMetadataFactory");
};

}
