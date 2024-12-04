#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadataFactory.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ObjectStorageQueueMetadataFactory & ObjectStorageQueueMetadataFactory::instance()
{
    static ObjectStorageQueueMetadataFactory ret;
    return ret;
}

ObjectStorageQueueMetadataFactory::FilesMetadataPtr ObjectStorageQueueMetadataFactory::getOrCreate(
    const std::string & zookeeper_path,
    ObjectStorageQueueMetadataPtr metadata,
    const StorageID & storage_id)
{
    std::lock_guard lock(mutex);
    auto it = metadata_by_path.find(zookeeper_path);
    if (it == metadata_by_path.end())
    {
        it = metadata_by_path.emplace(zookeeper_path, std::move(metadata)).first;
    }
    else
    {
        auto & metadata_from_table = metadata->getTableMetadata();
        auto & metadata_from_keeper = it->second->getTableMetadata();

        metadata_from_table.checkEquals(metadata_from_keeper);
    }

    it->second->registerIfNot(storage_id);
    return it->second;
}

void ObjectStorageQueueMetadataFactory::remove(const std::string & zookeeper_path, const StorageID & storage_id)
{
    std::lock_guard lock(mutex);
    auto it = metadata_by_path.find(zookeeper_path);

    if (it == metadata_by_path.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Metadata with zookeeper path {} does not exist", zookeeper_path);

    const size_t registry_size = it->second->unregister(storage_id);
    LOG_TRACE(log, "Remaining registry size: {}", registry_size);

    if (registry_size == 0)
    {
        try
        {
            auto zk_client = Context::getGlobalContextInstance()->getZooKeeper();
            zk_client->removeRecursive(it->first);
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }

        metadata_by_path.erase(it);
    }
}

std::unordered_map<std::string, ObjectStorageQueueMetadataFactory::FilesMetadataPtr> ObjectStorageQueueMetadataFactory::getAll()
{
    std::unordered_map<std::string, ObjectStorageQueueMetadataFactory::FilesMetadataPtr> result;
    for (const auto & [zk_path, metadata] : metadata_by_path)
        result.emplace(zk_path, metadata);
    return result;
}

}
