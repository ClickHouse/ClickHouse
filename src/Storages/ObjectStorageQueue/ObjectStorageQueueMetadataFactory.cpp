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
        it->second.metadata->setMetadataRefCount(*it->second.ref_count);
    }
    else
    {
        auto & metadata_from_table = metadata->getTableMetadata();
        auto & metadata_from_keeper = it->second.metadata->getTableMetadata();

        metadata_from_table.checkEquals(metadata_from_keeper);
    }

    it->second.metadata->registerIfNot(storage_id, false);
    *it->second.ref_count += 1;
    return it->second.metadata;
}

void ObjectStorageQueueMetadataFactory::remove(const std::string & zookeeper_path, const StorageID & storage_id)
{
    std::lock_guard lock(mutex);
    auto it = metadata_by_path.find(zookeeper_path);

    if (it == metadata_by_path.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Metadata with zookeeper path {} does not exist", zookeeper_path);

    *it->second.ref_count -= 1;

    size_t registry_size;
    try
    {
        registry_size = it->second.metadata->unregister(storage_id, false);
        LOG_TRACE(log, "Remaining registry size: {}", registry_size);
    }
    catch (const zkutil::KeeperException & e)
    {
        if (!Coordination::isHardwareError(e.code))
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        /// Any non-zero value would do.
        registry_size = 1;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        /// Any non-zero value would do.
        registry_size = 1;
    }

    if (registry_size == 0)
    {
        auto zk_client = Context::getGlobalContextInstance()->getZooKeeper();
        auto code = zk_client->tryRemoveRecursive(it->first);
        if (code != Coordination::Error::ZOK
            && !Coordination::isHardwareError(code))
        {
            LOG_ERROR(log, "Unexpected error while removing metadata: {}, path: {}", code, it->first);
        }
    }

    if (!it->second.ref_count)
        metadata_by_path.erase(it);
}

std::unordered_map<std::string, ObjectStorageQueueMetadataFactory::FilesMetadataPtr> ObjectStorageQueueMetadataFactory::getAll()
{
    std::unordered_map<std::string, ObjectStorageQueueMetadataFactory::FilesMetadataPtr> result;
    for (const auto & [zk_path, metadata] : metadata_by_path)
        result.emplace(zk_path, metadata.metadata);
    return result;
}

}
