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

ObjectStorageQueueMetadataFactory::FilesMetadataPtr
ObjectStorageQueueMetadataFactory::getOrCreate(const std::string & zookeeper_path, const ObjectStorageQueueSettings & settings)
{
    std::lock_guard lock(mutex);
    auto it = metadata_by_path.find(zookeeper_path);
    if (it == metadata_by_path.end())
    {
        auto files_metadata = std::make_shared<ObjectStorageQueueMetadata>(zookeeper_path, settings);
        it = metadata_by_path.emplace(zookeeper_path, std::move(files_metadata)).first;
    }
    else
    {
        it->second.metadata->checkSettings(settings);
        it->second.ref_count += 1;
    }
    return it->second.metadata;
}

void ObjectStorageQueueMetadataFactory::remove(const std::string & zookeeper_path)
{
    std::lock_guard lock(mutex);
    auto it = metadata_by_path.find(zookeeper_path);

    if (it == metadata_by_path.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Metadata with zookeeper path {} does not exist", zookeeper_path);

    chassert(it->second.ref_count > 0);
    if (--it->second.ref_count == 0)
    {
        try
        {
            auto zk_client = Context::getGlobalContextInstance()->getZooKeeper();
            zk_client->tryRemove(it->first);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        metadata_by_path.erase(it);
    }
}

std::unordered_map<std::string, ObjectStorageQueueMetadataFactory::FilesMetadataPtr> ObjectStorageQueueMetadataFactory::getAll()
{
    std::unordered_map<std::string, ObjectStorageQueueMetadataFactory::FilesMetadataPtr> result;
    for (const auto & [zk_path, metadata_and_ref_count] : metadata_by_path)
        result.emplace(zk_path, metadata_and_ref_count.metadata);
    return result;
}

}
