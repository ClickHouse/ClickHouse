#include <Storages/S3Queue/S3QueueMetadataFactory.h>

namespace DB
{

S3QueueMetadataFactory & S3QueueMetadataFactory::instance()
{
    static S3QueueMetadataFactory ret;
    return ret;
}

S3QueueMetadataFactory::MetadataPtr
S3QueueMetadataFactory::getOrCreate(const std::string & zookeeper_path, const S3QueueSettings & settings)
{
    std::lock_guard lock(mutex);
    auto it = metadata_by_path.find(zookeeper_path);
    if (it == metadata_by_path.end())
    {
        it = metadata_by_path.emplace(zookeeper_path, std::make_shared<S3QueueFilesMetadata>(fs::path(zookeeper_path), settings)).first;
    }
    else if (!it->second->checkSettings(settings))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Metadata with the same `s3queue_zookeeper_path` "
                        "was already created but with different settings");
    }
    return it->second;
}

}
