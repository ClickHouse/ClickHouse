#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/copyData.h>

namespace DB
{

AsynchronousReaderPtr IObjectStorage::getThreadPoolReader()
{
    constexpr size_t pool_size = 50;
    constexpr size_t queue_size = 1000000;
    static AsynchronousReaderPtr reader = std::make_shared<ThreadPoolRemoteFSReader>(pool_size, queue_size);
    return reader;
}

ThreadPool & IObjectStorage::getThreadPoolWriter()
{
    constexpr size_t pool_size = 100;
    constexpr size_t queue_size = 1000000;
    static ThreadPool writer(pool_size, pool_size, queue_size);
    return writer;
}

std::string IObjectStorage::getCacheBasePath() const
{
    return cache ? cache->getBasePath() : "";
}

void IObjectStorage::removeFromCache(const std::string & path)
{
    if (cache)
    {
        auto key = cache->hash(path);
        cache->remove(key);
    }
}

void IObjectStorage::copyObjectToAnotherObjectStorage(const std::string & object_from, const std::string & object_to, IObjectStorage & object_storage_to, std::optional<ObjectAttributes> object_to_attributes) // NOLINT
{
    if (&object_storage_to == this)
        copyObject(object_from, object_to, object_to_attributes);

    auto in = readObject(object_from);
    auto out = object_storage_to.writeObject(object_to, WriteMode::Rewrite);
    copyData(*in, *out);
    out->finalize();
}

}
