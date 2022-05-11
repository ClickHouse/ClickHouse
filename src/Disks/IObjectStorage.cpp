#include <Disks/IObjectStorage.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>

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

}
