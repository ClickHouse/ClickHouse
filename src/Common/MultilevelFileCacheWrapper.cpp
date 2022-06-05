#include "MultilevelFileCacheWrapper.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

MultiplelevelFileCacheWarpper::MultiplelevelFileCacheWarpper(
    const String & cache_base_path_, const FileCacheSettings & cache_settings_, FileCachePtr cache_)
    : IFileCache(cache_base_path_, cache_settings_), cache(cache_)
{
}

void MultiplelevelFileCacheWarpper::initialize()
{
    cache->initialize();
}

void MultiplelevelFileCacheWarpper::remove(const Key & key)
{
    cache->remove(key);
}

std::vector<String> MultiplelevelFileCacheWarpper::tryGetCachePaths(const Key & key)
{
    return cache->tryGetCachePaths(key);
}

FileSegmentsHolder MultiplelevelFileCacheWarpper::getOrSet(const Key & key, size_t offset, size_t size)
{
    return cache->getOrSet(key, offset, size);
}

FileSegmentsHolder MultiplelevelFileCacheWarpper::get(const Key & key, size_t offset, size_t size)
{
    return cache->get(key, offset, size);
}

FileSegmentsHolder MultiplelevelFileCacheWarpper::setDownloading(const Key & key, size_t offset, size_t size)
{
    return cache->setDownloading(key, offset, size);
}

FileSegments MultiplelevelFileCacheWarpper::getSnapshot() const
{
    return cache->getSnapshot();
}

String MultiplelevelFileCacheWarpper::dumpStructure(const Key & key)
{
    return cache->dumpStructure(key);
}

size_t MultiplelevelFileCacheWarpper::getUsedCacheSize() const
{
    return cache->getUsedCacheSize();
}

size_t MultiplelevelFileCacheWarpper::getFileSegmentsNum() const
{
    return cache->getFileSegmentsNum();
}

bool MultiplelevelFileCacheWarpper::tryReserve(const Key &, size_t, size_t, std::lock_guard<std::mutex> &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "tryReserve not implemented");
}

void MultiplelevelFileCacheWarpper::remove(Key, size_t, std::lock_guard<std::mutex> &, std::lock_guard<std::mutex> &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "remove");
}

bool MultiplelevelFileCacheWarpper::isLastFileSegmentHolder(
    const Key &, size_t, std::lock_guard<std::mutex> &, std::lock_guard<std::mutex> &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "isLastFileSegmentHolder");
}

void MultiplelevelFileCacheWarpper::reduceSizeToDownloaded(
    const Key &, size_t, std::lock_guard<std::mutex> &, std::lock_guard<std::mutex> &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "reduceSizeToDownloaded");
}

};
