#include <Common/LRUFileCachePriority.h>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSize;
    extern const Metric FilesystemCacheElements;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IFileCachePriority::WriteIterator LRUFileCachePriority::add(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> &)
{
#ifndef NDEBUG
    for (const auto & entry : queue)
    {
        if (entry.key == key && entry.offset == offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to add duplicate queue entry to queue. (Key: {}, offset: {}, size: {})",
                entry.key.toString(),
                entry.offset,
                entry.size);
    }
#endif

    auto iter = queue.insert(queue.end(), FileCacheRecord(key, offset, size));
    cache_size += size;

    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size);
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheElements);

    return std::make_shared<LRUFileCacheIterator>(this, iter);
}

bool LRUFileCachePriority::contains(const Key & key, size_t offset, std::lock_guard<std::mutex> &)
{
    for (const auto & record : queue)
    {
        if (key == record.key && offset == record.offset)
            return true;
    }
    return false;
}

void LRUFileCachePriority::removeAll(std::lock_guard<std::mutex> &)
{
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheSize, cache_size);
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheElements, queue.size());

    queue.clear();
    cache_size = 0;
}

LRUFileCachePriority::LRUFileCacheIterator::LRUFileCacheIterator(
    LRUFileCachePriority * cache_priority_, LRUFileCachePriority::LRUQueueIterator queue_iter_)
    : cache_priority(cache_priority_), queue_iter(queue_iter_)
{
}

IFileCachePriority::ReadIterator LRUFileCachePriority::getLowestPriorityReadIterator(std::lock_guard<std::mutex> &)
{
    return std::make_unique<const LRUFileCacheIterator>(this, queue.begin());
}

IFileCachePriority::WriteIterator LRUFileCachePriority::getLowestPriorityWriteIterator(std::lock_guard<std::mutex> &)
{
    return std::make_shared<LRUFileCacheIterator>(this, queue.begin());
}

size_t LRUFileCachePriority::getElementsNum(std::lock_guard<std::mutex> &) const
{
    return queue.size();
}

void LRUFileCachePriority::LRUFileCacheIterator::removeAndGetNext(std::lock_guard<std::mutex> &)
{
    cache_priority->cache_size -= queue_iter->size;

    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheSize, queue_iter->size);
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheElements);

    queue_iter = cache_priority->queue.erase(queue_iter);
}

void LRUFileCachePriority::LRUFileCacheIterator::incrementSize(size_t size_increment, std::lock_guard<std::mutex> &)
{
    cache_priority->cache_size += size_increment;
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size_increment);
    queue_iter->size += size_increment;
}

void LRUFileCachePriority::LRUFileCacheIterator::use(std::lock_guard<std::mutex> &)
{
    queue_iter->hits++;
    cache_priority->queue.splice(cache_priority->queue.end(), cache_priority->queue, queue_iter);
}

};
