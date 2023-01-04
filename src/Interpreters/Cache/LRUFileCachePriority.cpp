#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache.h>
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

IFileCachePriority::Iterator LRUFileCachePriority::add(
    const Key & key,
    size_t offset,
    size_t size,
    KeyTransactionCreatorPtr key_transaction_creator,
    const CachePriorityQueueGuard::Lock &)
{
#ifndef NDEBUG
    for (const auto & entry : queue)
    {
        if (entry.key == key && entry.offset == offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to add duplicate queue entry to queue. (Key: {}, offset: {}, size: {})",
                entry.key.toString(), entry.offset, entry.size);
    }
#endif

    auto iter = queue.insert(queue.end(), FileCacheRecord(key, offset, size, std::move(key_transaction_creator)));
    cache_size += size;

    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size);
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheElements);

    LOG_TRACE(log, "Added entry into LRU queue, key: {}, offset: {}", key.toString(), offset);

    return std::make_shared<LRUFileCacheIterator>(this, iter);
}

KeyTransactionPtr LRUFileCachePriority::LRUFileCacheIterator::createKeyTransaction(const CachePriorityQueueGuard::Lock &)
{
    return queue_iter->key_transaction_creator->create();
}

bool LRUFileCachePriority::contains(const Key & key, size_t offset, const CachePriorityQueueGuard::Lock &)
{
    for (const auto & record : queue)
    {
        if (key == record.key && offset == record.offset)
            return true;
    }
    return false;
}

void LRUFileCachePriority::removeAll(const CachePriorityQueueGuard::Lock &)
{
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheSize, cache_size);
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheElements, queue.size());

    LOG_TRACE(log, "Removed all entries from LRU queue");

    queue.clear();
    cache_size = 0;
}

// LRUFileCachePriority::KeyAndOffset LRUFileCachePriority::pop(const CachePriorityQueueGuard::Lock & lock)
// {
//     auto remove_it = getLowestPriorityIterator(lock);
//     KeyAndOffset result(remove_it->key(), remove_it->offset());
//     remove_it->removeAndGetNext(lock);
//     return result;
// }

LRUFileCachePriority::LRUFileCacheIterator::LRUFileCacheIterator(
    LRUFileCachePriority * cache_priority_, LRUFileCachePriority::LRUQueueIterator queue_iter_)
    : cache_priority(cache_priority_), queue_iter(queue_iter_)
{
}

IFileCachePriority::Iterator LRUFileCachePriority::getLowestPriorityIterator(const CachePriorityQueueGuard::Lock &)
{
    return std::make_shared<LRUFileCacheIterator>(this, queue.begin());
}

size_t LRUFileCachePriority::getElementsNum(const CachePriorityQueueGuard::Lock &) const
{
    return queue.size();
}

LRUFileCachePriority::Iterator LRUFileCachePriority::LRUFileCacheIterator::remove(const CachePriorityQueueGuard::Lock &)
{
    cache_priority->cache_size -= queue_iter->size;

    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheSize, queue_iter->size);
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheElements);

    LOG_TRACE(cache_priority->log, "Removed entry from LRU queue, key: {}, offset: {}", queue_iter->key.toString(), queue_iter->offset);

    auto next = std::make_shared<LRUFileCacheIterator>(cache_priority, cache_priority->queue.erase(queue_iter));
    queue_iter = cache_priority->queue.end();
    return next;
}

void LRUFileCachePriority::LRUFileCacheIterator::incrementSize(ssize_t size_increment, const CachePriorityQueueGuard::Lock &)
{
    cache_priority->cache_size += size_increment;
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size_increment);
    queue_iter->size += size_increment;
}

size_t LRUFileCachePriority::LRUFileCacheIterator::use(const CachePriorityQueueGuard::Lock &)
{
    cache_priority->queue.splice(cache_priority->queue.end(), cache_priority->queue, queue_iter);
    return ++queue_iter->hits;
}

};
