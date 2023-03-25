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
    const Key & key, size_t offset, size_t size, std::weak_ptr<KeyMetadata> key_metadata)
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

    const auto & size_limit = getSizeLimit();
    if (size_limit && current_size + size > size_limit)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Not enough space to add {}:{} with size {}: current size: {}/{}",
            key.toString(), offset, size, current_size, getSizeLimit());
    }

    current_size += size;

    auto iter = queue.insert(queue.end(), Entry(key, offset, size, key_metadata));

    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size);
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheElements);

    LOG_TEST(log, "Added entry into LRU queue, key: {}, offset: {}", key.toString(), offset);

    return std::make_shared<LRUFileCacheIterator>(this, iter);
}

void LRUFileCachePriority::removeAll()
{
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheSize, current_size);
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheElements, queue.size());

    LOG_TEST(log, "Removed all entries from LRU queue");

    queue.clear();
    current_size = 0;
}

void LRUFileCachePriority::pop()
{
    remove(queue.begin());
}

LRUFileCachePriority::LRUQueueIterator LRUFileCachePriority::remove(LRUQueueIterator it)
{
    current_size -= it->size;

    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheSize, it->size);
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheElements);

    LOG_TEST(log, "Removed entry from LRU queue, key: {}, offset: {}", it->key.toString(), it->offset);
    return queue.erase(it);
}

LRUFileCachePriority::LRUFileCacheIterator::LRUFileCacheIterator(
    LRUFileCachePriority * cache_priority_, LRUFileCachePriority::LRUQueueIterator queue_iter_)
    : cache_priority(cache_priority_), queue_iter(queue_iter_)
{
}

void LRUFileCachePriority::iterate(IterateFunc && func)
{
    for (auto it = queue.begin(); it != queue.end();)
    {
        auto result = func(*it);
        switch (result)
        {
            case IterationResult::BREAK:
            {
                return;
            }
            case IterationResult::CONTINUE:
            {
                ++it;
                break;
            }
            case IterationResult::REMOVE_AND_CONTINUE:
            {
                it = remove(it);
                break;
            }
        }
    }
}

LRUFileCachePriority::Iterator LRUFileCachePriority::LRUFileCacheIterator::remove()
{
    return std::make_shared<LRUFileCacheIterator>(cache_priority, cache_priority->remove(queue_iter));
}

void LRUFileCachePriority::LRUFileCacheIterator::incrementSize(ssize_t size)
{
    cache_priority->current_size += size;
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size);
    queue_iter->size += size;
}

size_t LRUFileCachePriority::LRUFileCacheIterator::use()
{
    cache_priority->queue.splice(cache_priority->queue.end(), cache_priority->queue, queue_iter);
    return ++queue_iter->hits;
}

};
