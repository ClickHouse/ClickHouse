#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <Common/logger_useful.h>

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
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const CacheGuard::Lock &)
{
    const auto & key = key_metadata->key;
#ifndef NDEBUG
    for (const auto & entry : queue)
    {
        /// entry.size == 0 means entry was invalidated.
        if (entry.size != 0 && entry.key == key && entry.offset == offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to add duplicate queue entry to queue. "
                "(Key: {}, offset: {}, size: {})",
                entry.key, entry.offset, entry.size);
    }
#endif

    const auto & size_limit = getSizeLimit();
    if (size_limit && current_size + size > size_limit)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Not enough space to add {}:{} with size {}: current size: {}/{}",
            key, offset, size, current_size, size_limit);
    }

    auto iter = queue.insert(queue.end(), Entry(key, offset, size, key_metadata));
    current_size += size;

    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size);
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheElements);

    LOG_TEST(
        log, "Added entry into LRU queue, key: {}, offset: {}, size: {}",
        key, offset, size);

    return std::make_shared<LRUFileCacheIterator>(this, iter);
}

void LRUFileCachePriority::removeAll(const CacheGuard::Lock &)
{
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheSize, current_size);
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheElements, queue.size());

    LOG_TEST(log, "Removed all entries from LRU queue");

    queue.clear();
    current_size = 0;
}

void LRUFileCachePriority::pop(const CacheGuard::Lock &)
{
    remove(queue.begin());
}

LRUFileCachePriority::LRUQueueIterator LRUFileCachePriority::remove(LRUQueueIterator it)
{
    current_size -= it->size;

    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheSize, it->size);
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheElements);

    LOG_TEST(
        log, "Removed entry from LRU queue, key: {}, offset: {}, size: {}",
        it->key, it->offset, it->size);

    return queue.erase(it);
}

LRUFileCachePriority::LRUFileCacheIterator::LRUFileCacheIterator(
    LRUFileCachePriority * cache_priority_,
    LRUFileCachePriority::LRUQueueIterator queue_iter_)
    : cache_priority(cache_priority_)
    , queue_iter(queue_iter_)
{
}

void LRUFileCachePriority::iterate(IterateFunc && func, const CacheGuard::Lock &)
{
    for (auto it = queue.begin(); it != queue.end();)
    {
        auto locked_key = it->key_metadata->tryLock();
        if (!locked_key || it->size == 0)
        {
            it = remove(it);
            continue;
        }

        auto metadata = locked_key->tryGetByOffset(it->offset);
        if (!metadata)
        {
            it = remove(it);
            continue;
        }

        if (metadata->size() != it->size)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Mismatch of file segment size in file segment metadata "
                "and priority queue: {} != {} ({})",
                it->size, metadata->size(), metadata->file_segment->getInfoForLog());
        }

        auto result = func(*locked_key, metadata);
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

LRUFileCachePriority::Iterator
LRUFileCachePriority::LRUFileCacheIterator::remove(const CacheGuard::Lock &)
{
    return std::make_shared<LRUFileCacheIterator>(
        cache_priority, cache_priority->remove(queue_iter));
}

void LRUFileCachePriority::LRUFileCacheIterator::annul()
{
    updateSize(-queue_iter->size);
    chassert(queue_iter->size == 0);
}

void LRUFileCachePriority::LRUFileCacheIterator::updateSize(int64_t size)
{
    LOG_TEST(
        cache_priority->log,
        "Update size with {} in LRU queue for key: {}, offset: {}, previous size: {}",
        size, queue_iter->key, queue_iter->offset, queue_iter->size);

    cache_priority->current_size += size;
    queue_iter->size += size;

    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size);

    chassert(cache_priority->current_size >= 0);
    chassert(queue_iter->size >= 0);
}

size_t LRUFileCachePriority::LRUFileCacheIterator::use(const CacheGuard::Lock &)
{
    cache_priority->queue.splice(cache_priority->queue.end(), cache_priority->queue, queue_iter);
    return ++queue_iter->hits;
}

}
