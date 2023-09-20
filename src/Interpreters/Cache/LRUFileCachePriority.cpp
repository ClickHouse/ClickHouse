#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <Common/logger_useful.h>
#include <pcg-random/pcg_random.hpp>

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
    if (size == 0)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Adding zero size entries to LRU queue is not allowed "
            "(key: {}, offset: {})", key, offset);
    }

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

    updateSize(size);
    updateElementsCount(1);

    LOG_TEST(
        log, "Added entry into LRU queue, key: {}, offset: {}, size: {}",
        key, offset, size);

    return std::make_shared<LRUFileCacheIterator>(this, iter);
}

void LRUFileCachePriority::removeAll(const CacheGuard::Lock &)
{
    LOG_TEST(log, "Removed all entries from LRU queue");

    updateSize(-current_size);
    updateElementsCount(-current_elements_num);
    queue.clear();
}

void LRUFileCachePriority::pop(const CacheGuard::Lock &)
{
    remove(queue.begin());
}

LRUFileCachePriority::LRUQueueIterator LRUFileCachePriority::remove(LRUQueueIterator it)
{
    /// If size is 0, entry is invalidated, current_elements_num was already updated.
    if (it->size)
    {
        updateSize(-it->size);
        updateElementsCount(-1);
    }

    LOG_TEST(
        log, "Removed entry from LRU queue, key: {}, offset: {}, size: {}",
        it->key, it->offset, it->size);

    return queue.erase(it);
}

void LRUFileCachePriority::updateSize(int64_t size)
{
    current_size += size;
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size);
}

void LRUFileCachePriority::updateElementsCount(int64_t num)
{
    current_elements_num += num;
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheElements, num);
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

void LRUFileCachePriority::LRUFileCacheIterator::remove(const CacheGuard::Lock &)
{
    checkUsable();
    cache_priority->remove(queue_iter);
    queue_iter = LRUQueueIterator{};
}

void LRUFileCachePriority::LRUFileCacheIterator::invalidate()
{
    checkUsable();

    LOG_TEST(
        cache_priority->log,
        "Invalidating entry in LRU queue. Key: {}, offset: {}, previous size: {}",
        queue_iter->key, queue_iter->offset, queue_iter->size);

    cache_priority->updateSize(-queue_iter->size);
    cache_priority->updateElementsCount(-1);
    queue_iter->size = 0;
}

void LRUFileCachePriority::LRUFileCacheIterator::updateSize(int64_t size)
{
    checkUsable();

    LOG_TEST(
        cache_priority->log,
        "Update size with {} in LRU queue for key: {}, offset: {}, previous size: {}",
        size, queue_iter->key, queue_iter->offset, queue_iter->size);

    cache_priority->updateSize(size);
    queue_iter->size += size;
}

size_t LRUFileCachePriority::LRUFileCacheIterator::use(const CacheGuard::Lock &)
{
    checkUsable();
    cache_priority->queue.splice(cache_priority->queue.end(), cache_priority->queue, queue_iter);
    return ++queue_iter->hits;
}

void LRUFileCachePriority::LRUFileCacheIterator::checkUsable() const
{
    if (queue_iter == LRUQueueIterator{})
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to use invalid iterator");
}

void LRUFileCachePriority::shuffle(const CacheGuard::Lock &)
{
    std::vector<LRUQueueIterator> its;
    its.reserve(queue.size());
    for (auto it = queue.begin(); it != queue.end(); ++it)
        its.push_back(it);
    pcg64 generator(randomSeed());
    std::shuffle(its.begin(), its.end(), generator);
    for (auto & it : its)
        queue.splice(queue.end(), queue, it);
}

}
