#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/EvictionCandidates.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <Common/logger_useful.h>
#include <pcg-random/pcg_random.hpp>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSize;
    extern const Metric FilesystemCacheElements;
}

namespace ProfileEvents
{
    extern const Event FilesystemCacheEvictionSkippedFileSegments;
    extern const Event FilesystemCacheEvictionTries;
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
    const CacheGuard::Lock & lock)
{
    return add(Entry(key_metadata->key, offset, size, key_metadata), lock);
}

std::unique_ptr<LRUFileCachePriority::LRUIterator> LRUFileCachePriority::add(Entry && entry, const CacheGuard::Lock &)
{
    if (entry.size == 0)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Adding zero size entries to LRU queue is not allowed "
            "(key: {}, offset: {})", entry.key, entry.offset);
    }

#ifndef NDEBUG
    for (const auto & queue_entry : queue)
    {
        /// entry.size == 0 means entry was invalidated.
        if (queue_entry.size != 0 && queue_entry.key == entry.key && queue_entry.offset == entry.offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to add duplicate queue entry to queue. "
                "(Key: {}, offset: {}, size: {})",
                entry.key, entry.offset, entry.size);
    }
#endif

    const auto & size_limit = getSizeLimit();
    if (size_limit && current_size + entry.size > size_limit)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Not enough space to add {}:{} with size {}: current size: {}/{}",
            entry.key, entry.offset, entry.size, current_size, size_limit);
    }

    auto it = queue.insert(queue.end(), entry);

    updateSize(entry.size);
    updateElementsCount(1);

    LOG_TEST(
        log, "Added entry into LRU queue, key: {}, offset: {}, size: {}",
        entry.key, entry.offset, entry.size);

    return std::make_unique<LRUIterator>(this, it);
}

LRUFileCachePriority::LRUQueue::iterator LRUFileCachePriority::remove(LRUQueue::iterator it, const CacheGuard::Lock &)
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


LRUFileCachePriority::LRUIterator::LRUIterator(LRUFileCachePriority * cache_priority_, LRUQueue::iterator queue_iter_)
    : cache_priority(cache_priority_), queue_iter(queue_iter_)
{
}

void LRUFileCachePriority::iterate(IterateFunc && func, const CacheGuard::Lock & lock)
{
    for (auto it = queue.begin(); it != queue.end();)
    {
        auto locked_key = it->key_metadata->tryLock();
        if (!locked_key || it->size == 0)
        {
            it = remove(it, lock);
            continue;
        }

        auto metadata = locked_key->tryGetByOffset(it->offset);
        if (!metadata)
        {
            it = remove(it, lock);
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
                it = remove(it, lock);
                break;
            }
        }
    }
}

bool LRUFileCachePriority::canFit(size_t size, const CacheGuard::Lock & lock) const
{
    return canFit(size, 0, 0, lock);
}

bool LRUFileCachePriority::canFit(size_t size, size_t released_size_assumption, size_t released_elements_assumption, const CacheGuard::Lock &) const
{
    return (max_size == 0 || (current_size + size - released_size_assumption <= max_size))
        && (max_elements == 0 || current_elements_num + 1 - released_elements_assumption <= max_elements);
}

bool LRUFileCachePriority::collectCandidatesForEviction(
    size_t size,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    IFileCachePriority::Iterator,
    FinalizeEvictionFunc &,
    const CacheGuard::Lock & lock)
{
    if (canFit(size, lock))
        return true;

    ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictionTries);

    IterateFunc iterate_func = [&](LockedKey & locked_key, const FileSegmentMetadataPtr & segment_metadata)
    {
        const auto & file_segment = segment_metadata->file_segment;
        chassert(file_segment->assertCorrectness());

        if (segment_metadata->releasable())
        {
            res.add(locked_key, segment_metadata);
            stat.update(segment_metadata->size(), file_segment->getKind(), true);
        }
        else
        {
            stat.update(segment_metadata->size(), file_segment->getKind(), false);
            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictionSkippedFileSegments);
        }

        return IterationResult::CONTINUE;
    };

    auto can_fit = [&]
    {
        return canFit(size, stat.stat.releasable_size, stat.stat.releasable_count, lock);
    };

    iterate([&](LockedKey & locked_key, const FileSegmentMetadataPtr & segment_metadata)
    {
        return can_fit() ? IterationResult::BREAK : iterate_func(locked_key, segment_metadata);
    }, lock);

    return can_fit();
}

std::unique_ptr<LRUFileCachePriority::LRUIterator> LRUFileCachePriority::move(LRUIterator & it, LRUFileCachePriority & other, const CacheGuard::Lock &)
{
    const auto & entry = it.getEntry();
    if (entry.size == 0)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Adding zero size entries to LRU queue is not allowed "
            "(key: {}, offset: {})", entry.key, entry.offset);
    }
#ifndef NDEBUG
    for (const auto & queue_entry : queue)
    {
        /// entry.size == 0 means entry was invalidated.
        if (queue_entry.size != 0 && queue_entry.key == entry.key && queue_entry.offset == entry.offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to add duplicate queue entry to queue. "
                "(Key: {}, offset: {}, size: {})",
                entry.key, entry.offset, entry.size);
    }
#endif

    queue.splice(queue.end(), other.queue, it.queue_iter);

    updateSize(entry.size);
    updateElementsCount(1);

    other.updateSize(-entry.size);
    other.updateElementsCount(-1);
    return std::make_unique<LRUIterator>(this, it.queue_iter);
}

FileSegments LRUFileCachePriority::dump(const CacheGuard::Lock & lock)
{
    FileSegments res;
    iterate([&](LockedKey &, const FileSegmentMetadataPtr & segment_metadata)
    {
        res.push_back(FileSegment::getSnapshot(segment_metadata->file_segment));
        return IterationResult::CONTINUE;
    }, lock);
    return res;
}

void LRUFileCachePriority::LRUIterator::remove(const CacheGuard::Lock & lock)
{
    checkUsable();
    cache_priority->remove(queue_iter, lock);
    queue_iter = LRUQueue::iterator{};
}

void LRUFileCachePriority::LRUIterator::invalidate()
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

void LRUFileCachePriority::LRUIterator::updateSize(int64_t size)
{
    checkUsable();

    LOG_TEST(
        cache_priority->log,
        "Update size with {} in LRU queue for key: {}, offset: {}, previous size: {}",
        size, queue_iter->key, queue_iter->offset, queue_iter->size);

    cache_priority->updateSize(size);
    queue_iter->size += size;
}

size_t LRUFileCachePriority::LRUIterator::increasePriority(const CacheGuard::Lock &)
{
    checkUsable();
    cache_priority->queue.splice(cache_priority->queue.end(), cache_priority->queue, queue_iter);
    return ++queue_iter->hits;
}

void LRUFileCachePriority::LRUIterator::checkUsable() const
{
    if (queue_iter == LRUQueue::iterator{})
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to use invalid iterator");
}

void LRUFileCachePriority::shuffle(const CacheGuard::Lock &)
{
    std::vector<LRUQueue::iterator> its;
    its.reserve(queue.size());
    for (auto it = queue.begin(); it != queue.end(); ++it)
        its.push_back(it);
    pcg64 generator(randomSeed());
    std::shuffle(its.begin(), its.end(), generator);
    for (auto & it : its)
        queue.splice(queue.end(), queue, it);
}

}
