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
    extern const Event FilesystemCacheEvictMicroseconds;
    extern const Event FilesystemCacheEvictedBytes;
    extern const Event FilesystemCacheEvictedFileSegments;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

LRUFileCachePriority::LRUFileCachePriority(size_t max_size_, size_t max_elements_, StatePtr state_)
    : IFileCachePriority(max_size_, max_elements_)
{
    if (state_)
        state = state_;
    else
        state = std::make_shared<State>();
}

IFileCachePriority::IteratorPtr LRUFileCachePriority::add( /// NOLINT
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const UserInfo &,
    const CachePriorityGuard::Lock & lock,
    bool)
{
    return std::make_shared<LRUIterator>(add(std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata), lock));
}

LRUFileCachePriority::LRUIterator LRUFileCachePriority::add(EntryPtr entry, const CachePriorityGuard::Lock & lock)
{
    if (entry->size == 0)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Adding zero size entries to LRU queue is not allowed "
            "(key: {}, offset: {})", entry->key, entry->offset);
    }

#ifndef NDEBUG
    for (const auto & queue_entry : queue)
    {
        /// entry.size == 0 means entry was invalidated.
        if (queue_entry->size != 0 && queue_entry->key == entry->key && queue_entry->offset == entry->offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to add duplicate queue entry to queue. "
                "(Key: {}, offset: {}, size: {})",
                entry->key, entry->offset, entry->size);
    }
#endif

    const auto & size_limit = getSizeLimit(lock);
    if (size_limit && state->current_size + entry->size > size_limit)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Not enough space to add {}:{} with size {}: current size: {}/{}",
            entry->key, entry->offset, entry->size, state->current_size, size_limit);
    }

    auto iterator = queue.insert(queue.end(), entry);

    updateSize(entry->size);
    updateElementsCount(1);

    LOG_TEST(
        log, "Added entry into LRU queue, key: {}, offset: {}, size: {}",
        entry->key, entry->offset, entry->size);

    return LRUIterator(this, iterator);
}

LRUFileCachePriority::LRUQueue::iterator LRUFileCachePriority::remove(LRUQueue::iterator it, const CachePriorityGuard::Lock &)
{
    /// If size is 0, entry is invalidated, current_elements_num was already updated.
    const auto & entry = **it;
    if (entry.size)
    {
        updateSize(-entry.size);
        updateElementsCount(-1);
    }

    LOG_TEST(
        log, "Removed entry from LRU queue, key: {}, offset: {}, size: {}",
        entry.key, entry.offset, entry.size);

    return queue.erase(it);
}

void LRUFileCachePriority::updateSize(int64_t size)
{
    state->current_size += size;
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size);
}

void LRUFileCachePriority::updateElementsCount(int64_t num)
{
    state->current_elements_num += num;
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheElements, num);
}

LRUFileCachePriority::LRUIterator::LRUIterator(
    LRUFileCachePriority * cache_priority_,
    LRUQueue::iterator iterator_)
    : cache_priority(cache_priority_)
    , iterator(iterator_)
{
}

LRUFileCachePriority::LRUIterator::LRUIterator(const LRUIterator & other)
{
    *this = other;
}

LRUFileCachePriority::LRUIterator & LRUFileCachePriority::LRUIterator::operator =(const LRUIterator & other)
{
    if (this == &other)
        return *this;

    cache_priority = other.cache_priority;
    iterator = other.iterator;
    return *this;
}

bool LRUFileCachePriority::LRUIterator::operator ==(const LRUIterator & other) const
{
    return cache_priority == other.cache_priority && iterator == other.iterator;
}

void LRUFileCachePriority::iterate(IterateFunc && func, const CachePriorityGuard::Lock & lock)
{
    for (auto it = queue.begin(); it != queue.end();)
    {
        const auto & entry = **it;
        auto locked_key = entry.key_metadata->tryLock();
        if (!locked_key || entry.size == 0)
        {
            it = remove(it, lock);
            continue;
        }

        auto metadata = locked_key->tryGetByOffset(entry.offset);
        if (!metadata)
        {
            it = remove(it, lock);
            continue;
        }

        if (metadata->size() != entry.size)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Mismatch of file segment size in file segment metadata "
                "and priority queue: {} != {} ({})",
                entry.size, metadata->size(), metadata->file_segment->getInfoForLog());
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

bool LRUFileCachePriority::canFit( /// NOLINT
    size_t size,
    const CachePriorityGuard::Lock & lock,
    IteratorPtr,
    bool) const
{
    return canFit(size, 0, 0, lock);
}

bool LRUFileCachePriority::canFit(
    size_t size,
    size_t released_size_assumption,
    size_t released_elements_assumption,
    const CachePriorityGuard::Lock &) const
{
    return (max_size == 0 || (state->current_size + size - released_size_assumption <= max_size))
        && (max_elements == 0 || state->current_elements_num + 1 - released_elements_assumption <= max_elements);
}

bool LRUFileCachePriority::collectCandidatesForEviction(
    size_t size,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    IFileCachePriority::IteratorPtr,
    FinalizeEvictionFunc &,
    const UserID &,
    const CachePriorityGuard::Lock & lock)
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

LRUFileCachePriority::LRUIterator LRUFileCachePriority::move(LRUIterator & it, LRUFileCachePriority & other, const CachePriorityGuard::Lock &)
{
    const auto & entry = *it.getEntry();
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
        if (queue_entry->size != 0 && queue_entry->key == entry.key && queue_entry->offset == entry.offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to add duplicate queue entry to queue. "
                "(Key: {}, offset: {}, size: {})",
                entry.key, entry.offset, entry.size);
    }
#endif

    queue.splice(queue.end(), other.queue, it.iterator);

    updateSize(entry.size);
    updateElementsCount(1);

    other.updateSize(-entry.size);
    other.updateElementsCount(-1);
    return LRUIterator(this, it.iterator);
}

IFileCachePriority::PriorityDumpPtr LRUFileCachePriority::dump(const CachePriorityGuard::Lock & lock)
{
    std::vector<FileSegmentInfo> res;
    iterate([&](LockedKey &, const FileSegmentMetadataPtr & segment_metadata)
    {
        res.emplace_back(FileSegment::getInfo(segment_metadata->file_segment));
        return IterationResult::CONTINUE;
    }, lock);
    return std::make_shared<LRUPriorityDump>(res);
}

bool LRUFileCachePriority::modifySizeLimits(
    size_t max_size_, size_t max_elements_, double /* size_ratio_ */, const CachePriorityGuard::Lock & lock)
{
    if (max_size == max_size_ && max_elements == max_elements_)
        return false; /// Nothing to change.

    auto check_limits_satisfied = [&]()
    {
        return (max_size_ == 0 || state->current_size <= max_size_)
            && (max_elements_ == 0 || state->current_elements_num <= max_elements_);
    };

    if (check_limits_satisfied())
    {
        max_size = max_size_;
        max_elements = max_elements_;
        return true;
    }

    auto iterate_func = [&](LockedKey & locked_key, const FileSegmentMetadataPtr & segment_metadata)
    {
        chassert(segment_metadata->file_segment->assertCorrectness());

        if (!segment_metadata->releasable())
            return IterationResult::CONTINUE;

        auto segment = segment_metadata->file_segment;
        locked_key.removeFileSegment(segment->offset(), segment->lock());

        ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedFileSegments);
        ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedBytes, segment->getDownloadedSize());
        return IterationResult::REMOVE_AND_CONTINUE;
    };

    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::FilesystemCacheEvictMicroseconds);
    iterate(
        [&](LockedKey & locked_key, const FileSegmentMetadataPtr & segment_metadata)
        { return check_limits_satisfied() ? IterationResult::BREAK : iterate_func(locked_key, segment_metadata); },
        lock);

    max_size = max_size_;
    max_elements = max_elements_;
    return true;
}

void LRUFileCachePriority::LRUIterator::remove(const CachePriorityGuard::Lock & lock)
{
    assertValid();
    cache_priority->remove(iterator, lock);
    iterator = LRUQueue::iterator{};
}

void LRUFileCachePriority::LRUIterator::invalidate()
{
    assertValid();

    const auto & entry = *iterator;
    LOG_TEST(
        cache_priority->log,
        "Invalidating entry in LRU queue. Key: {}, offset: {}, previous size: {}",
        entry->key, entry->offset, entry->size);

    cache_priority->updateSize(-entry->size);
    cache_priority->updateElementsCount(-1);
    entry->size = 0;
}

void LRUFileCachePriority::LRUIterator::updateSize(int64_t size)
{
    assertValid();

    const auto & entry = *iterator;
    LOG_TEST(
        cache_priority->log,
        "Update size with {} in LRU queue for key: {}, offset: {}, previous size: {}",
        size, entry->key, entry->offset, entry->size);

    cache_priority->updateSize(size);
    entry->size += size;
}

size_t LRUFileCachePriority::LRUIterator::increasePriority(const CachePriorityGuard::Lock &)
{
    assertValid();
    cache_priority->queue.splice(cache_priority->queue.end(), cache_priority->queue, iterator);
    return ++((*iterator)->hits);
}

void LRUFileCachePriority::LRUIterator::assertValid() const
{
    if (iterator == LRUQueue::iterator{})
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to use invalid iterator");
}

void LRUFileCachePriority::shuffle(const CachePriorityGuard::Lock &)
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
