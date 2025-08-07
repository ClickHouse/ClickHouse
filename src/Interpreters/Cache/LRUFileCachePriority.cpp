#include <Interpreters/Cache/EvictionCandidates.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <pcg-random/pcg_random.hpp>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <Common/randomSeed.h>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSize;
    extern const Metric FilesystemCacheElements;
}

namespace ProfileEvents
{
    extern const Event FilesystemCacheEvictionSkippedFileSegments;
    extern const Event FilesystemCacheEvictionTries;
    extern const Event FilesystemCacheEvictionSkippedEvictingFileSegments;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

LRUFileCachePriority::LRUFileCachePriority(
    size_t max_size_,
    size_t max_elements_,
    const std::string & description_,
    StatePtr state_)
    : IFileCachePriority(max_size_, max_elements_)
    , description(description_)
    , log(getLogger("LRUFileCachePriority" + (description.empty() ? "" : "(" + description + ")")))
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
    const CachePriorityGuard::WriteLock & lock,
    bool)
{
    return std::make_shared<LRUIterator>(add(std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata), lock));
}

LRUFileCachePriority::LRUIterator LRUFileCachePriority::add(EntryPtr entry, const CachePriorityGuard::WriteLock & lock)
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
        if (queue_entry->size != 0
            && !queue_entry->isEvictingUnlocked()
            && queue_entry->key == entry->key && queue_entry->offset == entry->offset)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to add duplicate queue entry to queue: {}",
                entry->toString());
        }
    }
#endif

    if (!canFit(entry->size, 1, lock))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Not enough space to add a new entry {}. Current state: {}",
            entry->toString(), getStateInfoForLog(lock));
    }

    auto iterator = queue.insert(queue.end(), entry);

    updateSize(entry->size);
    updateElementsCount(1);

    LOG_TEST(
        log, "Added entry into LRU queue, key: {}, offset: {}, size: {}",
        entry->key, entry->offset, entry->size.load());

    return LRUIterator(this, iterator);
}

LRUFileCachePriority::LRUQueue::iterator
LRUFileCachePriority::remove(LRUQueue::iterator it, const CachePriorityGuard::WriteLock & lock)
{
    /// If size is 0, entry is invalidated, current_elements_num was already updated.
    auto & entry = **it;
    if (entry.size)
    {
        updateSize(-entry.size);
        updateElementsCount(-1);
    }

    entry.setRemoved(lock);

    LOG_TEST(
        log, "Removed entry from LRU queue, key: {}, offset: {}, size: {}",
        entry.key, entry.offset, entry.size.load());

    return queue.erase(it);
}

void LRUFileCachePriority::updateSize(int64_t size)
{
    chassert(size != 0);
    chassert(size > 0 || state->current_size >= size_t(-size));

    LOG_TEST(log, "Updating size with {}, current is {}",
             size, state->current_size.load());

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

void LRUFileCachePriority::iterate(
    IterateFunc func,
    FileCacheReserveStat & stat,
    const CachePriorityGuard::ReadLock & /*lock*/)
{
    LOG_TEST(log, "Queue size: {}", queue.size());
    for (auto it = queue.begin(); it != queue.end();)
    {
        const auto & entry = **it;

        if (entry.size == 0)
        {
            /// entry.size == 0 means that queue entry was invalidated,
            /// valid (active) queue entries always have size > 0,
            /// so we can safely remove it.
            stat.update(
                entry.size,
                FileSegmentKind::Unknown,
                FileCacheReserveStat::State::Invalidated,
                std::make_shared<LRUIterator>(this, it));
            ++it;
            continue;
        }

        /// Check Unlocked version of setEvicting before taking
        /// key_metadata lock as an optimization.
        if (entry.isEvictingUnlocked())
        {
            /// Skip queue entries which are in evicting state.
            /// We threat them the same way as deleted entries.
            ++it;
            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictionSkippedEvictingFileSegments);
            stat.update(entry.size, FileSegmentKind::Unknown, FileCacheReserveStat::State::Evicting);
            continue;
        }

        auto locked_key = entry.key_metadata->tryLock();
        if (!locked_key || entry.size == 0)
        {
            /// locked_key == nullptr means that the cache key of
            /// the file segment of this queue entry no longer exists.
            /// This is normal if the key was removed from metadata,
            /// while queue entries can be removed lazily (with delay).
            stat.update(
                entry.size,
                FileSegmentKind::Unknown,
                FileCacheReserveStat::State::Invalidated,
                nullptr);
            ++it;
            continue;
        }

        if (entry.isEvicting(*locked_key))
        {
            /// Skip queue entries which are in evicting state.
            /// We threat them the same way as deleted entries.
            stat.update(entry.size, FileSegmentKind::Unknown, FileCacheReserveStat::State::Evicting);
            ++it;
            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictionSkippedEvictingFileSegments);
            continue;
        }

        auto metadata = locked_key->tryGetByOffset(entry.offset);
        if (!metadata)
        {
            /// Same as explained in comment above, metadata == nullptr,
            /// if file segment was removed from cache metadata,
            /// but queue entry still exists because it is lazily removed.
            stat.update(
                entry.size,
                FileSegmentKind::Unknown,
                FileCacheReserveStat::State::Invalidated, nullptr);
            ++it;
            continue;
        }

        if (metadata->size() != entry.size)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Mismatch of file segment size in file segment metadata "
                "and priority queue: {} != {} ({})",
                entry.size.load(), metadata->size(), metadata->file_segment->getInfoForLog());
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
        }
    }
}

bool LRUFileCachePriority::canFit( /// NOLINT
    size_t size,
    size_t elements,
    const CachePriorityGuard::WriteLock & lock,
    IteratorPtr,
    bool) const
{
    return canFit(size, elements, 0, 0, lock);
}

bool LRUFileCachePriority::canFit(
    size_t size,
    size_t elements,
    size_t released_size_assumption,
    size_t released_elements_assumption,
    const CachePriorityGuard::WriteLock &,
    const size_t * max_size_,
    const size_t * max_elements_) const
{
    return (max_size == 0
            || (state->current_size + size - released_size_assumption <= (max_size_ ? *max_size_ : max_size.load())))
        && (max_elements == 0
            || state->current_elements_num + elements - released_elements_assumption <= (max_elements_ ? *max_elements_ : max_elements.load()));
}

IFileCachePriority::EvictionInfo LRUFileCachePriority::checkEvictionInfo(
    size_t size,
    size_t elements,
    const CachePriorityGuard::WriteLock & lock)
{
    EvictionInfo info;

    const size_t available_size = max_size.load() - state->current_size;
    if (available_size < size)
        info.size_to_evict = size - available_size;

    const size_t available_elements = max_elements.load() - state->current_elements_num;
    if (available_elements < elements)
        info.elements_to_evict = elements - available_elements;

    if ((info.size_to_evict && available_size) || (info.elements_to_evict && available_elements))
    {
        /// As eviction is done without a cache priority lock,
        /// then if some space was partially available and some needed
        /// to be freed via eviction, we need to make sure that this
        /// partially available space is still available
        /// after we finish with eviction for non-available space.
        /// So we create a space holder for the currently available part
        /// of the required space for the duration of eviction of the other
        /// currently non-available part of the space.
        info.hold_space = std::make_unique<IFileCachePriority::HoldSpace>(
            info.size_to_evict ? available_size : 0,
            info.elements_to_evict ? available_elements : 0,
            *this,
            lock);
    }
    return info;
}

bool LRUFileCachePriority::collectCandidatesForEviction(
    size_t size,
    size_t elements,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    IFileCachePriority::IteratorPtr /* reservee */,
    const UserID &,
    const CachePriorityGuard::ReadLock & lock)
{
    ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictionTries);

    iterate([&](LockedKey & locked_key, const FileSegmentMetadataPtr & segment_metadata)
    {
        if ((!size || stat.total_stat.releasable_size >= size) && (!elements || stat.total_stat.releasable_count >= elements))
            return IterationResult::BREAK;

        const auto & file_segment = segment_metadata->file_segment;
        chassert(file_segment->assertCorrectness());

        if (segment_metadata->releasable())
        {
            res.add(segment_metadata, locked_key);
            stat.update(
                segment_metadata->size(),
                file_segment->getKind(),
                FileCacheReserveStat::State::Releasable);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictionSkippedFileSegments);
            stat.update(
                segment_metadata->size(),
                file_segment->getKind(),
                FileCacheReserveStat::State::NonReleasable);
        }

        return IterationResult::CONTINUE;
    }, stat, lock);

    const bool success = (!size || stat.total_stat.releasable_size >= size)
        && (!elements || stat.total_stat.releasable_count >= elements);

    if (!success)
    {
        LOG_TEST(
            log, "Failed to collect eviction candidates "
            "(for size: {}, elements: {}, current size: {}, current elements: {}): {}",
            size, elements, getSize(lock), getElementsCount(lock), stat.total_stat.toString());
    }
    return success;
}

LRUFileCachePriority::LRUIterator LRUFileCachePriority::move(
    LRUIterator & it,
    LRUFileCachePriority & other,
    const CachePriorityGuard::WriteLock &)
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
                "Attempt to add duplicate queue entry to queue: {}",
                entry.toString());
    }
#endif

    queue.splice(queue.end(), other.queue, it.iterator);

    updateSize(entry.size);
    updateElementsCount(1);

    other.updateSize(-entry.size);
    other.updateElementsCount(-1);
    return LRUIterator(this, it.iterator);
}

IFileCachePriority::PriorityDumpPtr LRUFileCachePriority::dump(const CachePriorityGuard::ReadLock & lock)
{
    std::vector<FileSegmentInfo> res;
    FileCacheReserveStat stat{};
    iterate([&](LockedKey &, const FileSegmentMetadataPtr & segment_metadata)
    {
        res.emplace_back(FileSegment::getInfo(segment_metadata->file_segment));
        return IterationResult::CONTINUE;
    }, stat, lock);
    return std::make_shared<LRUPriorityDump>(res);
}

bool LRUFileCachePriority::modifySizeLimits(
    size_t max_size_, size_t max_elements_, double /* size_ratio_ */, const CachePriorityGuard::WriteLock &)
{
    if (max_size == max_size_ && max_elements == max_elements_)
        return false; /// Nothing to change.

    if (state->current_size > max_size_ || state->current_elements_num > max_elements_)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot modify size limits to {} in size and {} in elements: "
                        "not enough space freed. Current size: {}/{}, elements: {}/{} ({})",
                        max_size_, max_elements_, state->current_size.load(), max_size.load(),
                        state->current_elements_num.load(), max_elements.load(), description);
    }

    LOG_INFO(log, "Modifying size limits from {} to {} in size, "
             "from {} to {} in elements count",
             max_size.load(), max_size_, max_elements.load(), max_elements_);

    max_size = max_size_;
    max_elements = max_elements_;
    return true;
}

IFileCachePriority::EntryPtr LRUFileCachePriority::LRUIterator::getEntry() const
{
    assertValid();
    return *iterator;
}

void LRUFileCachePriority::LRUIterator::remove(const CachePriorityGuard::WriteLock & lock)
{
    if (iterator == LRUQueue::iterator{})
        return;
    //assertValid();
    cache_priority->remove(iterator, lock);
    iterator = LRUQueue::iterator{};
}

void LRUFileCachePriority::LRUIterator::invalidate()
{
    assertValid();

    const auto & entry = *iterator;

    chassert(entry->size != 0);
    cache_priority->updateSize(-entry->size);
    cache_priority->updateElementsCount(-1);

    LOG_TEST(cache_priority->log,
             "Invalidated entry in LRU queue {}: {}",
             entry->toString(), cache_priority->getApproxStateInfoForLog());

    entry->size = 0;
}

void LRUFileCachePriority::LRUIterator::incrementSize(size_t size, const CachePriorityGuard::WriteLock & lock)
{
    chassert(size);
    assertValid();

    const auto & entry = *iterator;

    if (!cache_priority->canFit(size, /* elements */0, lock))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot increment size by {} for entry {}. Current state: {}",
                        size, entry->toString(), cache_priority->getStateInfoForLog(lock));
    }

    LOG_TEST(
        cache_priority->log,
        "Incrementing size with {} in LRU queue for entry {}",
        size, entry->toString());

    cache_priority->updateSize(size);
    entry->size += size;

    cache_priority->check(lock);
}

void LRUFileCachePriority::LRUIterator::decrementSize(size_t size)
{
    assertValid();

    const auto & entry = *iterator;
    LOG_TEST(cache_priority->log,
             "Decrement size with {} in LRU queue entry {}",
             size, entry->toString());

    chassert(size);
    chassert(entry->size >= size);

    cache_priority->updateSize(-size);
    entry->size -= size;
}

size_t LRUFileCachePriority::LRUIterator::increasePriority(const CachePriorityGuard::WriteLock & lock)
{
    assertValid();
    cache_priority->queue.splice(cache_priority->queue.end(), cache_priority->queue, iterator);
    cache_priority->check(lock);
    return ++((*iterator)->hits);
}

void LRUFileCachePriority::LRUIterator::assertValid() const
{
    if (iterator == LRUQueue::iterator{})
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to use invalid iterator");
}

void LRUFileCachePriority::shuffle(const CachePriorityGuard::WriteLock &)
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

std::string LRUFileCachePriority::getStateInfoForLog(const CachePriorityGuard::WriteLock & lock) const
{
    return fmt::format("size: {}/{}, elements: {}/{} (description: {})",
                       getSize(lock), max_size.load(), getElementsCount(lock), max_elements.load(), description);
}

std::string LRUFileCachePriority::getApproxStateInfoForLog() const
{
    return fmt::format("size: {}/{}, elements: {}/{} (description: {})",
                       getSizeApprox(), max_size.load(), getElementsCountApprox(), max_elements.load(), description);
}

void LRUFileCachePriority::holdImpl(
    size_t size,
    size_t elements,
    const CachePriorityGuard::WriteLock & lock)
{
    chassert(size || elements);

    if (!canFit(size, elements, lock))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot take space {} in size and {} in elements. "
                        "Current state {}/{} in size, {}/{} in elements",
                        size, elements, state->current_size.load(), max_size.load(),
                        state->current_elements_num.load(), max_elements.load());
    }

    state->current_size += size;
    state->current_elements_num += elements;

    LOG_TEST(log, "Hold {} by size and {} by elements", size, elements);
}

void LRUFileCachePriority::releaseImpl(size_t size, size_t elements)
{
    chassert(size || elements);

    state->current_size -= size;
    state->current_elements_num -= elements;

    LOG_TEST(log, "Released {} by size and {} by elements", size, elements);
}

}
