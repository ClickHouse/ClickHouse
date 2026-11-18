#include <Interpreters/FileCache/EvictionCandidates.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/LRUFileCachePriority.h>
#include <pcg-random/pcg_random.hpp>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/logger_useful.h>
#include <Common/randomSeed.h>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSize;
    extern const Metric FilesystemCacheElements;
    extern const Metric FilesystemCachePriorityQueueElements;
    extern const Metric FilesystemCacheInvalidatedElements;
}

namespace ProfileEvents
{
    extern const Event FilesystemCacheEvictionSkippedFileSegments;
    extern const Event FilesystemCacheEvictionTries;
    extern const Event FilesystemCacheEvictionSkippedEvictingFileSegments;
    extern const Event FilesystemCacheEvictionSkippedMovingFileSegments;
    extern const Event FilesystemCacheEvictionReusedIterator;
    extern const Event FilesystemCacheBackgroundRemovedInvalidatedEntries;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void LRUFileCachePriority::State::add(uint64_t size_, uint64_t elements_, const CacheStateGuard::Lock &)
{
    chassert(size_ || elements_);

    LOG_TEST(log, "Updating size with {}, current is {}", size_, size.load(std::memory_order_relaxed));

    if (size_)
    {
        size.fetch_add(size_, std::memory_order_relaxed);
        CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size_);
    }

    if (elements_)
    {
        elements_num.fetch_add(elements_, std::memory_order_relaxed);
        CurrentMetrics::add(CurrentMetrics::FilesystemCacheElements, elements_);
    }
}

void LRUFileCachePriority::State::sub(uint64_t size_, uint64_t elements_)
{
    chassert(size_ || elements_);

    if (size_)
    {
        chassert(size >= size_);
        size -= size_;
        CurrentMetrics::sub(CurrentMetrics::FilesystemCacheSize, size_);
    }

    if (elements_)
    {
        chassert(elements_num >= elements_);
        elements_num -= elements_;
        CurrentMetrics::sub(CurrentMetrics::FilesystemCacheElements, elements_);
    }
}

LRUFileCachePriority::LRUFileCachePriority(
    QueueType queue_type_,
    size_t max_size_,
    size_t max_elements_,
    const std::string & description_,
    StatePtr state_)
    : IFileCachePriority(queue_type_, max_size_, max_elements_)
    , description(description_)
    , log(getLogger("LRUFileCachePriority" + (description.empty() ? "" : "(" + description + ")")))
    , reserve_eviction_pos(queue.end())
    , background_eviction_pos(queue.end())
    , queue_id(randomSeed())
{
    if (state_)
        state = state_;
    else
        state = std::make_shared<State>(log);
}

LRUFileCachePriority::~LRUFileCachePriority()
{
    /// The queue may still hold `Invalidated` entries pending removal by the proactive
    /// cleanup when the priority is destroyed: e.g. when an emptied per-user overcommit
    /// priority is pruned by `CacheUsagePerUser::snapshot`, or when a whole cache is
    /// dropped. Such entries never go through `remove`, so compensate the metrics here.
    /// The destructor runs with exclusive ownership of the queue, no locks are needed.
    if (getQueueType() != QueueType::Main || queue.empty())
        return;

    size_t invalidated = 0;
    for (const auto & entry : queue)
    {
        if (entry->getState() == Entry::State::Invalidated)
            ++invalidated;
    }

    if (invalidated)
        CurrentMetrics::sub(CurrentMetrics::FilesystemCacheInvalidatedElements, invalidated);
    CurrentMetrics::sub(CurrentMetrics::FilesystemCachePriorityQueueElements, queue.size());
}

IFileCachePriority::IteratorPtr LRUFileCachePriority::add( /// NOLINT
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const CachePriorityGuard::WriteLock & lock,
    const CacheStateGuard::Lock * state_lock,
    bool)
{
    return std::make_shared<LRUIterator>(add(
        std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata),
        lock,
        state_lock));
}

LRUFileCachePriority::LRUIterator LRUFileCachePriority::add(
    EntryPtr entry,
    const CachePriorityGuard::WriteLock &,
    const CacheStateGuard::Lock * state_lock)
{
    if (entry->size && !state_lock)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Adding non-zero size entry without state lock "
            "(key: {}, offset: {})", entry->key, entry->offset);
    }

#ifndef NDEBUG
    for (const auto & queue_entry : queue)
    {
        if (queue_entry->getState() == Entry::State::Active
            && queue_entry->key == entry->key && queue_entry->offset == entry->offset)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to add duplicate queue entry to queue: {}",
                entry->toString());
        }
    }
#endif

    if (entry->size && !canFit(entry->size, /* elements */1, *state_lock))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Not enough space to add a new entry {}. Current state: {}",
            entry->toString(), getStateInfoForLog(*state_lock));
    }

    auto iterator = queue.insert(queue.end(), entry);
    if (getQueueType() == QueueType::Main)
        CurrentMetrics::add(CurrentMetrics::FilesystemCachePriorityQueueElements);

    if (entry->size)
        state->add(entry->size, /* elements */1, *state_lock);

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
        state->sub(entry.size, /* elements */1);

    const bool was_invalidated = entry.getState() == Entry::State::Invalidated;
    entry.setRemoved(lock);

    LOG_TEST(
        log, "Removed entry from LRU queue, key: {}, offset: {}, size: {}",
        entry.key, entry.offset, entry.size.load());

    moveEvictionPosIfEqual(it, lock);

    if (getQueueType() == QueueType::Main)
    {
        if (was_invalidated)
            CurrentMetrics::sub(CurrentMetrics::FilesystemCacheInvalidatedElements);
        CurrentMetrics::sub(CurrentMetrics::FilesystemCachePriorityQueueElements);
    }

    return queue.erase(it);
}

void LRUFileCachePriority::addInvalidatedRef(std::weak_ptr<Entry> entry, LRUQueue::iterator it) noexcept
{
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);

    size_t prev = 0;
    {
        std::lock_guard lock(invalidated_mutex);
        invalidated_refs.push_back({std::move(entry), it});
        prev = invalidated_count.fetch_add(1, std::memory_order_relaxed);
    }

    /// `invalidate_notifier` is set once on startup and never changed, so it needs no lock.
    /// Notify outside `invalidated_mutex` to avoid calling into the schedule pool under it.
    const size_t threshold = invalidated_threshold.load(std::memory_order_relaxed);
    if (invalidate_notifier && threshold && prev + 1 == threshold)
        invalidate_notifier();
}

LRUFileCachePriority::LRUIterator::LRUIterator(
    LRUFileCachePriority * cache_priority_,
    LRUQueue::iterator iterator_)
    : cache_priority(cache_priority_)
    , iterator(iterator_)
    , entry(*iterator)
{
    assertValid();
}

LRUFileCachePriority::LRUIterator::LRUIterator(const LRUIterator & other)
{
    *this = other;
}

LRUFileCachePriority::LRUIterator &
LRUFileCachePriority::LRUIterator::operator =(const LRUIterator & other)
{
    if (this == &other)
        return *this;

    cache_priority = other.cache_priority;
    iterator = other.iterator;
    entry = other.entry;
    return *this;
}

bool LRUFileCachePriority::LRUIterator::operator ==(const LRUIterator & other) const
{
    return cache_priority == other.cache_priority && iterator == other.iterator;
}

void LRUFileCachePriority::iterate(
    IterateFunc func,
    FileCacheReserveStat & stat,
    const CachePriorityGuard::ReadLock & lock)
{
    InvalidatedEntriesInfos invalidated_entries;
    iterateImpl(queue.begin(), func, stat, invalidated_entries, lock);
}

size_t LRUFileCachePriority::removeInvalidatedEntries(size_t max_batch, CachePriorityGuard & cache_guard)
{
    if (invalidated_count.load(std::memory_order_relaxed) == 0)
        return 0;

    /// The write lock is acquired lazily, only when a live `Invalidated` ref is found,
    /// and is then held for the rest of the batch. Refs whose entry was already removed
    /// elsewhere (e.g. by the opportunistic `removeEntries` during eviction) can be
    /// discarded without it: entries are registered here only in `Invalidated` state and
    /// the only transition out of it is the terminal `Removed`, set under the write lock
    /// in `remove`. So an expired or non-`Invalidated` ref can never become removable
    /// again and is skipped without the lock and without touching its stale iterator.
    std::optional<CachePriorityGuard::WriteLock> lock;

    size_t removed = 0;
    while (removed < max_batch)
    {
        InvalidatedRef ref;
        {
            std::lock_guard cleanup_lock(invalidated_mutex);
            if (invalidated_refs.empty())
                break;
            ref = std::move(invalidated_refs.front());
            invalidated_refs.pop_front();
        }

        try
        {
            if (auto entry = ref.entry.lock(); entry && entry->getState() == Entry::State::Invalidated)
            {
                if (!lock)
                    lock.emplace(cache_guard.writeLock());

                /// The entry could have been removed while the lock was being acquired.
                if (entry->getState() == Entry::State::Invalidated)
                {
                    remove(ref.iterator, *lock);
                    ProfileEvents::increment(ProfileEvents::FilesystemCacheBackgroundRemovedInvalidatedEntries);
                }
            }
        }
        catch (...)
        {
            /// Put the ref back so it is retried later. Must not throw and lose it:
            /// suppress `MEMORY_LIMIT_EXCEEDED` from the `push_front` allocation.
            LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
            std::lock_guard cleanup_lock(invalidated_mutex);
            invalidated_refs.push_front(std::move(ref));
            throw;
        }

        invalidated_count.fetch_sub(1, std::memory_order_relaxed);
        ++removed;
    }
    return removed;
}

LRUFileCachePriority::LRUQueue::iterator
LRUFileCachePriority::iterateImpl(
    LRUQueue::iterator start_pos,
    IterateFunc func,
    FileCacheReserveStat & stat,
    InvalidatedEntriesInfos & invalidated_entries,
    const CachePriorityGuard::ReadLock &)
{
    const size_t max_elements_to_iterate = queue.size();
    auto it = start_pos;

    for (size_t iterated_elements = 0; iterated_elements < max_elements_to_iterate; ++iterated_elements)
    {
        if (it == queue.end())
            it = queue.begin();

        const auto & entry = **it;

        //LOG_TEST(log, "Entry: {}", entry.toString());

        auto is_evictable_state = [&]() -> bool
        {
            switch (entry.getState())
            {
                case Entry::State::Active:
                {
                    /// A newly added entry may have size 0 before the first
                    /// space reservation completes. It is not yet evictable.
                    return entry.size > 0;
                }
                case Entry::State::PreActive:
                {
                    /// Entry is being moved between SLRU queues. Size may already be
                    /// non-zero, but the entry is not evictable until `SLRUIterator::setIterator`
                    /// transitions it to Active atomically with the iterator pointer update.
                    /// For SLRU transitions `size > 0` alone is not a sufficient guard, because
                    /// the SLRUIterator might still point to the old entry.
                    return false;
                }
                case Entry::State::Invalidated:
                {
                    stat.update(entry.size, FileSegmentKind::Regular, FileCacheReserveStat::State::Invalidated);
                    invalidated_entries.emplace_back(*it, std::make_shared<LRUIterator>(this, it));
                    return false;
                }
                case Entry::State::Evicting:
                {
                    /// Skip queue entries which are in evicting state.
                    /// We threat them the same way as deleted entries.
                    ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictionSkippedEvictingFileSegments);
                    stat.update(entry.size, FileSegmentKind::Regular, FileCacheReserveStat::State::Evicting);
                    return false;
                }
                case Entry::State::Moving:
                {
                    ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictionSkippedMovingFileSegments);
                    stat.update(entry.size, FileSegmentKind::Regular, FileCacheReserveStat::State::Moving);
                    return false;
                }
                case Entry::State::Removed:
                {
                    /// As we iterate under priority read lock and removed flag is
                    /// set under priority write lock right before it is removed from queue,
                    /// then the entry must have been removed from queue
                    /// before we acquired read lock.
                    chassert(false, "Entry should have been removed from queue: " + entry.toString());
                    return false;
                }
            }
        };

        /// Check state without locked key as an optimization.
        if (!is_evictable_state())
        {
            ++it;
            continue;
        }

        auto key_metadata = entry.key_metadata.lock();
        auto locked_key = key_metadata ? key_metadata->tryLock() : nullptr;
        if (!locked_key)
        {
            /// locked_key == nullptr means that the cache key of
            /// the file segment of this queue entry no longer exists.
            /// This is normal if the key was removed from metadata,
            /// while queue entries can be removed lazily (with delay).
            stat.update(entry.size, FileSegmentKind::Regular, FileCacheReserveStat::State::Invalidated);
            ++it;
            continue;
        }

        /// Reread entry state under locked key.
        if (!is_evictable_state())
        {
            ++it;
            continue;
        }

        auto metadata = locked_key->tryGetByOffset(entry.offset);
        if (!metadata)
        {
            stat.update(entry.size, FileSegmentKind::Regular, FileCacheReserveStat::State::Invalidated);
            ++it;
            /// We should have quit earlier in is_evictable_state under locked key.
            chassert(false);
            continue;
        }

        auto result = func(*locked_key, metadata);
        switch (result)
        {
            case IterationResult::BREAK:
            {
                return it;
            }
            case IterationResult::CONTINUE:
            {
                ++it;
                break;
            }
        }
    }
    return queue.end();
}

bool LRUFileCachePriority::canFit( /// NOLINT
    size_t size,
    size_t elements,
    const CacheStateGuard::Lock & lock,
    IteratorPtr,
    const OriginInfo &,
    bool) const
{
    return canFit(size, elements, 0, 0, lock);
}

bool LRUFileCachePriority::canFit(
    size_t size,
    size_t elements,
    size_t released_size_assumption,
    size_t released_elements_assumption,
    const CacheStateGuard::Lock & lock,
    const size_t * max_size_,
    const size_t * max_elements_) const
{
    const size_t current_size = state->getSize(lock);
    const size_t current_elements_num = state->getElementsCount(lock);
    return (max_size == 0
            || (current_size + size - released_size_assumption <= (max_size_ ? *max_size_ : max_size.load())))
        && (max_elements == 0
            || current_elements_num + elements - released_elements_assumption <= (max_elements_ ? *max_elements_ : max_elements.load()));
}

EvictionInfoPtr LRUFileCachePriority::collectEvictionInfo(
    size_t size,
    size_t elements,
    IFileCachePriority::Iterator *,
    bool is_total_space_cleanup,
    const IFileCachePriority::OriginInfo & origin_info,
    const CacheStateGuard::Lock & lock)
{
    auto info = std::make_unique<QueueEvictionInfo>(description, origin_info.user_id);
    if (!size && !elements)
        return std::make_unique<EvictionInfo>(queue_id, std::move(info));

    /// Total space cleanup is for keep_free_space_size(elements)_ratio feature.
    if (is_total_space_cleanup)
    {
        info->size_to_evict = std::min(size, getSize(lock));
        info->elements_to_evict = std::min(elements, getElementsCount(lock));
        return std::make_unique<EvictionInfo>(queue_id, std::move(info));
    }

    /// max_size == 0 => unlimitted size
    const size_t available_size = max_size ? max_size - state->getSize(lock) : size;
    if (available_size < size)
        info->size_to_evict = size - available_size;

    /// max_elements == 0 => unlimitted elements
    const size_t available_elements = max_elements ? max_elements - state->getElementsCount(lock) : elements;
    if (available_elements < elements)
        info->elements_to_evict = elements - available_elements;

    /// As eviction is done without a cache priority lock,
    /// then if some space was partially available and some needed
    /// to be freed via eviction, we need to make sure that this
    /// partially available space is still available
    /// after we finish with eviction for non-available space.
    /// So we create a space holder for the currently available part
    /// of the required space for the duration of eviction of the other
    /// currently non-available part of the space.
    size_t size_to_hold = info->size_to_evict ? available_size : size;
    size_t elements_to_hold = info->elements_to_evict ? available_elements : elements;

    if (size_to_hold || elements_to_hold)
    {
        info->hold_space = std::make_unique<IFileCachePriority::HoldSpace>(
            size_to_hold,
            elements_to_hold,
            *this,
            lock);
    }
    return std::make_unique<EvictionInfo>(queue_id, std::move(info));
}

bool LRUFileCachePriority::collectCandidatesForEviction(
    EvictionInfo & eviction_info,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    InvalidatedEntriesInfos & invalidated_entries,
    IFileCachePriority::IteratorPtr /* reservee */,
    EvictionCursor eviction_cursor,
    size_t max_candidates_size,
    bool /* is_total_space_cleanup */,
    const OriginInfo &,
    CachePriorityGuard & cache_guard,
    CacheStateGuard &)
{
    const auto & info = eviction_info.get(queue_id);
    size_t size = info.size_to_evict;
    size_t elements = info.elements_to_evict;

    if (!size && !elements)
        return true;

    ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictionTries);

    auto get_iteration_result = [&]()
    {
        if ((!size || stat.total_stat.releasable_size >= size)
            && (!elements || stat.total_stat.releasable_count >= elements))
            return IterationResult::BREAK;

        if (max_candidates_size && res.size() >= max_candidates_size)
            return IterationResult::BREAK;

        return IterationResult::CONTINUE;
    };

    auto lock = cache_guard.readLock();

    const bool use_cursor = eviction_cursor != EvictionCursor::FromHead;

    auto start_pos = queue.begin();
    if (use_cursor)
    {
        auto current_eviction_pos = getEvictionPos(eviction_cursor, lock);
        if (current_eviction_pos != LRUQueue::iterator{}
            && current_eviction_pos != queue.end()
            && start_pos != current_eviction_pos)
        {
            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictionReusedIterator);
            start_pos = current_eviction_pos;
        }
    }

    auto iteration_pos = iterateImpl(
        start_pos,
        [&](LockedKey & locked_key, const FileSegmentMetadataPtr & segment_metadata)
    {
        if (get_iteration_result() == IterationResult::BREAK)
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

            return get_iteration_result();
        }

        ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictionSkippedFileSegments);
        stat.update(
            segment_metadata->size(),
            file_segment->getKind(),
            FileCacheReserveStat::State::NonReleasable);

        return IterationResult::CONTINUE;
    },
    stat, invalidated_entries, lock);

    if (use_cursor)
        setEvictionPos(eviction_cursor, iteration_pos, lock);

    lock.unlock();

    const bool success = (max_candidates_size && res.size() >= max_candidates_size)
        || ((!size || stat.total_stat.releasable_size >= size)
            && (!elements || stat.total_stat.releasable_count >= elements));

    if (!success)
    {
        LOG_TEST(
            log, "Failed to collect eviction candidates "
            "(for size: {}, elements: {}, current size: {}, current elements: {}): {}",
            size, elements, getSizeApprox(), getElementsCountApprox(), stat.total_stat.toString());
    }
    return success;
}

LRUFileCachePriority::LRUIterator LRUFileCachePriority::move(
    LRUIterator & it,
    LRUFileCachePriority & other,
    const CachePriorityGuard::WriteLock & lock,
    const CacheStateGuard::Lock & state_lock)
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

    other.moveEvictionPosIfEqual(it.iterator, lock);
    queue.splice(queue.end(), other.queue, it.iterator);

    state->add(entry.size, /* elements */1, state_lock);
    other.state->sub(entry.size, /* elements */1);

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
    return std::make_shared<IPriorityDump>(res);
}

bool LRUFileCachePriority::modifySizeLimits(
    size_t max_size_, size_t max_elements_, double /* size_ratio_ */, const CacheStateGuard::Lock & lock)
{
    if (max_size == max_size_ && max_elements == max_elements_)
        return false; /// Nothing to change.

    if (state->getSize(lock) > max_size_ || state->getElementsCount(lock) > max_elements_)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot modify size limits to {} in size and {} in elements: "
                        "not enough space freed. Current size: {}/{}, elements: {}/{} ({})",
                        max_size_, max_elements_, state->getSize(lock), max_size.load(),
                        state->getElementsCount(lock), max_elements.load(), description);
    }

    LOG_INFO(log, "Modifying size limits from {} to {} in size, "
             "from {} to {} in elements count",
             max_size.load(), max_size_, max_elements.load(), max_elements_);

    max_size = max_size_;
    max_elements = max_elements_;
    return true;
}

EvictionInfoPtr LRUFileCachePriority::collectEvictionInfoForResize(
    size_t desired_max_size,
    size_t desired_max_elements,
    const OriginInfo & origin_info,
    const CacheStateGuard::Lock & lock)
{
    size_t current_size = getSize(lock);
    size_t current_elements = getElementsCount(lock);
    size_t size_to_evict = current_size > desired_max_size ? current_size - desired_max_size : 0;
    size_t elements_to_evict = current_elements > desired_max_elements ? current_elements - desired_max_elements : 0;
    return collectEvictionInfo(
        size_to_evict, elements_to_evict,
        /* reservee */ nullptr,
        /* is_total_space_cleanup */ true,
        origin_info, lock);
}

bool LRUFileCachePriority::tryIncreasePriority(
    Iterator & iterator,
    bool /* is_space_reservation_complete */,
    CachePriorityGuard & queue_guard,
    CacheStateGuard &)
{
    auto lock = queue_guard.tryWriteLock();
    if (!lock.owns_lock())
        return false;

    const auto & entry = iterator.getEntry();
    chassert(entry->getState() == Entry::State::Active);

    auto it = dynamic_cast<const LRUFileCachePriority::LRUIterator &>(iterator).get();
    moveEvictionPosIfEqual(it, lock);
    queue.splice(queue.end(), queue, it);
    return true;
}

IFileCachePriority::EntryPtr LRUFileCachePriority::LRUIterator::getEntry() const
{
    assertValid();
    return entry.lock();
}

bool LRUFileCachePriority::LRUIterator::isValid(const CachePriorityGuard::WriteLock &) const
{
    return entry.lock() != nullptr && iterator != LRUQueue::iterator{};
}

void LRUFileCachePriority::LRUIterator::remove(const CachePriorityGuard::WriteLock & lock)
{
    assertValid();
    cache_priority->remove(iterator, lock);
    iterator = LRUQueue::iterator{};
}

void LRUFileCachePriority::LRUIterator::invalidate() noexcept
{
    invalidateImpl();

    /// Only the `Main` priority drains `invalidated_refs` via the background cleanup task.
    if (cache_priority->getQueueType() == QueueType::Main)
        cache_priority->addInvalidatedRef(entry, iterator);
}

void LRUFileCachePriority::LRUIterator::invalidateBeforeRemove(const CachePriorityGuard::WriteLock &) noexcept
{
    /// The caller removes the entry under the same write lock right after,
    /// so do not register it for the background cleanup.
    invalidateImpl();
}

void LRUFileCachePriority::LRUIterator::invalidateImpl() noexcept
{
    auto entry_ptr = entry.lock();
    chassert(entry_ptr);

#ifdef DEBUG_OR_SANITIZER_BUILD
    LOG_TEST(cache_priority->log,
             "Invalidating entry in LRU queue {}: {}",
             entry_ptr->toString(), cache_priority->getApproxStateInfoForLog());
#endif

    size_t entry_size = entry_ptr->size;
    entry_ptr->size = 0;
    entry_ptr->setInvalidatedFlag();
    if (cache_priority->getQueueType() == QueueType::Main)
        CurrentMetrics::add(CurrentMetrics::FilesystemCacheInvalidatedElements);

    if (entry_size)
        cache_priority->state->sub(entry_size, 1);
}

void LRUFileCachePriority::LRUIterator::incrementSize(
    size_t size,
    const CacheStateGuard::Lock & lock)
{
    chassert(size);
    assertValid();

    auto entry_ptr = entry.lock();
    chassert(entry_ptr);

    size_t elements = entry_ptr->size > 0 ? 0 : 1;

    if (!cache_priority->canFit(size, elements, lock))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot increment size by {} for entry {}. Current state: {}",
                        size, entry_ptr->toString(), cache_priority->getStateInfoForLog(lock));
    }

    LOG_TEST(
        cache_priority->log,
        "Incrementing size with {} in LRU queue for entry {}",
        size, entry_ptr->toString());

    cache_priority->state->add(size, elements, lock);
    entry_ptr->size += size;

    cache_priority->check(lock);
}

void LRUFileCachePriority::LRUIterator::decrementSize(size_t size)
{
    assertValid();
    chassert(size);

    auto entry_ptr = entry.lock();
    chassert(entry_ptr);
    chassert(entry_ptr->size >= 0);
    chassert(entry_ptr->size >= size);

    LOG_TEST(cache_priority->log,
             "Decrement size with {} in LRU queue entry {}",
             size, entry_ptr->toString());

    const bool became_empty = entry_ptr->size == size;
    cache_priority->state->sub(size, /* elements */became_empty ? 1 : 0);
    entry_ptr->size -= size;
}

bool LRUFileCachePriority::LRUIterator::assertValid() const
{
    const bool is_iterator_valid = iterator != LRUQueue::iterator{};
    auto entry_ptr = entry.lock();
    if (!entry_ptr || !is_iterator_valid)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Attempt to use invalid iterator (entry: {}, iterator: {})",
            bool(entry_ptr), is_iterator_valid);
    }
    chassert(entry_ptr == *iterator);
    return true;
}

void LRUFileCachePriority::shuffle(const CachePriorityGuard::WriteLock &)
{
    chassert(TSA_SUPPRESS_WARNING_FOR_READ(reserve_eviction_pos) == queue.end());
    chassert(TSA_SUPPRESS_WARNING_FOR_READ(background_eviction_pos) == queue.end());
    std::vector<LRUQueue::iterator> its;
    its.reserve(queue.size());
    for (auto it = queue.begin(); it != queue.end(); ++it)
        its.push_back(it);
    pcg64 generator(randomSeed());
    std::shuffle(its.begin(), its.end(), generator);
    for (auto & it : its)
        queue.splice(queue.end(), queue, it);
}

std::string LRUFileCachePriority::getStateInfoForLog(const CacheStateGuard::Lock & lock) const
{
    return fmt::format(
        "size: {}/{}, elements: {}/{}, hold size: {}, hold elements: {}, description: {}",
        getSize(lock), max_size.load(),
        getElementsCount(lock), max_elements.load(),
        total_hold_size.load(), total_hold_elements.load(), description);
}

std::string LRUFileCachePriority::getApproxStateInfoForLog() const
{
    return fmt::format("size: {}/{}, elements: {}/{} (description: {})",
                       getSizeApprox(), max_size.load(), getElementsCountApprox(), max_elements.load(), description);
}

void LRUFileCachePriority::holdImpl(
    size_t size,
    size_t elements,
    const CacheStateGuard::Lock & lock)
{
    chassert(size || elements);

    if (!canFit(size, elements, lock))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot take space {} in size and {} in elements. "
                        "({})", size, elements, getStateInfoForLog(lock));
    }

    state->add(size, elements, lock);

    total_hold_size += size;
    total_hold_elements += elements;

    //LOG_TEST(log, "Hold {} by size and {} by elements", size, elements);
}

void LRUFileCachePriority::releaseImpl(size_t size, size_t elements)
{
    /// Once the atomic decrements below reach 0, `CacheUsagePerUser::snapshot`
    /// may concurrently erase this priority and free `cache_usage_stat_guard`,
    /// so pin it locally to keep the mutex alive until `~unique_lock`.
    /// This lock is only needed for `OvercommitFileCachePriority::check`'s
    /// debug consistency check; non-overcommit policies leave the guard unset.
    auto stat_guard = cache_usage_stat_guard;
    auto lock = stat_guard
        ? std::optional<CacheUsageStatGuard::Lock>(stat_guard->lock())
        : std::nullopt;

    state->sub(size, elements);

    total_hold_size -= size;
    total_hold_elements -= elements;

    //LOG_TEST(log, "Released {} by size and {} by elements", size, elements);
}

LRUFileCachePriority::LRUQueue::iterator & LRUFileCachePriority::evictionPos(EvictionCursor cursor)
{
    switch (cursor)
    {
        case EvictionCursor::Reserve:
            return reserve_eviction_pos;
        case EvictionCursor::Background:
            return background_eviction_pos;
        case EvictionCursor::FromHead:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "EvictionCursor::FromHead has no persistent cursor");
    }
}

const LRUFileCachePriority::LRUQueue::iterator & LRUFileCachePriority::evictionPos(EvictionCursor cursor) const
{
    switch (cursor)
    {
        case EvictionCursor::Reserve:
            return reserve_eviction_pos;
        case EvictionCursor::Background:
            return background_eviction_pos;
        case EvictionCursor::FromHead:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "EvictionCursor::FromHead has no persistent cursor");
    }
}

LRUFileCachePriority::LRUQueue::iterator LRUFileCachePriority::getEvictionPos(EvictionCursor cursor, const CachePriorityGuard::ReadLock &) const
{
    std::lock_guard lk(eviction_pos_mutex);
    return evictionPos(cursor);
}

void LRUFileCachePriority::setEvictionPos(EvictionCursor cursor, LRUQueue::iterator it, const CachePriorityGuard::ReadLock &)
{
    std::lock_guard lk(eviction_pos_mutex);
    evictionPos(cursor) = it;
}

void LRUFileCachePriority::moveEvictionPosIfEqual(LRUQueue::iterator it, const CachePriorityGuard::WriteLock &)
{
    std::lock_guard lk(eviction_pos_mutex);
    for (auto * pos : {&reserve_eviction_pos, &background_eviction_pos})
    {
        if (*pos != LRUQueue::iterator{} && *pos == it)
            *pos = std::next(it);
    }
}
}
