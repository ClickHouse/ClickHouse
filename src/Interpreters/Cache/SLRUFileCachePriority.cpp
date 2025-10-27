#include <mutex>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/SLRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/EvictionCandidates.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>


namespace ProfileEvents
{
    extern const Event FilesystemCacheEvictedFileSegmentsDuringPriorityIncrease;
}
namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    size_t getRatio(size_t total, double ratio)
    {
        return std::lround(total * std::clamp(ratio, 0.0, 1.0));
    }
}

SLRUFileCachePriority::SLRUFileCachePriority(
    size_t max_size_,
    size_t max_elements_,
    double size_ratio_,
    const std::string & description_,
    LRUFileCachePriority::StatePtr probationary_state_,
    LRUFileCachePriority::StatePtr protected_state_)
    : IFileCachePriority(max_size_, max_elements_)
    , description(description_)
    , size_ratio(size_ratio_)
    , protected_queue(LRUFileCachePriority(getRatio(max_size_, size_ratio),
                                           getRatio(max_elements_, size_ratio),
                                           description_ + ", protected",
                                           protected_state_))
    , probationary_queue(LRUFileCachePriority(getRatio(max_size_, 1 - size_ratio),
                                              getRatio(max_elements_, 1 - size_ratio),
                                              description_ + ", probationary",
                                              probationary_state_))
    , log(getLogger("SLRUFileCachePriority(" + description_ + ")"))
{
    LOG_INFO(
        log, "Probationary queue {} in size and {} in elements. "
        "Protected queue {} in size and {} in elements",
        probationary_queue.max_size.load(), probationary_queue.max_elements.load(),
        protected_queue.max_size.load(), protected_queue.max_elements.load());

    if (probationary_queue.max_size == 0 || protected_queue.max_size == 0)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Incorrect max size cache configuration. Max size: {}, size ratio: {}. "
            "Cannot have zero max size after ratio is applied.",
            max_size_, size_ratio_);
    }
    if (probationary_queue.max_elements == 0 || protected_queue.max_elements == 0)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Incorrect max elements cache configuration. Max size: {}, size ratio: {}. "
            "Cannot have zero max elements after ratio is applied.",
            max_elements_, size_ratio_);
    }
}

FileCachePriorityPtr SLRUFileCachePriority::copy() const
{
    return std::make_unique<SLRUFileCachePriority>(
        max_size, max_elements, size_ratio, description, probationary_queue.state, protected_queue.state);
}

size_t SLRUFileCachePriority::getSize(const CacheStateGuard::Lock & lock) const
{
    return protected_queue.getSize(lock) + probationary_queue.getSize(lock);
}

size_t SLRUFileCachePriority::getElementsCount(const CacheStateGuard::Lock & lock) const
{
    return protected_queue.getElementsCount(lock) + probationary_queue.getElementsCount(lock);
}

size_t SLRUFileCachePriority::getSizeApprox() const
{
    return protected_queue.getSizeApprox() + probationary_queue.getSizeApprox();
}

size_t SLRUFileCachePriority::getElementsCountApprox() const
{
    return protected_queue.getElementsCountApprox() + probationary_queue.getElementsCountApprox();
}

bool SLRUFileCachePriority::canFit( /// NOLINT
    size_t size,
    size_t elements,
    const CacheStateGuard::Lock & lock,
    IteratorPtr reservee,
    bool best_effort) const
{
    if (best_effort)
        return probationary_queue.canFit(size, elements, lock) || protected_queue.canFit(size, elements, lock);

    if (reservee)
    {
        const auto * slru_iterator = assert_cast<SLRUIterator *>(reservee->getNestedOrThis());
        if (slru_iterator->is_protected)
            return protected_queue.canFit(size, elements, lock);
        return probationary_queue.canFit(size, elements, lock);
    }
    return probationary_queue.canFit(size, elements, lock);
}

IFileCachePriority::IteratorPtr SLRUFileCachePriority::add( /// NOLINT
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const UserInfo &,
    const CachePriorityGuard::WriteLock & lock,
    const CacheStateGuard::Lock * state_lock,
    bool is_startup)
{
    bool is_protected;
    if (is_startup)
    {
        if (!state_lock)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Startup initialization requires state lock");

        /// If it is server startup, we put entries in any queue it will fit in,
        /// but with preference for probationary queue,
        /// because we do not know the distribution between queues after server restart.
        is_protected = probationary_queue.canFit(size, /* elements */1, *state_lock);
    }
    else
    {
        if (size && !state_lock)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Adding non-zero size entry without state lock "
                "(key: {}, offset: {})", key_metadata->key, offset);
        }
        is_protected = false;
    }

    auto entry = std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata);
    return std::make_shared<SLRUIterator>(
        this,
        is_protected
            ? protected_queue.add(std::move(entry), lock, state_lock)
            : probationary_queue.add(std::move(entry), lock, state_lock),
        is_protected);
}

void SLRUFileCachePriority::iterate(
    IterateFunc func,
    FileCacheReserveStat & stat,
    const CachePriorityGuard::ReadLock & lock)
{
    protected_queue.iterate(func, stat, lock);
    probationary_queue.iterate(func, stat, lock);
}

void SLRUFileCachePriority::resetEvictionPos(const CachePriorityGuard::ReadLock & lock)
{
    protected_queue.resetEvictionPos(lock);
    probationary_queue.resetEvictionPos(lock);
}

EvictionInfoPtr SLRUFileCachePriority::collectEvictionInfo(
    size_t size,
    size_t elements,
    IFileCachePriority::Iterator * reservee,
    bool is_total_space_cleanup,
    const CacheStateGuard::Lock & lock)
{
    return collectEvictionInfoImpl(size, elements, reservee, is_total_space_cleanup, lock, std::nullopt);
}

EvictionInfoPtr SLRUFileCachePriority::collectEvictionInfoImpl(
    size_t size,
    size_t elements,
    IFileCachePriority::Iterator * reservee,
    bool is_total_space_cleanup,
    const CacheStateGuard::Lock & lock,
    std::optional<bool> in_protected)
{
    if (!size && !elements)
        return std::make_unique<EvictionInfo>();

    /// Total space cleanup is for keep_free_space_size(elements)_ratio feature.
    if (is_total_space_cleanup)
    {
        /// Remove everything from probationary first
        /// and only if it's empty - remove from protected as well.
        size_t evict_size_from_probationary = std::min(size, probationary_queue.getSize(lock));
        size_t evict_elements_from_probationary = std::min(elements, probationary_queue.getElementsCount(lock));

        chassert(evict_size_from_probationary || evict_elements_from_probationary);
        size -= evict_size_from_probationary;
        elements -= evict_elements_from_probationary;

        size_t evict_size_from_protected = size ? std::min(size, protected_queue.getSize(lock)) : 0;
        size_t evict_elements_from_protected = elements ? std::min(elements, protected_queue.getElementsCount(lock)) : 0;

        auto info = probationary_queue.collectEvictionInfo(
            evict_size_from_probationary,
            evict_elements_from_probationary,
            reservee,
            is_total_space_cleanup,
            lock);

        if (evict_size_from_protected || evict_elements_from_protected)
        {
            info->add(protected_queue.collectEvictionInfo(
                          size, elements, reservee, is_total_space_cleanup, lock));
        }

        return info;
    }

    bool evict_in_protected = in_protected.value_or(false);
    SLRUIterator * slru_iterator = nullptr;
    if (evict_in_protected)
    {
        chassert(!reservee);
    }
    else if (reservee)
    {
        slru_iterator = assert_cast<SLRUIterator *>(reservee->getNestedOrThis());
        evict_in_protected = slru_iterator->is_protected;

        chassert(evict_in_protected
                 ? slru_iterator->lru_iterator.cache_priority == &protected_queue
                 : slru_iterator->lru_iterator.cache_priority == &probationary_queue);
    }

    std::unique_ptr<EvictionInfo> info;
    if (evict_in_protected)
    {
        info = protected_queue.collectEvictionInfo(size, elements, reservee, is_total_space_cleanup, lock);

        /// If protected queue required eviction, we need to "downgrade"
        /// its eviction candidates into probationary queue.
        if (info->requiresEviction())
        {
            /// Make sure we have space in probationary queue for the downgrade.
            auto downgrade_info = probationary_queue.collectEvictionInfo(
                info->size_to_evict,
                info->elements_to_evict,
                reservee,
                is_total_space_cleanup,
                lock);
            info->add(std::move(downgrade_info));
        }
    }
    else
    {
        info = probationary_queue.collectEvictionInfo(size, elements, reservee, is_total_space_cleanup, lock);
    }

    /// Make sure this queue element (our reservee) is not
    /// 1. "downgraded" (moved from protected to probationary queue)
    /// by other concurrent space reservation attempts.
    /// 2. "upgraded" by concurrently executed tryIncreasePriority.
    /// E.g. we mark reservee's queue entry unmovable for the duration
    /// of its space reservation.
    if (slru_iterator)
    {
        slru_iterator->disableMoving(lock);
        info->setOnFinishFunc([=]{ slru_iterator->enableMoving(); });
    }
    return info;
}

bool SLRUFileCachePriority::collectCandidatesForEviction(
    const EvictionInfo & eviction_info,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    IFileCachePriority::IteratorPtr reservee,
    bool continue_from_last_eviction_pos,
    size_t max_candidates_size,
    bool is_total_space_cleanup,
    const UserID & user_id,
    const CachePriorityGuard::ReadLock & lock)
{
    if (is_total_space_cleanup)
    {
        auto success = probationary_queue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            user_id,
            lock);

        if (success)
            return true;

        /// We do not use collectCandidatesForEvictionInProtected method,
        /// because it will "downgrade" instead of remove,
        /// but for total space cleanup we need remove.
        return protected_queue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            user_id,
            lock);
    }

    /// If `reservee` is nullptr, then it is the first space reservation attempt
    /// for a corresponding file segment, so it will be directly put into probationary queue.
    if (!reservee)
    {
        return probationary_queue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            user_id,
            lock);
    }

    auto * slru_iterator = assert_cast<SLRUIterator *>(reservee->getNestedOrThis());
    bool success = false;

    /// If `reservee` is not nullptr (e.g. is already in some queue),
    /// we need to check in which queue (protected/probationary) it currently is
    /// (in order to know where we need to free space).
    if (slru_iterator->is_protected)
    {
        chassert(slru_iterator->lru_iterator.cache_priority == &protected_queue);
        /// Entry is in protected queue.
        /// Check if we have enough space in protected queue to fit a new size of entry.
        /// `size` is the increment to the current entry.size we want to increase.
        success = collectCandidatesForEvictionInProtected(
            eviction_info,
            stat,
            res,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            user_id,
            lock);
    }
    else
    {
        chassert(slru_iterator->lru_iterator.cache_priority == &probationary_queue);
        success = probationary_queue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            user_id,
            lock);
    }

    return success;
}

/// TODO: currently this will find only releadable entries,
/// but since we are only downgrading, then it does not matter.
bool SLRUFileCachePriority::collectCandidatesForEvictionInProtected(
    const EvictionInfo & eviction_info,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    IFileCachePriority::IteratorPtr reservee,
    bool continue_from_last_eviction_pos,
    size_t max_candidates_size,
    bool is_total_space_cleanup,
    const UserID & user_id,
    const CachePriorityGuard::ReadLock & lock)
{
    auto downgrade_candidates = std::make_shared<EvictionCandidates>();
    FileCacheReserveStat downgrade_stat;
    if (!protected_queue.collectCandidatesForEviction(
        eviction_info,
        downgrade_stat,
        *downgrade_candidates,
        reservee,
        continue_from_last_eviction_pos,
        max_candidates_size,
        is_total_space_cleanup,
        user_id,
        lock))
    {
        return false;
    }

    /// We can have no downgrade candidates because cache size could
    /// reduce concurrently because of lock-free cache entries invalidation.
    if (downgrade_candidates->size() == 0)
    {
        return true;
    }

    /// If not enough space - we need to "downgrade" lowest priority entries
    /// from protected queue to probationary queue,
    /// so collect eviction candidates in probationary now.
    if (!probationary_queue.collectCandidatesForEviction(
        eviction_info,
        stat,
        res,
        reservee,
        continue_from_last_eviction_pos,
        max_candidates_size,
        is_total_space_cleanup,
        user_id,
        lock))
    {
        return false;
    }

    /// Set callback to execute the "downgrade".
    /// As PriorityGuard::WriteLock allows to only move elements,
    /// but not increment size of any of the queues,
    /// we move elements with zero size and increase the size later in a separate callback.
    auto downgraded_file_segments = std::make_shared<std::vector<FileSegmentMetadataPtr>>();
    downgraded_file_segments->reserve(downgrade_candidates->size());
    res.setAfterEvictWriteFunc([=, this](const CachePriorityGuard::WriteLock & lk) mutable
    {
        for (auto & [key, key_candidates] : *downgrade_candidates)
        {
            while (!key_candidates.candidates.empty())
            {
                auto candidate = key_candidates.candidates.front();
                auto * candidate_it = assert_cast<SLRUIterator *>(candidate->getQueueIterator()->getNestedOrThis());

                auto prev_lru_iterator = candidate_it->lru_iterator;
                downgrade_candidates->addEntryToInvalidate(
                    std::make_shared<SLRUIterator>(candidate_it->cache_priority, std::move(prev_lru_iterator), true));

                auto entry = candidate_it->getEntry();
                EntryPtr new_entry = std::make_shared<Entry>(entry->key, entry->offset, /* size */0, entry->key_metadata);
                candidate_it->lru_iterator = probationary_queue.add(new_entry, lk, nullptr);
                candidate_it->is_protected = false;
                downgraded_file_segments->push_back(candidate);

                key_candidates.candidates.pop_front();
            }
        }
    });

    /// Set incrementing size callback, as explained in the previous comment.
    res.setAfterEvictStateFunc([=](const CacheStateGuard::Lock & lk)
    {
        downgrade_candidates->invalidateQueueEntries(lk);
        chassert(!downgraded_file_segments->empty());
        for (const auto & metadata : *downgraded_file_segments)
        {
            const auto & file_segment = metadata->file_segment;
            auto * iterator = assert_cast<SLRUIterator *>(file_segment->getQueueIterator()->getNestedOrThis());
            iterator->setEntry(iterator->lru_iterator.getEntry(), lk);
            chassert(iterator->getEntry()->size == 0);
            iterator->incrementSize(file_segment->getReservedSize(), lk);
        }
    });

    LOG_TEST(
        log, "Eviction info: {}. Downgrading {} elements from protected to probationary. Total size: {}",
        eviction_info.toString(), downgrade_candidates->size(), downgrade_stat.total_stat.releasable_size);

    return true;
}

bool SLRUFileCachePriority::tryIncreasePriority(
    Iterator & iterator_,
    CachePriorityGuard & queue_guard,
    CacheStateGuard & state_guard)
{
    auto & iterator = dynamic_cast<SLRUFileCachePriority::SLRUIterator &>(iterator_);
    chassert(iterator.assertValid());

    /// If entry is already in protected queue,
    /// we only need to increase its priority within the protected queue.
    if (iterator.is_protected)
    {
        return protected_queue.tryIncreasePriority(iterator.lru_iterator, queue_guard, state_guard);
    }

    chassert(iterator.lru_iterator.cache_priority == &probationary_queue);

    EntryPtr entry = iterator.getEntry();

    /// Entry is in probationary queue.
    /// We need to move it to protected queue.
    std::unique_ptr<EvictionInfo> downgrade_info;
    {
        auto lock = state_guard.lock();
        if (!iterator.isMovable(lock))
        {
            lock.unlock();
            /// We cannot move the entry to protected queue as it is
            /// temporarily unmovable because of a concurrent space reservation.
            return probationary_queue.tryIncreasePriority(iterator.lru_iterator, queue_guard, state_guard);
        }

        downgrade_info = collectEvictionInfoImpl(
            entry->size,
            /* elements */1,
            /* reservee */nullptr,
            /* is_total_space_cleanup */false,
            lock,
            /* in_protected */true);

#ifdef DEBUG_OR_SANITIZER_BUILD
        LOG_TEST(
            log, "Entry: {}. Downgrade info: {} ({})",
            entry->toString(), downgrade_info->toString(), getStateInfoForLog(lock));
#endif
    }

    /// We need to remove the entry from probationary first
    /// in order to make space for downgrade from protected.
    //iterator.lru_iterator.remove(lock);

    EvictionCandidates downgrade_candidates;
    FileCacheReserveStat downgrade_stat;
    /// Check if there is enough space in protected queue to move entry there.
    /// If not - we need to "downgrade" lowest priority entries from protected
    /// queue to probationary queue.

    bool collected_downgrade_candidates = false;
    {
        auto lock = queue_guard.readLock();
        collected_downgrade_candidates = collectCandidatesForEvictionInProtected(
            *downgrade_info,
            downgrade_stat,
            downgrade_candidates,
            /* reservee */nullptr,
            /* continue_from_last_eviction_pos */false,
            /* max_candidates_size */0,
            /* is_total_space_cleanup */false,
            FileCache::getInternalUser().user_id,
            lock);
    }

    if (!collected_downgrade_candidates)
        return probationary_queue.tryIncreasePriority(iterator.lru_iterator, queue_guard, state_guard);

    downgrade_candidates.evict();

    /// Count how much we evict,
    /// because it could affect performance if we have to do this often.
    ProfileEvents::increment(
        ProfileEvents::FilesystemCacheEvictedFileSegmentsDuringPriorityIncrease,
        downgrade_candidates.size());

    std::optional<LRUIterator> new_iterator;
    /// Create an *empty* entry.
    EntryPtr new_entry = std::make_shared<Entry>(entry->key, entry->offset, /* size */0, entry->key_metadata);
    {
        auto lock = queue_guard.writeLock();
        downgrade_candidates.afterEvictWrite(lock);
        removeEntries(downgrade_stat.total_stat.invalidated_entries, lock);

        iterator.lru_iterator.remove(lock);

        try
        {
            new_iterator = protected_queue.add(new_entry, lock, /* state_lock */nullptr);
        }
        catch (...)
        {
            chassert(false);
            /// TODO return the entry back to avoid a case when file segment has no entry.
            throw;
        }
    }

    try
    {
        auto lock = state_guard.lock();
        downgrade_info->releaseHoldSpace(lock);
        downgrade_candidates.afterEvictState(lock);

        new_iterator->incrementSize(entry->size, lock);
        iterator.lru_iterator = *new_iterator;
        iterator.setEntry(std::move(new_entry), lock);
        iterator.is_protected = true;
    }
    catch (...)
    {
        new_iterator->remove(queue_guard.writeLock());
        chassert(false);
        /// TODO return the entry back to avoid a case when file segment has no entry.
        throw;
    }

    return true;
}

LRUFileCachePriority::LRUIterator SLRUFileCachePriority::addOrThrow(
    EntryPtr entry,
    LRUFileCachePriority & queue,
    const CachePriorityGuard::WriteLock & lock,
    const CacheStateGuard::Lock & state_lock)
{
    try
    {
        return queue.add(entry, lock, &state_lock);
    }
    catch (...)
    {
        const auto initial_exception = getCurrentExceptionMessage(true);
        try
        {
            /// We cannot allow a situation that a file exists on filesystem, but
            /// there is no corresponding entry in priority queue for it,
            /// because it will mean that cache became inconsistent.
            /// So let's try to fix the situation.
            auto metadata = entry->key_metadata->tryLock();
            chassert(metadata);
            if (metadata)
            {
                auto segment_metadata = metadata->tryGetByOffset(entry->offset);
                metadata->removeFileSegment(entry->offset, segment_metadata->file_segment->lock());
            }
        }
        catch (...)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected exception: {} (Initial exception: {}). Cache will become inconsistent",
                getCurrentExceptionMessage(true), initial_exception);
        }

        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Failed to create queue entry: {}", getCurrentExceptionMessage(true));
    }
}

IFileCachePriority::PriorityDumpPtr SLRUFileCachePriority::dump(const CachePriorityGuard::ReadLock & lock)
{
    auto res = dynamic_pointer_cast<LRUFileCachePriority::LRUPriorityDump>(probationary_queue.dump(lock));
    auto part_res = dynamic_pointer_cast<LRUFileCachePriority::LRUPriorityDump>(protected_queue.dump(lock));
    res->merge(*part_res);
    return res;
}

void SLRUFileCachePriority::shuffle(const CachePriorityGuard::WriteLock & lock)
{
    protected_queue.shuffle(lock);
    probationary_queue.shuffle(lock);
}

bool SLRUFileCachePriority::modifySizeLimits(
    size_t max_size_, size_t max_elements_, double size_ratio_, const CacheStateGuard::Lock & lock)
{
    if (max_size == max_size_ && max_elements == max_elements_ && size_ratio == size_ratio_)
        return false; /// Nothing to change.

    protected_queue.modifySizeLimits(getRatio(max_size_, size_ratio_), getRatio(max_elements_, size_ratio_), 0, lock);
    probationary_queue.modifySizeLimits(getRatio(max_size_, 1 - size_ratio_), getRatio(max_elements_, 1 - size_ratio_), 0, lock);

    max_size = max_size_;
    max_elements = max_elements_;
    size_ratio = size_ratio_;
    return true;
}

SLRUFileCachePriority::SLRUIterator::SLRUIterator(
    SLRUFileCachePriority * cache_priority_,
    LRUFileCachePriority::LRUIterator && lru_iterator_,
    bool is_protected_)
    : cache_priority(cache_priority_)
    , lru_iterator(lru_iterator_)
    , entry(lru_iterator.getEntry())
    , is_protected(is_protected_)
{
}

SLRUFileCachePriority::EntryPtr SLRUFileCachePriority::SLRUIterator::getEntry() const
{
    std::lock_guard lock(entry_mutex);
    auto entry_ptr = entry.lock();
    if (!entry_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Entry pointer expired");
    return entry_ptr;
}

void SLRUFileCachePriority::SLRUIterator::setEntry(EntryPtr && entry_, const CacheStateGuard::Lock &)
{
    std::lock_guard lock(entry_mutex);
    entry = entry_;
}

void SLRUFileCachePriority::SLRUIterator::incrementSize(size_t size, const CacheStateGuard::Lock & lock)
{
    assertValid();
    lru_iterator.incrementSize(size, lock);
}

void SLRUFileCachePriority::SLRUIterator::decrementSize(size_t size)
{
    assertValid();
    lru_iterator.decrementSize(size);
}

void SLRUFileCachePriority::SLRUIterator::invalidate()
{
    assertValid();
    lru_iterator.invalidate();
}

bool SLRUFileCachePriority::SLRUIterator::isValid(const CachePriorityGuard::WriteLock & lock)
{
    return lru_iterator.isValid(lock);
}

void SLRUFileCachePriority::SLRUIterator::remove(const CachePriorityGuard::WriteLock & lock)
{
    assertValid();
    lru_iterator.remove(lock);
}

bool SLRUFileCachePriority::SLRUIterator::assertValid() const
{
    lru_iterator.assertValid();
    return true;
}

std::string SLRUFileCachePriority::getStateInfoForLog(const CacheStateGuard::Lock & lock) const
{
    return fmt::format("total size {}/{}, elements {}/{}, "
                       "probationary queue size {}/{}, elements {}/{}, "
                       "protected queue size {}/{}, elements {}/{}",
                       getSize(lock), max_size.load(), getElementsCount(lock), max_elements.load(),
                       probationary_queue.getSize(lock), probationary_queue.max_size.load(),
                       probationary_queue.getElementsCount(lock), probationary_queue.max_elements.load(),
                       protected_queue.getSize(lock), protected_queue.max_size.load(),
                       protected_queue.getElementsCount(lock), protected_queue.max_elements.load());
}

void SLRUFileCachePriority::check(const CacheStateGuard::Lock & lock) const
{
    probationary_queue.check(lock);
    protected_queue.check(lock);
    IFileCachePriority::check(lock);
}

}
