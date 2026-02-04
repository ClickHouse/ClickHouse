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
    size_t getRatio(size_t total, double ratio, bool ceil = false)
    {
        if (ceil)
            return static_cast<size_t>(std::ceil(static_cast<double>(total) * std::clamp(ratio, 0.0, 1.0)));
        return std::lround(static_cast<double>(total) * std::clamp(ratio, 0.0, 1.0));
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
    const OriginInfo &,
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
    const CachePriorityGuard::WriteLock & lock,
    const CacheStateGuard::Lock * state_lock,
    bool is_startup)
{
    bool is_protected = false;
    if (is_startup)
    {
        chassert(size);
        if (!state_lock)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Startup initialization requires state lock");

        /// If it is server startup, we put entries in any queue it will fit in,
        /// but with preference for probationary queue,
        /// because we do not know the distribution between queues after server restart.
        is_protected = !probationary_queue.canFit(size, /* elements */1, *state_lock);
    }
    else if (size && !state_lock)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Adding non-zero size entry without state lock "
            "(key: {}, offset: {})", key_metadata->key, offset);
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

void SLRUFileCachePriority::resetEvictionPos()
{
    protected_queue.resetEvictionPos();
    probationary_queue.resetEvictionPos();
}

EvictionInfoPtr SLRUFileCachePriority::collectEvictionInfo(
    size_t size,
    size_t elements,
    IFileCachePriority::Iterator * reservee,
    bool is_total_space_cleanup,
    bool is_dynamic_resize,
    const OriginInfo & origin_info,
    const CacheStateGuard::Lock & lock)
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

        auto info = probationary_queue.collectEvictionInfo(
            evict_size_from_probationary,
            evict_elements_from_probationary,
            reservee,
            is_total_space_cleanup,
            is_dynamic_resize,
            origin_info,
            lock);

        size_t evict_size_from_protected = size ? std::min(size, protected_queue.getSize(lock)) : 0;
        size_t evict_elements_from_protected = elements ? std::min(elements, protected_queue.getElementsCount(lock)) : 0;

        info->add(
            protected_queue.collectEvictionInfo(
                evict_size_from_protected,
                evict_elements_from_protected,
                reservee,
                is_total_space_cleanup,
                is_dynamic_resize,
                origin_info,
                lock));
        return info;
    }

    if (is_dynamic_resize)
    {
        auto info = protected_queue.collectEvictionInfo(
            getRatio(size, size_ratio, /* ceil */true),
            getRatio(elements, size_ratio, /* ceil */true),
            /* reservee */nullptr,
            is_total_space_cleanup,
            is_dynamic_resize,
            origin_info,
            lock);
        info->add(probationary_queue.collectEvictionInfo(
            getRatio(size, 1 - size_ratio, /* ceil */true),
            getRatio(elements, 1 - size_ratio, /* ceil */true),
            /* reservee */nullptr,
            is_total_space_cleanup,
            is_dynamic_resize,
            origin_info,
            lock));
        return info;
    }

    bool evict_in_protected = false;
    SLRUIterator * slru_iterator = nullptr;
    if (reservee)
    {
        slru_iterator = assert_cast<SLRUIterator *>(reservee->getNestedOrThis());
        evict_in_protected = slru_iterator->is_protected;

        chassert(evict_in_protected
                 ? slru_iterator->lru_iterator.cache_priority == &protected_queue
                 : slru_iterator->lru_iterator.cache_priority == &probationary_queue);
    }

    if (evict_in_protected)
    {
        /// If protected queue required eviction, we need to "downgrade"
        /// its eviction candidates into probationary queue
        /// (to make sure we have space in probationary queue for the downgrade).
        /// But we cannot do it here, as we do not know in advance the exact size to downgrade.
        /// So we will do it in collectCandidatesForEviction.
        return protected_queue.collectEvictionInfo(
            size, elements, reservee, is_total_space_cleanup, is_dynamic_resize, origin_info, lock);
    }

    return probationary_queue.collectEvictionInfo(
        size, elements, reservee, is_total_space_cleanup, is_dynamic_resize, origin_info, lock);
}

bool SLRUFileCachePriority::collectCandidatesForEviction(
    const EvictionInfo & eviction_info,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    InvalidatedEntriesInfos & invalidated_entries,
    IFileCachePriority::IteratorPtr reservee,
    bool continue_from_last_eviction_pos,
    size_t max_candidates_size,
    bool is_total_space_cleanup,
    const OriginInfo & origin_info,
    CachePriorityGuard & cache_guard,
    CacheStateGuard & state_guard)
{
    if (is_total_space_cleanup)
    {
        bool success_probationary = probationary_queue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            invalidated_entries,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            cache_guard,
            state_guard);
        /// We do not quit here if !success_probationary,
        /// because in case of keep_up_free_space_ratio it is ok to evict at least something
        /// (so we will check res.size() instead of returned bool value).

        /// We do not use collectCandidatesForEvictionInProtected method,
        /// because it will "downgrade" instead of remove,
        /// but for total space cleanup we need remove.
        bool success_protected = protected_queue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            invalidated_entries,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            cache_guard,
            state_guard);

        return success_probationary && success_protected;
    }

    /// If `reservee` is nullptr, then it is the first space reservation attempt
    /// for a corresponding file segment, so it will be directly put into probationary queue.
    if (!reservee)
    {
        return probationary_queue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            invalidated_entries,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            cache_guard,
            state_guard);
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
        success = collectCandidatesForEvictionInProtected(
            eviction_info,
            stat,
            res,
            invalidated_entries,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            cache_guard,
            state_guard);
    }
    else
    {
        chassert(slru_iterator->lru_iterator.cache_priority == &probationary_queue);
        success = probationary_queue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            invalidated_entries,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            cache_guard,
            state_guard);
    }

    return success;
}

/// TODO: currently this will find only releasable entries,
/// but since we are only downgrading, then it does not matter.
bool SLRUFileCachePriority::collectCandidatesForEvictionInProtected(
    const EvictionInfo & eviction_info,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    InvalidatedEntriesInfos & invalidated_entries,
    IFileCachePriority::IteratorPtr reservee,
    bool continue_from_last_eviction_pos,
    size_t max_candidates_size,
    bool is_total_space_cleanup,
    const OriginInfo & origin_info,
    CachePriorityGuard & cache_guard,
    CacheStateGuard & state_guard)
{
    auto downgrade_candidates = std::make_shared<EvictionCandidates>();
    FileCacheReserveStat downgrade_stat;
    if (!protected_queue.collectCandidatesForEviction(
        eviction_info,
        downgrade_stat,
        *downgrade_candidates,
        invalidated_entries,
        reservee,
        continue_from_last_eviction_pos,
        max_candidates_size,
        is_total_space_cleanup,
        origin_info,
        cache_guard,
        state_guard))
    {
        return false;
    }

    /// We can have no downgrade candidates because cache size could
    /// reduce concurrently because of lock-free cache entries invalidation.
    if (downgrade_candidates->size() == 0)
    {
        return true;
    }

    /// We did not collect eviction info for probationary queue in advance
    /// (when doing so for protected queue),
    /// because we could not know how much space we will need to downgrade
    /// (because downgraded space >= space to reserve in protected).
    /// So we do it now.
    auto probationary_eviction_info = probationary_queue.collectEvictionInfo(
        downgrade_stat.total_stat.releasable_size,
        downgrade_stat.total_stat.releasable_count,
        /* reservee */nullptr,
        /* is_total_space_cleanup */false,
        /* is_dynamic_resize */false,
        origin_info,
        state_guard.lock());

    const bool requires_eviction = probationary_eviction_info->requiresEviction();
    /// FIXME: const_cast is a bad practice.
    const_cast<EvictionInfo &>(eviction_info).add(std::move(probationary_eviction_info));
    if (requires_eviction)
    {
        /// If not enough space - we need to "downgrade" lowest priority entries
        /// from protected queue to probationary queue,
        /// so collect eviction candidates in probationary now.
        if (!probationary_queue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            invalidated_entries,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            cache_guard,
            state_guard))
        {
            return false;
        }
    }

    struct DowngradedEntryInfo
    {
        IteratorPtr slru_iterator;
        /// Entry size as it was in protected queue.
        size_t entry_size = 0;
        /// Previous iterator to entry in protected queue.
        LRUIterator prev_nested_iterator;
        /// New iterator to entry in probationary queue.
        LRUIterator new_nested_iterator;
    };
    /// RAII wrapper to protect against the case when afterEvictState callback
    /// is not called because of some unexpected exception.
    struct DowngradedEntriesInfos : private std::vector<DowngradedEntryInfo>
    {
        explicit DowngradedEntriesInfos(size_t size) { reserve(size); }

        void add(DowngradedEntryInfo && info) { push_back(info); }

        [[maybe_unused]] size_t getSize() const { return size(); }

        std::optional<DowngradedEntryInfo> next()
        {
            if (empty())
                return std::nullopt;
            auto info = std::move(back());
            pop_back();
            return info;
        }

        ~DowngradedEntriesInfos()
        {
            /// Invalidate new unused iterators.
            /// If entries number is non-zero here, it must mean there was
            /// some exception because of which we failed to process new iterators.
            for (auto & entry : *this)
                entry.new_nested_iterator.invalidate();
        }
    };
    auto downgraded_entries = std::make_shared<DowngradedEntriesInfos>(downgrade_candidates->size());

    /// Set callback to execute the "downgrade".
    /// As PriorityGuard::WriteLock allows to only move elements,
    /// but not increment size of any of the queues,
    /// we move elements with zero size and increase the size later in a separate callback.
    res.setAfterEvictWriteFunc([=, this](const CachePriorityGuard::WriteLock & lk) mutable
    {
        for (auto & [key, key_candidates] : *downgrade_candidates)
        {
            while (!key_candidates.candidates.empty())
            {
                auto iterator = key_candidates.candidates.back()->getQueueIterator();
                auto * slru_iterator = assert_cast<SLRUIterator *>(iterator->getNestedOrThis());
                auto entry = slru_iterator->getEntry();
                chassert(entry->size > 0);

                /// Add a new empty queue entry,
                /// save pointers to a new empty entry and old non-empty entry.
                /// Once we have state lock, we will increment size for new entry
                /// and reset size for the old entry,
                /// thus size will be transferred from one entry to another.
                auto empty_entry = std::make_shared<Entry>(entry->key, entry->offset, /* size */0, entry->key_metadata);
                auto new_iterator = probationary_queue.add(std::move(empty_entry), lk, /* state_lock */nullptr);
                downgraded_entries->add(DowngradedEntryInfo{
                    .slru_iterator = iterator,
                    .entry_size = entry->size,
                    .prev_nested_iterator = slru_iterator->lru_iterator,
                    .new_nested_iterator = new_iterator
                });
                key_candidates.candidates.pop_back();
            }
        }
    });

    /// Set incrementing size callback, as explained in the previous comment.
    res.setAfterEvictStateFunc([=, this](const CacheStateGuard::Lock & lk)
    {
        chassert(downgraded_entries->getSize() > 0);
        while (true)
        {
            auto info = downgraded_entries->next();
            if (!info.has_value())
                break;

            auto * iterator = assert_cast<SLRUIterator *>(info->slru_iterator->getNestedOrThis());
            chassert(iterator);
            try
            {
                info->new_nested_iterator.incrementSize(info->entry_size, lk);
            }
            catch (...)
            {
                info->new_nested_iterator.invalidate();
                throw;
            }
            iterator->setIterator(std::move(info->new_nested_iterator), /* is_protected */false, lk);
            info->prev_nested_iterator.invalidate();
            check(lk);
            info->slru_iterator->check(lk);
        }
    });

    LOG_TEST(
        log,
        "Eviction info: {}. "
        "Downgrading {} elements from protected to probationary. "
        "Total size: {}",
        eviction_info.toString(), downgrade_candidates->size(), downgrade_stat.total_stat.releasable_size);

    return true;
}

bool SLRUFileCachePriority::tryIncreasePriority(
    Iterator & iterator_,
    bool is_space_reservation_complete,
    CachePriorityGuard & queue_guard,
    CacheStateGuard & state_guard)
{
    auto & iterator = dynamic_cast<SLRUFileCachePriority::SLRUIterator &>(iterator_);
    chassert(iterator.assertValid());

    /// If entry is already in protected queue,
    /// we only need to increase its priority within the protected queue.
    if (iterator.is_protected)
    {
        return protected_queue.tryIncreasePriority(
            iterator.lru_iterator, is_space_reservation_complete, queue_guard, state_guard);
    }
    else if (!is_space_reservation_complete)
    {
        /// `is_space_reservation_complete` means that file segment is fully downloaded.
        /// This is a limitation of current implementation
        /// that we opt to upgrade only those entries which have already completed space reservation.
        /// Because otherwise it becomes too complex to handle concurrent
        /// space reservation and priority increase (because of the granular locking).
        return probationary_queue.tryIncreasePriority(
            iterator.lru_iterator, is_space_reservation_complete, queue_guard, state_guard);
    }

    chassert(iterator.lru_iterator.cache_priority == &probationary_queue);

    EntryPtr prev_entry = iterator.getEntry();

    {
        auto locked_key = prev_entry->key_metadata->lock();
        const auto entry_state = prev_entry->getState();
        chassert(entry_state == Entry::State::Active || entry_state == Entry::State::Evicting);
        if (entry_state != Entry::State::Active)
            return false;

        /// As we are in progress now of moving this queue entry to a protected queue,
        /// then we need to make sure no one tries to concurrently evict this entry from cache.
        /// So we set "moving flag" to make sure no one touches this entry in the meantime.
        /// And we do not reuse "evicting flag" because we want queries to be able
        /// to use this file segment in the meantime.
        prev_entry->setMovingFlag(*locked_key);
    }

    bool reset_evicting_flag_for_prev_entry = true;
    SCOPE_EXIT({
        if (reset_evicting_flag_for_prev_entry)
            prev_entry->resetFlag(/* from_state */Entry::State::Moving);
    });

    /// Entry is in probationary queue.
    /// Check if there is enough space in protected queue to move entry there.
    /// If not - we need to "downgrade" lowest priority entries from protected
    /// queue to probationary queue.
    std::unique_ptr<EvictionInfo> downgrade_info;
    {
        auto lock = state_guard.lock();
        downgrade_info = protected_queue.collectEvictionInfo(
            prev_entry->size,
            /* elements */1,
            /* reservee */nullptr,
            /* is_total_space_cleanup */false,
            /* is_dynamic_resize */false,
            FileCache::getInternalOrigin(),
            lock);

#ifdef DEBUG_OR_SANITIZER_BUILD
        LOG_TEST(
            log, "Entry: {}. Downgrade info: {} ({})",
            prev_entry->toString(), downgrade_info->toString(), getStateInfoForLog(lock));
#endif
    }

    EvictionCandidates downgrade_candidates;
    FileCacheReserveStat downgrade_stat;
    InvalidatedEntriesInfos invalidated_entries;

    if (!collectCandidatesForEvictionInProtected(
        *downgrade_info,
        downgrade_stat,
        downgrade_candidates,
        invalidated_entries,
        /* reservee */nullptr,
        /* continue_from_last_eviction_pos */false,
        /* max_candidates_size */0,
        /* is_total_space_cleanup */false,
        FileCache::getInternalOrigin(),
        queue_guard,
        state_guard))
    {
        reset_evicting_flag_for_prev_entry = false;
        prev_entry->resetFlag(/* from_state */Entry::State::Moving);

        return probationary_queue.tryIncreasePriority(
            iterator.lru_iterator, is_space_reservation_complete, queue_guard, state_guard);
    }

    downgrade_candidates.evict();

    /// Count how much we evict,
    /// because it could affect performance if we have to do this often.
    ProfileEvents::increment(
        ProfileEvents::FilesystemCacheEvictedFileSegmentsDuringPriorityIncrease,
        downgrade_candidates.size());

    auto new_iterator = [&]{
        auto lock = queue_guard.writeLock();
        downgrade_candidates.afterEvictWrite(lock);
        removeEntries(invalidated_entries, lock);

        auto empty_entry = std::make_shared<Entry>(
            prev_entry->key,
            prev_entry->offset,
            /* size */0,
            prev_entry->key_metadata);

        return protected_queue.add(
            std::move(empty_entry), lock, /* state_lock */nullptr);
    }();

    auto prev_iterator = iterator.lru_iterator;
    {
        auto lock = state_guard.lock();
        try
        {
            downgrade_info->releaseHoldSpace(lock);
            downgrade_candidates.afterEvictState(lock);
            new_iterator.incrementSize(prev_entry->size, lock);
        }
        catch (...)
        {
            new_iterator.invalidate();
            throw;
        }
        iterator.setIterator(std::move(new_iterator), /* is_protected */true, lock);
        prev_iterator.invalidate();
        reset_evicting_flag_for_prev_entry = false;
        check(lock);
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
    auto res = probationary_queue.dump(lock);
    auto part_res = protected_queue.dump(lock);
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

void SLRUFileCachePriority::SLRUIterator::setIterator(
    LRUIterator && iterator_,
    bool is_protected_,
    const CacheStateGuard::Lock &)
{
    auto new_entry = iterator_.getEntry();
    chassert(new_entry->size > 0);

    lru_iterator = iterator_;
    is_protected = is_protected_;

    std::lock_guard lock(entry_mutex);
    entry = new_entry;
}

void SLRUFileCachePriority::SLRUIterator::incrementSize(size_t size, const CacheStateGuard::Lock & lock)
{
    assertValid();
    lru_iterator.incrementSize(size, lock);
    check(lock);
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

bool SLRUFileCachePriority::SLRUIterator::isValid(const CachePriorityGuard::WriteLock & lock) const
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
