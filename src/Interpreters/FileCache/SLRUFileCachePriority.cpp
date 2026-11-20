#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Interpreters/FileCache/SLRUFileCachePriority.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/EvictionCandidates.h>
#include <base/scope_guard.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>
#include <Common/FailPoint.h>
#include <Common/randomSeed.h>
#include <random>


namespace ProfileEvents
{
    extern const Event FilesystemCacheDowngradedFileSegments;
    extern const Event FilesystemCachePromotedFileSegments;
}
namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char file_cache_slru_downgrade_fail_before_finalize[];
    extern const char file_cache_slru_pause_promotion_before_downgrade[];
    extern const char file_cache_modify_size_limits_fail[];
}

namespace
{
    size_t getRatio(size_t total, double ratio, bool ceil = false)
    {
        if (ceil)
            return static_cast<size_t>(std::ceil(static_cast<double>(total) * std::clamp(ratio, 0.0, 1.0)));
        return std::lround(static_cast<double>(total) * std::clamp(ratio, 0.0, 1.0));
    }

    size_t excess(size_t a, size_t b) { return a > b ? a - b : 0; }

    /// The two shares add up to the total.
    size_t getProbationaryShare(size_t total, double ratio) { return total - getRatio(total, ratio); }
}

SLRUFileCachePriority::SLRUFileCachePriority(
    QueueType queue_type_,
    size_t max_size_,
    size_t max_elements_,
    double size_ratio_,
    const std::string & description_,
    LRUFileCachePriority::StatePtr probationary_state_,
    LRUFileCachePriority::StatePtr protected_state_,
    bool lend_unused_protected_capacity_)
    : IFileCachePriority(queue_type_, max_size_, max_elements_)
    , description(description_)
    , size_ratio(size_ratio_)
    , lend_unused_protected_capacity(lend_unused_protected_capacity_)
    , protected_queue(LRUFileCachePriority(queue_type_,
                                           getRatio(max_size_, size_ratio),
                                           getRatio(max_elements_, size_ratio),
                                           description_ + ", protected",
                                           protected_state_))
    , probationary_queue(LRUFileCachePriority(queue_type_,
                                              getProbationaryShare(max_size_, size_ratio),
                                              getProbationaryShare(max_elements_, size_ratio),
                                              description_ + ", probationary",
                                              probationary_state_))
    , log(getLogger("SLRUFileCachePriority(" + description_ + ")"))
    , take_back_queue_id(randomSeed())
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

    /// Both sub-queues share the SLRU's single structural guard: entries move between the two
    /// sub-queues, so they cannot be protected by independent locks.
    auto & guard = getPriorityGuard();
    protected_queue.setPriorityGuard(guard);
    probationary_queue.setPriorityGuard(guard);
}

size_t SLRUFileCachePriority::getLendableSize(const CacheStateGuard::Lock & lock) const
{
    if (!lend_unused_protected_capacity)
        return 0;
    /// Keep the protected limit non-zero: zero means unlimited.
    const size_t used = std::max<size_t>(protected_queue.getSize(lock), 1);
    const size_t limit = protected_queue.max_size;
    return limit > used ? limit - used : 0;
}

size_t SLRUFileCachePriority::getLendableElements(const CacheStateGuard::Lock & lock) const
{
    if (!lend_unused_protected_capacity)
        return 0;
    const size_t used = std::max<size_t>(protected_queue.getElementsCount(lock), 1);
    const size_t limit = protected_queue.max_elements;
    return limit > used ? limit - used : 0;
}

size_t SLRUFileCachePriority::getLentSize(const CacheStateGuard::Lock &) const
{
    return excess(excess(getRatio(max_size, size_ratio), protected_queue.max_size), pending_take_back_size);
}

size_t SLRUFileCachePriority::getLentElements(const CacheStateGuard::Lock &) const
{
    const size_t share = getRatio(max_elements, size_ratio);
    const size_t limit = protected_queue.max_elements;
    return share > limit ? share - limit : 0;
}

bool SLRUFileCachePriority::canFitInProbationary(size_t size, size_t elements, const CacheStateGuard::Lock & lock) const
{
    return probationary_queue.getSize(lock) + size <= probationary_queue.max_size + getLendableSize(lock)
        && probationary_queue.getElementsCount(lock) + elements <= probationary_queue.max_elements + getLendableElements(lock);
}

void SLRUFileCachePriority::lendProtectedCapacity(size_t size, size_t elements, const CacheStateGuard::Lock & lock)
{
    const size_t probationary_size = probationary_queue.getSize(lock);
    const size_t probationary_max_size = probationary_queue.max_size;
    if (size && probationary_size + size > probationary_max_size)
    {
        const size_t lend = std::min(probationary_size + size - probationary_max_size, getLendableSize(lock));
        protected_queue.max_size -= lend;
        probationary_queue.max_size += lend;
    }

    const size_t probationary_elements = probationary_queue.getElementsCount(lock);
    const size_t probationary_max_elements = probationary_queue.max_elements;
    if (elements && probationary_elements + elements > probationary_max_elements)
    {
        const size_t lend = std::min(probationary_elements + elements - probationary_max_elements, getLendableElements(lock));
        protected_queue.max_elements -= lend;
        probationary_queue.max_elements += lend;
    }
}

void SLRUFileCachePriority::takeBackLentCapacity(size_t size, const CacheStateGuard::Lock & lock)
{
    const size_t take = std::min({
        excess(protected_queue.getSize(lock) + size, protected_queue.max_size),
        getLentSize(lock),
        excess(probationary_queue.max_size, probationary_queue.getSize(lock))});
    probationary_queue.max_size -= take;
    protected_queue.max_size += take;
}

FileCachePriorityPtr SLRUFileCachePriority::copy() const
{
    chassert(!lend_unused_protected_capacity);
    return std::make_unique<SLRUFileCachePriority>(
        getQueueType(), max_size, max_elements, size_ratio, description, probationary_queue.state, protected_queue.state,
        /* lend_unused_protected_capacity */ false);
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
    bool is_initial_load) const
{
    if (is_initial_load)
        return probationary_queue.canFit(size, elements, lock) || protected_queue.canFit(size, elements, lock);

    if (reservee)
    {
        const auto * slru_iterator = assert_cast<SLRUIterator *>(reservee->getNestedOrThis());
        if (slru_iterator->is_protected)
            return protected_queue.canFit(size, elements, lock);
        return canFitInProbationary(size, elements, lock);
    }
    return canFitInProbationary(size, elements, lock);
}

IFileCachePriority::IteratorPtr SLRUFileCachePriority::add( /// NOLINT
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const CacheStateGuard::Lock * state_lock,
    bool is_initial_load)
{
    bool is_protected = false;
    if (is_initial_load)
    {
        chassert(size);
        if (!state_lock)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Startup initialization requires state lock");

        /// If it is server startup, we put entries in any queue it will fit in,
        /// but with preference for probationary queue,
        /// because we do not know the distribution between queues after server restart.
        /// Use the disk-accounted size for the fit check, because that is the size the
        /// `Entry` will occupy in the subqueue (with `use_real_disk_size` the `Entry`
        /// constructor aligns the size to the filesystem block size).
        const size_t accounted_size = key_metadata->useRealDiskSize() ? key_metadata->alignFileSize(size) : size;
        is_protected = !probationary_queue.canFit(accounted_size, /* elements */1, *state_lock);
    }
    else if (size && !state_lock)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Adding non-zero size entry without state lock "
            "(key: {}, offset: {})", key_metadata->key, offset);
    }

    auto lock = getPriorityGuard().writeLock();
    auto entry = std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata);
    return std::make_shared<SLRUIterator>(
        this,
        is_protected
            ? protected_queue.add(std::move(entry), lock, state_lock)
            : probationary_queue.add(std::move(entry), lock, state_lock),
        is_protected);
}

IFileCachePriority::IteratorPtr SLRUFileCachePriority::addForRestore( /// NOLINT
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    QueueEntryType original_queue_type,
    const CachePriorityGuard::WriteLock & lock,
    const CacheStateGuard::Lock * state_lock)
{
    /// Restore to the original queue: protected entries go back to protected,
    /// everything else goes to probationary.
    bool is_protected = (original_queue_type == QueueEntryType::SLRU_Protected);

    /// The queue limits could have moved since the entry was removed (see `lendProtectedCapacity`),
    /// so an entry which does not fit its original queue goes to probationary, which can borrow.
    if (state_lock && size)
    {
        const size_t accounted_size = key_metadata->useRealDiskSize() ? key_metadata->alignFileSize(size) : size;
        if (is_protected && !protected_queue.canFit(accounted_size, /* elements */1, *state_lock))
            is_protected = false;
        if (!is_protected)
            lendProtectedCapacity(accounted_size, /* elements */1, *state_lock);
    }

    auto entry = std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata);
    return std::make_shared<SLRUIterator>(
        this,
        is_protected
            ? protected_queue.add(std::move(entry), lock, state_lock)
            : probationary_queue.add(std::move(entry), lock, state_lock),
        is_protected);
}

void SLRUFileCachePriority::iterate(IterateFunc func, FileCacheReserveStat & stat)
{
    auto lock = getPriorityGuard().readLock();
    protected_queue.iterateImpl(protected_queue.queue.begin(), func, stat, lock);
    probationary_queue.iterateImpl(probationary_queue.queue.begin(), func, stat, lock);
}

void SLRUFileCachePriority::resetEvictionPos(EvictionCursor cursor)
{
    protected_queue.resetEvictionPos(cursor);
    probationary_queue.resetEvictionPos(cursor);
}

EvictionInfoPtr SLRUFileCachePriority::collectEvictionInfo( /// NOLINT
    size_t size,
    size_t elements,
    IFileCachePriority::Iterator * reservee,
    bool is_total_space_cleanup,
    const OriginInfo & origin_info,
    const CacheStateGuard::Lock & lock,
    QueueEntryType reservee_queue_type)
{
    if (!size && !elements)
    {
        /// Create empty target EvictionInfo for each subqueue.
        auto info = probationary_queue.collectEvictionInfo(0, 0, reservee, is_total_space_cleanup, origin_info, lock);
        info->add(protected_queue.collectEvictionInfo(0, 0, reservee, is_total_space_cleanup, origin_info, lock));
        return info;
    }

    /// Total space cleanup is for keep_free_space_size(elements)_ratio feature.
    if (is_total_space_cleanup)
    {
        /// Remove everything from probationary first
        /// and only if it's empty - remove from protected as well.
        size_t evict_size_from_probationary = std::min(size, probationary_queue.getSize(lock));
        size_t evict_elements_from_probationary = std::min(elements, probationary_queue.getElementsCount(lock));

        /// Probationary may be empty while protected still has entries (everything got promoted);
        /// then we pass zeroes to probationary and route the full request to protected.
        size -= evict_size_from_probationary;
        elements -= evict_elements_from_probationary;

        auto info = probationary_queue.collectEvictionInfo(
            evict_size_from_probationary,
            evict_elements_from_probationary,
            reservee,
            is_total_space_cleanup,
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
    else
        evict_in_protected = reservee_queue_type == QueueEntryType::SLRU_Protected;

    if (evict_in_protected)
    {
        /// If protected queue required eviction, we need to "downgrade"
        /// its eviction candidates into probationary queue
        /// (to make sure we have space in probationary queue for the downgrade).
        /// But we cannot do it here, as we do not know in advance the exact size to downgrade.
        /// So we will do it in collectCandidatesForEviction.
        /// A protected entry which grows takes lent capacity back: probationary makes room for it (hold and eviction, as for
        /// any probationary reservation), and the limit moves when the entry grows (see `SLRUIterator::incrementSize`).
        const size_t take_back = std::min(
            excess(size, excess(protected_queue.max_size, protected_queue.getSize(lock))), getLentSize(lock));
        auto info = protected_queue.collectEvictionInfo(
            size - take_back, elements, reservee, is_total_space_cleanup, origin_info, lock);
        if (take_back)
        {
            info->add(probationary_queue.collectEvictionInfo(
                take_back, /* elements */0, /* reservee */nullptr, is_total_space_cleanup, origin_info, lock));
            auto pending = std::make_unique<QueueEvictionInfo>(description, origin_info.user_id);
            pending->hold_space = std::make_unique<HoldSpace>(take_back, /* elements */0, *this, lock);
            info->add(std::make_unique<EvictionInfo>(take_back_queue_id, std::move(pending)));
        }
        return info;
    }

    /// Do not evict from probationary while the protected queue has unused capacity.
    lendProtectedCapacity(size, elements, lock);
    return probationary_queue.collectEvictionInfo(
        size, elements, reservee, is_total_space_cleanup, origin_info, lock);
}

bool SLRUFileCachePriority::collectCandidatesForEviction( /// NOLINT
    EvictionInfo & eviction_info,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    IFileCachePriority::IteratorPtr reservee,
    EvictionCursor eviction_cursor,
    size_t max_candidates_size,
    bool is_total_space_cleanup,
    const OriginInfo & origin_info,
    CacheStateGuard & state_guard,
    QueueEntryType reservee_queue_type)
{
    if (!eviction_info.requiresEviction())
        return true;

    if (is_total_space_cleanup)
    {
        /// Use per-queue local stat objects so that each sub-queue's
        /// stopping condition sees only its own accumulated releasable bytes,
        /// not the cumulative total from both passes.
        FileCacheReserveStat probationary_stat;
        bool success_probationary = probationary_queue.collectCandidatesForEviction(
            eviction_info,
            probationary_stat,
            res,
            reservee,
            eviction_cursor,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            state_guard);
        /// We do not quit here if !success_probationary,
        /// because in case of keep_up_free_space_ratio it is ok to evict at least something
        /// (so we will check res.size() instead of returned bool value).

        /// We do not use collectCandidatesForEvictionInProtected method,
        /// because it will "downgrade" instead of remove,
        /// but for total space cleanup we need remove.
        FileCacheReserveStat protected_stat;
        bool success_protected = protected_queue.collectCandidatesForEviction(
            eviction_info,
            protected_stat,
            res,
            reservee,
            eviction_cursor,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            state_guard);

        stat += probationary_stat;
        stat += protected_stat;

        return success_probationary && success_protected;
    }

    /// If `reservee` is nullptr, then either it is the first space reservation attempt for a
    /// corresponding file segment (so it will be directly put into probationary queue), or the
    /// caller could not hand over the reservee and named its sub-queue instead.
    if (!reservee && reservee_queue_type != QueueEntryType::SLRU_Protected)
    {
        return probationary_queue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            reservee,
            eviction_cursor,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            state_guard);
    }

    bool evict_in_protected = !reservee;
    if (reservee)
    {
        auto * slru_iterator = assert_cast<SLRUIterator *>(reservee->getNestedOrThis());
        evict_in_protected = slru_iterator->is_protected;
        chassert(evict_in_protected
                 ? slru_iterator->lru_iterator.cache_priority == &protected_queue
                 : slru_iterator->lru_iterator.cache_priority == &probationary_queue);
    }

    bool success = false;

    /// If `reservee` is not nullptr (e.g. is already in some queue),
    /// we need to check in which queue (protected/probationary) it currently is
    /// (in order to know where we need to free space).
    if (evict_in_protected)
    {
        /// Entry is in protected queue.
        /// Check if we have enough space in protected queue to fit a new size of entry.
        success = collectCandidatesForEvictionInProtected(
            eviction_info,
            stat,
            res,
            reservee,
            eviction_cursor,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            state_guard,
            /* wait_for_priority_lock */true);
    }
    else
    {
        success = probationary_queue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            reservee,
            eviction_cursor,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            state_guard);
    }

    return success;
}

/// TODO: currently this will find only releasable entries,
/// but since we are only downgrading, then it does not matter.
bool SLRUFileCachePriority::collectCandidatesForEvictionInProtected(
    EvictionInfo & eviction_info,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    IFileCachePriority::IteratorPtr reservee,
    EvictionCursor eviction_cursor,
    size_t max_candidates_size,
    bool is_total_space_cleanup,
    const OriginInfo & origin_info,
    CacheStateGuard & state_guard,
    bool wait_for_priority_lock)
{
    auto collect = [&](LRUFileCachePriority & queue, FileCacheReserveStat & queue_stat, EvictionCandidates & candidates)
    {
        if (wait_for_priority_lock)
        {
            return queue.collectCandidatesForEviction(
                eviction_info,
                queue_stat,
                candidates,
                reservee,
                eviction_cursor,
                max_candidates_size,
                is_total_space_cleanup,
                origin_info,
                state_guard);
        }
        return queue.tryCollectCandidatesForEviction(
            eviction_info, queue_stat, candidates, eviction_cursor, max_candidates_size);
    };

    /// Probationary eviction which a growing protected entry asked for (see `collectEvictionInfo`); 0 for promotions.
    size_t probationary_size_to_evict = 0;
    if (auto it = eviction_info.find(probationary_queue.queue_id); it != eviction_info.end())
        probationary_size_to_evict = it->second->size_to_evict;

    /// No eviction callback: these candidates are only downgraded (moved), never evicted.
    auto downgrade_candidates = std::make_shared<EvictionCandidates>(IFileCachePriority::OnEvictCallback{});
    FileCacheReserveStat downgrade_stat;
    if (!collect(protected_queue, downgrade_stat, *downgrade_candidates))
        return false;

    /// We can have no downgrade candidates because cache size could
    /// reduce concurrently because of lock-free cache entries invalidation.
    if (downgrade_candidates->size() == 0)
    {
        return !probationary_size_to_evict || collect(probationary_queue, stat, res);
    }

    /// We did not collect eviction info for probationary queue in advance
    /// (when doing so for protected queue),
    /// because we could not know how much space we will need to downgrade
    /// (because downgraded space >= space to reserve in protected).
    /// So we do it now.
    auto probationary_eviction_info = probationary_queue.collectEvictionInfo(
        downgrade_stat.total_stat.releasable_size + probationary_size_to_evict,
        downgrade_stat.total_stat.releasable_count,
        /* reservee */nullptr,
        /* is_total_space_cleanup */false,
        origin_info,
        state_guard.lock());

    const bool requires_eviction = probationary_eviction_info->requiresEviction();
    eviction_info.addOrUpdate(std::move(probationary_eviction_info));
    if (requires_eviction)
    {
        /// If not enough space - we need to "downgrade" lowest priority entries
        /// from protected queue to probationary queue,
        /// so collect eviction candidates in probationary now.
        if (!collect(probationary_queue, stat, res))
            return false;
    }

    struct DowngradedEntryInfo
    {
        IteratorPtr slru_iterator;
        /// Entry size as it was in protected queue.
        size_t entry_size = 0;
        /// Previous entry/iterator in protected queue.
        /// The strong `prev_entry` lets rollback reset the flag without the throwing `getEntry`.
        EntryPtr prev_entry;
        LRUIterator prev_nested_iterator;
        /// New iterator to entry in probationary queue.
        LRUIterator new_nested_iterator;
        bool rollbacked = false;

        void rollbackState() noexcept
        {
            chassert(!rollbacked, "State is already rollbacked");
            if (rollbacked)
                return;

            /// Invalidate the new probationary `PreActive` entry, and
            /// reset the old protected entry's `Evicting` flag back to `Active`.
            new_nested_iterator.invalidate();
            prev_entry->resetFlag(Entry::State::Evicting);
            rollbacked = true;
        }
    };
    /// RAII wrapper to protect against the case when afterEvictState callback
    /// is not called because of some unexpected exception.
    struct DowngradedEntriesInfos : private std::vector<DowngradedEntryInfo>
    {
        explicit DowngradedEntriesInfos(size_t size) { reserve(size); }

        void add(DowngradedEntryInfo && info) { push_back(info); }

        [[nodiscard]] [[maybe_unused]] size_t getSize() const { return size(); }

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
            /// Roll back unfinalized downgrades. `rollbackState` is noexcept, so one
            /// entry cannot abort the loop or escape the destructor.
            for (auto & entry : *this)
                entry.rollbackState();
        }
    };
    auto downgraded_entries = std::make_shared<DowngradedEntriesInfos>(downgrade_candidates->size());

    /// Set callback to execute the "downgrade".
    /// As PriorityGuard::WriteLock allows to only move elements,
    /// but not increment size of any of the queues,
    /// we move elements with zero size and increase the size later in a separate callback.
    res.addAfterEvictWriteCallback(&getPriorityGuard(), [=, this](const CachePriorityGuard::WriteLock & lk) mutable
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
                /// PreActive: iterateImpl skips this entry until setIterator atomically transitions it to Active.
                auto empty_entry = std::make_shared<Entry>(entry->key, entry->offset, /* size */0, entry->getKeyMetadata(), Entry::State::PreActive);
                auto new_iterator = probationary_queue.add(std::move(empty_entry), lk, /* state_lock */nullptr);
                downgraded_entries->add(DowngradedEntryInfo{
                    .slru_iterator = iterator,
                    .entry_size = entry->size,
                    .prev_entry = entry,
                    .prev_nested_iterator = slru_iterator->lru_iterator,
                    .new_nested_iterator = new_iterator
                });
                key_candidates.candidates.pop_back();
            }
        }
    });

    /// Set incrementing size callback, as explained in the previous comment.
    res.addAfterEvictStateCallback([=, this](const CacheStateGuard::Lock & lk)
    {
        fiu_do_on(FailPoints::file_cache_slru_downgrade_fail_before_finalize,
        {
            static constexpr double fault_probability = 0.001;
            if (std::bernoulli_distribution(fault_probability)(thread_local_rng))
                throw Exception(ErrorCodes::FAULT_INJECTED,
                                "Injected fault before SLRU downgrade finalization");
        });

        chassert(downgraded_entries->getSize() > 0);
        ProfileEvents::increment(ProfileEvents::FilesystemCacheDowngradedFileSegments, downgraded_entries->getSize());
        while (true)
        {
            auto info = downgraded_entries->next();
            if (!info.has_value())
                break;

            SLRUIterator * iterator = nullptr;
            try
            {
                iterator = assert_cast<SLRUIterator *>(info->slru_iterator->getNestedOrThis());
                chassert(iterator);
                info->new_nested_iterator.incrementSize(info->entry_size, lk);
            }
            catch (...)
            {
                /// This entry was already popped off `downgraded_entries`
                /// (in downgraded_entries->next()), so its destructor will not roll it back.
                info->rollbackState();
                throw;
            }
            iterator->setIterator(std::move(info->new_nested_iterator), /* is_protected */false, lk);
            info->prev_nested_iterator.invalidate();
            check(lk);
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
    CacheStateGuard & state_guard)
{
    auto & iterator = dynamic_cast<SLRUFileCachePriority::SLRUIterator &>(iterator_);
    chassert(iterator.assertValid());

    /// If entry is already in protected queue,
    /// we only need to increase its priority within the protected queue.
    if (iterator.is_protected)
    {
        return protected_queue.tryIncreasePriority(
            iterator.lru_iterator, is_space_reservation_complete, state_guard);
    }
    else if (!is_space_reservation_complete)
    {
        /// The move must not run concurrently with a space reservation for this entry: the reservation
        /// holds space and collects eviction candidates in the queue where the entry was when it started.
        /// So a file segment which can be downloaded concurrently only gets a higher priority in
        /// probationary (see `FileSegment::increasePriority`).
        return probationary_queue.tryIncreasePriority(
            iterator.lru_iterator, is_space_reservation_complete, state_guard);
    }

    chassert(iterator.lru_iterator.cache_priority == &probationary_queue);

    EntryPtr prev_entry = iterator.getEntry();

    {
        auto locked_key = prev_entry->getKeyMetadata()->lock();
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
    /// Capacity lent to probationary comes back with the entry; only the rest of the entry needs downgrades.
    size_t take_back_size = 0;
    size_t take_back_elements = 0;
    std::unique_ptr<EvictionInfo> downgrade_info;
    {
        auto lock = state_guard.lock();
        take_back_size = std::min<size_t>(prev_entry->size, getLentSize(lock));
        take_back_elements = std::min<size_t>(1, getLentElements(lock));
        downgrade_info = protected_queue.collectEvictionInfo(
            prev_entry->size - take_back_size,
            /* elements */1 - take_back_elements,
            /* reservee */nullptr,
            /* is_total_space_cleanup */false,
            FileCache::getInternalOrigin(),
            lock);

#ifdef DEBUG_OR_SANITIZER_BUILD
        LOG_TEST(
            log, "Entry: {}. Downgrade info: {} ({})",
            prev_entry->toString(), downgrade_info->toString(), getStateInfoForLog(lock));
#endif
    }

    /// Declared before `eviction_candidates`: its write callback refers to it.
    std::optional<LRUIterator> new_iterator;

    /// Holds the real probationary evictions that free room for the downgraded
    /// protected entries (empty if probationary already has room). The downgrade
    /// move itself is committed in the afterEvict* callbacks.
    EvictionCandidates eviction_candidates(getOnEvictCallback());
    FileCacheReserveStat downgrade_stat;

    SharedEvictionCursor::ActiveEvictor promotion_evictor(promotion_cursor);

    FailPointInjection::pauseFailPoint(FailPoints::file_cache_slru_pause_promotion_before_downgrade);

    /// Best-effort, as in `LRUFileCachePriority::tryIncreasePriority`: a writer waiting on this
    /// writer-preferring guard stalls every reader of the cache, so the walks give up instead of
    /// queueing behind one, and the hit then at most bumps the entry within probationary.
    if (!collectCandidatesForEvictionInProtected(
        *downgrade_info,
        downgrade_stat,
        eviction_candidates,
        /* reservee */nullptr,
        promotion_cursor.getCursor(*this),
        /* max_candidates_size */0,
        /* is_total_space_cleanup */false,
        FileCache::getInternalOrigin(),
        state_guard,
        /* wait_for_priority_lock */false))
    {
        reset_evicting_flag_for_prev_entry = false;
        prev_entry->resetFlag(/* from_state */Entry::State::Moving);

        return probationary_queue.tryIncreasePriority(
            iterator.lru_iterator, is_space_reservation_complete, state_guard);
    }

    /// Added under the same write lock as the downgrades, so the finalization takes it once.
    eviction_candidates.addAfterEvictWriteCallback(&getPriorityGuard(), [&](const CachePriorityGuard::WriteLock & lock)
    {
        /// PreActive: iterateImpl skips this entry until setIterator atomically transitions it to Active.
        /// prev_entry is in Moving state here, so its KeyMetadata is still alive.
        auto empty_entry = std::make_shared<Entry>(
            prev_entry->key,
            prev_entry->offset,
            /* size */0,
            prev_entry->getKeyMetadata(),
            Entry::State::PreActive);

        new_iterator.emplace(protected_queue.add(std::move(empty_entry), lock, /* state_lock */nullptr));
    });

    eviction_candidates.evict();
    eviction_candidates.afterEvictWrite();
    chassert(new_iterator.has_value());

    auto prev_iterator = iterator.lru_iterator;
    {
        auto lock = state_guard.lock();
        size_t taken_size = 0;
        size_t taken_elements = 0;
        try
        {
            downgrade_info->releaseHoldSpace(lock);
            eviction_candidates.afterEvictState(lock);
            if (take_back_size || take_back_elements)
            {
                /// Lent capacity could have been taken back concurrently.
                taken_size = std::min(take_back_size, getLentSize(lock));
                taken_elements = std::min(take_back_elements, getLentElements(lock));
                if (!protected_queue.canFit(prev_entry->size - taken_size, /* elements */1 - taken_elements, lock))
                {
                    new_iterator->invalidate();
                    return false;
                }
                protected_queue.max_size += taken_size;
                protected_queue.max_elements += taken_elements;
            }
            new_iterator->incrementSize(prev_entry->size, lock);
        }
        catch (...)
        {
            protected_queue.max_size -= taken_size;
            protected_queue.max_elements -= taken_elements;
            new_iterator->invalidate();
            throw;
        }
        iterator.setIterator(std::move(*new_iterator), /* is_protected */true, lock);
        prev_iterator.invalidate();
        /// The entry has left the probationary queue, so it can return the capacity.
        probationary_queue.max_size -= taken_size;
        probationary_queue.max_elements -= taken_elements;
        reset_evicting_flag_for_prev_entry = false;
        check(lock);
    }

    ProfileEvents::increment(ProfileEvents::FilesystemCachePromotedFileSegments);
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
            auto metadata = entry->getKeyMetadata()->tryLock();
            if (!metadata)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot lock key metadata to clean up file segment {}:{}", entry->key, entry->offset);

            auto segment_metadata = metadata->tryGetByOffset(entry->offset);
            metadata->removeFileSegment(entry->offset, segment_metadata->file_segment->lock());
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

IFileCachePriority::PriorityDumpPtr SLRUFileCachePriority::dump()
{
    auto res = probationary_queue.dump();
    auto part_res = protected_queue.dump();
    res->merge(*part_res);
    return res;
}

void SLRUFileCachePriority::shuffle()
{
    protected_queue.shuffle();
    probationary_queue.shuffle();
}

bool SLRUFileCachePriority::modifySizeLimits(
    size_t max_size_, size_t max_elements_, double size_ratio_, const CacheStateGuard::Lock & lock)
{
    if (max_size == max_size_ && max_elements == max_elements_ && size_ratio == size_ratio_)
        return false; /// Nothing to change.

    const size_t prev_protected_size = protected_queue.getSizeLimit(lock);
    const size_t prev_protected_elements = protected_queue.getElementsLimit(lock);

    const size_t protected_share = getRatio(max_size_, size_ratio_);
    const size_t protected_elements_share = getRatio(max_elements_, size_ratio_);
    const size_t probationary_share = getProbationaryShare(max_size_, size_ratio_);
    const size_t probationary_elements_share = getProbationaryShare(max_elements_, size_ratio_);
    /// Keep lending the protected capacity which probationary uses above its new share.
    size_t lent_size = 0;
    size_t lent_elements = 0;
    if (lend_unused_protected_capacity)
    {
        lent_size = std::min(
            excess(probationary_queue.getSize(lock), probationary_share),
            excess(protected_share, std::max<size_t>(protected_queue.getSize(lock), 1)));
        lent_elements = std::min(
            excess(probationary_queue.getElementsCount(lock), probationary_elements_share),
            excess(protected_elements_share, std::max<size_t>(protected_queue.getElementsCount(lock), 1)));
    }

    protected_queue.modifySizeLimits(protected_share - lent_size, protected_elements_share - lent_elements, 0, lock);

    try
    {
        fiu_do_on(FailPoints::file_cache_modify_size_limits_fail,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected fault in modifySizeLimits");
        });
        probationary_queue.modifySizeLimits(
            probationary_share + lent_size, probationary_elements_share + lent_elements, 0, lock);
    }
    catch (...)
    {
        protected_queue.modifySizeLimits(prev_protected_size, prev_protected_elements, 0, lock);
        throw;
    }

    max_size = max_size_;
    max_elements = max_elements_;
    size_ratio = size_ratio_;
    return true;
}

EvictionInfoPtr SLRUFileCachePriority::collectEvictionInfoForResize(
    size_t desired_max_size,
    size_t desired_max_elements,
    const IFileCachePriority::OriginInfo & origin_info,
    const CacheStateGuard::Lock & lock)
{
    /// Delegate to each sub-queue's collectEvictionInfoForResize with per-sub-queue
    /// desired limits derived from the desired total and ratio.
    /// This is needed because the total cache size might be under the desired total,
    /// but one sub-queue (e.g. protected) might exceed its new sub-limit.

    const size_t protected_share = getRatio(desired_max_size, size_ratio);
    const size_t protected_elements_share = getRatio(desired_max_elements, size_ratio);
    auto info = protected_queue.collectEvictionInfoForResize(protected_share, protected_elements_share, origin_info, lock);

    size_t probationary_limit = getProbationaryShare(desired_max_size, size_ratio);
    size_t probationary_elements_limit = getProbationaryShare(desired_max_elements, size_ratio);
    if (lend_unused_protected_capacity)
    {
        /// Probationary keeps what fits into the protected share that protected does not use after the resize.
        probationary_limit += excess(
            protected_share, std::max<size_t>(std::min(protected_queue.getSize(lock), protected_share), 1));
        probationary_elements_limit += excess(
            protected_elements_share,
            std::max<size_t>(std::min(protected_queue.getElementsCount(lock), protected_elements_share), 1));
    }
    info->add(probationary_queue.collectEvictionInfoForResize(probationary_limit, probationary_elements_limit, origin_info, lock));

    return info;
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
    const CacheStateGuard::Lock & state_lock)
{
    auto new_entry = iterator_.getEntry();
    chassert(new_entry->size > 0);

    lru_iterator = iterator_;
    is_protected = is_protected_;

    std::lock_guard lock(entry_mutex);
    entry = new_entry;
    /// Atomically activate the new entry together with the pointer update.
    /// `iterateImpl` reads entry state without holding `entry_mutex`, so it sees either:
    ///   - PreActive  → skips the entry (not evictable yet), or
    ///   - Active     → calls getEntry() under entry_mutex and is guaranteed to see new_entry.
    /// This eliminates the race where iterateImpl finds new_entry as evictable but
    /// getEntry() still returns the previous queue entry, causing setEvictingFlag
    /// to be called on an entry that is not in Active state.
    new_entry->setActiveFlag(state_lock);
}

void SLRUFileCachePriority::SLRUIterator::incrementSize(size_t size, const CacheStateGuard::Lock & lock)
{
    assertValid();
    if (is_protected)
        cache_priority->takeBackLentCapacity(size, lock);
    lru_iterator.incrementSize(size, lock);
    check(lock);
}

void SLRUFileCachePriority::SLRUIterator::decrementSize(size_t size)
{
    assertValid();
    lru_iterator.decrementSize(size);
}

void SLRUFileCachePriority::SLRUIterator::invalidate() noexcept
{
    assertValid();
    lru_iterator.invalidate();
}

void SLRUFileCachePriority::SLRUIterator::invalidateBeforeRemove(const CachePriorityGuard::WriteLock & lock) noexcept
{
    assertValid();
    lru_iterator.invalidateBeforeRemove(lock);
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

void SLRUFileCachePriority::SLRUIterator::remove()
{
    auto lock = cache_priority->getPriorityGuard().writeLock();
    remove(lock);
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
