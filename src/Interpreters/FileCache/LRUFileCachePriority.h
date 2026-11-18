#pragma once

#include <deque>
#include <list>
#include <mutex>
#include <optional>
#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Interpreters/FileCache/CacheUsage.h>
#include <Common/logger_useful.h>
#include <Interpreters/FileCache/Guards.h>

class FileCacheTest_MoveEvictionPos_Test;

namespace DB
{

/// Based on the LRU algorithm implementation, the record with the lowest priority is stored at
/// the head of the queue, and the record with the highest priority is stored at the tail.
class LRUFileCachePriority : public IFileCachePriority
{
protected:
    class State
    {
    public:
        explicit State(LoggerPtr log_) : log(log_) {}

        size_t getSize(const CacheStateGuard::Lock &) const { return size; }
        size_t getSizeApprox() const { return size.load(std::memory_order_relaxed); }

        size_t getElementsCount(const CacheStateGuard::Lock &) const { return elements_num; }
        size_t getElementsCountApprox() const { return elements_num.load(std::memory_order_relaxed); }

        void add(uint64_t size_, uint64_t elements_, const CacheStateGuard::Lock &);
        void sub(uint64_t size_, uint64_t elements_);

    private:
        std::atomic<size_t> size = 0;
        std::atomic<size_t> elements_num = 0;
        LoggerPtr log;
    };
    using StatePtr = std::shared_ptr<State>;

public:
    LRUFileCachePriority(
        QueueType queue_type_,
        size_t max_size_,
        size_t max_elements_,
        const std::string & description_ = "none",
        StatePtr state_ = nullptr);

    ~LRUFileCachePriority() override;

    Type getType() const override { return Type::LRU; }

    size_t getSize(const CacheStateGuard::Lock & lock) const override { return state->getSize(lock); }
    size_t getSizeApprox() const override { return state->getSizeApprox(); }

    size_t getElementsCount(const CacheStateGuard::Lock & lock) const override { return state->getElementsCount(lock); }
    size_t getElementsCountApprox() const override { return state->getElementsCountApprox(); }

    std::string getStateInfoForLog(const CacheStateGuard::Lock & lock) const override;

    EvictionInfoPtr collectEvictionInfo(
        size_t size,
        size_t elements,
        IFileCachePriority::Iterator * reservee,
        bool is_total_space_cleanup,
        const IFileCachePriority::OriginInfo & origin,
        const CacheStateGuard::Lock &) override;

    bool canFit( /// NOLINT
        size_t size,
        size_t elements,
        const CacheStateGuard::Lock &,
        IteratorPtr reservee = nullptr,
        const OriginInfo & origin_info = {},
        bool is_initial_load = false) const override;

    /// Create a queue entry for given key and offset.
    /// Write priority lock is required.
    /// State lock is required only if non-zero size entry is being added.
    /// In most cases, we first add a zero-size queue entry with write priority lock,
    /// then release that lock and take cache state lock
    /// with which we increase size of the newly added zero-size queue entry.
    IteratorPtr add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const CacheStateGuard::Lock *,
        bool is_initial_load = false) override;

    IteratorPtr addForRestore( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        QueueEntryType original_queue_type,
        const CachePriorityGuard::WriteLock & lock,
        const CacheStateGuard::Lock * state_lock) override;

    void sealStructure() { structure_sealed = true; }

    bool collectCandidatesForEviction(
        EvictionInfo & eviction_info,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        EvictionCursor eviction_cursor,
        size_t max_candidates_size,
        bool is_total_space_cleanup,
        const OriginInfo & origin_info,
        CacheStateGuard &) override;

    bool tryIncreasePriority(
        Iterator & iterator,
        bool is_space_reservation_complete,
        CacheStateGuard & state_guard) override;

    void shuffle() override;

    PriorityDumpPtr dump() override;

    bool modifySizeLimits(
        size_t max_size_,
        size_t max_elements_,
        double size_ratio_,
        const CacheStateGuard::Lock &) override;

    EvictionInfoPtr collectEvictionInfoForResize(
        size_t desired_max_size,
        size_t desired_max_elements,
        const OriginInfo & origin_info,
        const CacheStateGuard::Lock & lock) override;

    FileCachePriorityPtr copy() const { return std::make_unique<LRUFileCachePriority>(getQueueType(), max_size, max_elements, description, state); }

    /// See a comment near eviction_pos.
    void resetEvictionPos(EvictionCursor cursor) override
    {
        std::lock_guard lock(eviction_pos_mutex);
        evictionPos(cursor) = LRUQueue::iterator{};
    }

    /// Used only for unit test.
    size_t getEvictionPosCount(EvictionCursor cursor)
    {
        std::lock_guard lock(eviction_pos_mutex);
        if (evictionPos(cursor) == LRUQueue::iterator{})
            return 0;
        return std::distance(queue.begin(), evictionPos(cursor));
    }

protected:
    /// By default a priority locks its own `priority_guard`. SLRU redirects its two sub-queues
    /// (via `setPriorityGuard`) to share the SLRU's guard, so all its operations serialize on one guard.
    CachePriorityGuard & getPriorityGuard() const override
    {
        return effective_priority_guard ? *effective_priority_guard : priority_guard;
    }

    void holdImpl(
        size_t size,
        size_t elements,
        const CacheStateGuard::Lock & lock) override;

    void releaseImpl(size_t size, size_t elements) override;

    size_t getHoldSize() override { return total_hold_size; }

    size_t getHoldElements() override { return total_hold_elements; }

    /// Raw pointer so as not to add a reference `CacheUsagePerUser::canRemoveUser` would see.
    /// Dereferenced only while the priority is in use, when the non-zero size/elements counters
    /// block erasure and so keep the `CacheUsage` alive; the destructor never dereferences it.
    void setCacheUsage(CacheUsagePtr usage) override
    {
        cache_usage = usage.get();
    }

private:
    class LRUIterator;
    using LRUQueue = std::list<EntryPtr>;
    friend class SLRUFileCachePriority;
    friend class ::FileCacheTest_MoveEvictionPos_Test;

    /// Non-null when this queue's structural locking is redirected to another guard
    /// (set by SLRU for its sub-queues). See `getPriorityGuard`.
    CachePriorityGuard * effective_priority_guard = nullptr;

    void setPriorityGuard(CachePriorityGuard & guard) { effective_priority_guard = &guard; }

    size_t removeInvalidatedEntries(size_t max_batch) override;

    /// Throws if `sealStructure` was called. Called from every operation which mutates or reads
    /// `queue`, so a wrapper missing an override throws instead of silently using the sealed, unused base queue.
    void assertNotSealed(std::string_view method) const;

    LRUQueue queue;
    bool structure_sealed = false;
    const std::string description;
    LoggerPtr log;
    StatePtr state;
    /// Where the last collectCandidatesForEviction stopped, so a pass resumes instead of
    /// rescanning from the head
    LRUQueue::iterator reserve_eviction_pos TSA_GUARDED_BY(eviction_pos_mutex);
    LRUQueue::iterator background_eviction_pos TSA_GUARDED_BY(eviction_pos_mutex);
    mutable std::mutex eviction_pos_mutex;

    /// Select the cursor member for `cursor`. `FromHead` has no cursor and must not be passed.
    LRUQueue::iterator & evictionPos(EvictionCursor cursor) TSA_REQUIRES(eviction_pos_mutex);
    const LRUQueue::iterator & evictionPos(EvictionCursor cursor) const TSA_REQUIRES(eviction_pos_mutex);
    struct InvalidatedRef
    {
        std::weak_ptr<Entry> entry;
        LRUQueue::iterator iterator;
    };
    std::deque<InvalidatedRef> invalidated_refs TSA_GUARDED_BY(invalidated_mutex);
    mutable std::mutex invalidated_mutex;
    /// Size of `invalidated_refs`, kept as an atomic so the background cleanup can skip
    /// this queue without taking `invalidated_mutex` when there is nothing to clean up.
    std::atomic<size_t> invalidated_count = 0;
    /// Id of the current priority queue.
    /// Used to find its eviction info in collected eviction info map
    /// (which contains eviction info for several priority queues).
    const size_t queue_id;

    /// Total "hold" size by "IFileCachePriority::HoldSpace"
    /// (updated in holdImpl, releaseImpl).
    std::atomic<size_t> total_hold_size = 0;
    std::atomic<size_t> total_hold_elements = 0;
    /// When set, state mutations mirror into the per-user counters.
    CacheUsage * cache_usage = nullptr;

    bool canFit(
        size_t size,
        size_t elements,
        size_t released_size_assumption,
        size_t released_elements_assumption,
        const CacheStateGuard::Lock &,
        const size_t * max_size_ = nullptr,
        const size_t * max_elements_ = nullptr) const;

    LRUQueue::iterator remove(LRUQueue::iterator it, const CachePriorityGuard::WriteLock &);

    /// Apply a delta to `state` and, when `cache_usage` is set, mirror it there. The two updates
    /// are separate, so the mirror can transiently lag (see `CacheUsage::update`).
    void entryAdd(uint64_t size, uint64_t elements, const CacheStateGuard::Lock &);
    void entrySub(uint64_t size, uint64_t elements);

    /// Record an entry that invalidate() left in the queue for the background cleanup to remove.
    void addInvalidatedRef(std::weak_ptr<Entry> entry, LRUQueue::iterator it) noexcept;

    void iterate(IterateFunc func, FileCacheReserveStat & stat) override;

    LRUQueue::iterator iterateImpl(
        LRUQueue::iterator start_pos,
        IterateFunc func,
        FileCacheReserveStat & stat,
        const CachePriorityGuard::ReadLock &);

    LRUIterator add(
        EntryPtr entry,
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock *);

    /// Move a queue element from one queue to another.
    /// Used in SLRU eviction policy to upgrade/downgrade queue entries.
    LRUIterator move(
        LRUIterator & it,
        LRUFileCachePriority & other,
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock &);

    std::string getApproxStateInfoForLog() const;

    LRUQueue::iterator getEvictionPos(EvictionCursor cursor, const CachePriorityGuard::ReadLock &) const;
    void setEvictionPos(EvictionCursor cursor, LRUQueue::iterator it, const CachePriorityGuard::ReadLock &);
    /// Advance every cursor that points at `it` (which is about to be removed/spliced out).
    void moveEvictionPosIfEqual(LRUQueue::iterator it, const CachePriorityGuard::WriteLock &);
};

class LRUFileCachePriority::LRUIterator : public IFileCachePriority::Iterator
{
    friend class LRUFileCachePriority;
    friend class SLRUFileCachePriority;

public:
    LRUIterator(LRUFileCachePriority * cache_priority_, LRUQueue::iterator iterator_);

    LRUIterator(const LRUIterator & other);
    LRUIterator & operator =(const LRUIterator & other);
    bool operator ==(const LRUIterator & other) const;

    EntryPtr getEntry() const override;

    bool isValid(const CachePriorityGuard::WriteLock &) const override;

    void remove(const CachePriorityGuard::WriteLock &) override;

    void remove() override;

    void invalidate() noexcept override;

    void invalidateBeforeRemove(const CachePriorityGuard::WriteLock &) noexcept override;

    void incrementSize(size_t size, const CacheStateGuard::Lock &) override;

    void decrementSize(size_t size) override;

    QueueEntryType getType() const override { return QueueEntryType::LRU; }

    CachePriorityGuard & getPriorityGuard() const override { return cache_priority->getPriorityGuard(); }

    LRUQueue::iterator get() const { return iterator; }

private:
    bool assertValid() const;

    void invalidateImpl() noexcept;

    LRUFileCachePriority * cache_priority{};

    LRUQueue::iterator iterator;
    /// We store entry separately from iterator,
    /// because we want to be able to change its atomic state
    /// without any queue lock - both shared and unique locks - (in invalidate() method).
    /// A non-zero size entry will always stay in the queue by the same iterator
    /// until its state becomes Invalidated and it is removed.
    std::weak_ptr<Entry> entry;
};

}
