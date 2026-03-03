#pragma once

#include <list>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/UserCacheUsage.h>
#include <Common/logger_useful.h>
#include <Interpreters/Cache/Guards.h>


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
        size_t max_size_,
        size_t max_elements_,
        const std::string & description_ = "none",
        StatePtr state_ = nullptr);

    Type getType() const override { return Type::LRU; }

    size_t getSize(const CacheStateGuard::Lock & lock) const override { return state->getSize(lock); }
    size_t getSizeApprox() const override { return state->getSizeApprox(); }

    size_t getElementsCount(const CacheStateGuard::Lock & lock) const override { return state->getElementsCount(lock); }
    size_t getElementsCountApprox() const override { return state->getElementsCountApprox(); }

    size_t getQueueID() const { return queue_id; }

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
        bool best_effort = false) const override;

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
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock *,
        bool best_effort = false) override;

    bool collectCandidatesForEviction(
        const EvictionInfo & eviction_info,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        InvalidatedEntriesInfos & invalidated_entries,
        IFileCachePriority::IteratorPtr reservee,
        bool continue_from_last_eviction_pos,
        size_t max_candidates_size,
        bool is_total_space_cleanup,
        const OriginInfo & origin_info,
        CachePriorityGuard &,
        CacheStateGuard &) override;

    bool tryIncreasePriority(
        Iterator & iterator,
        bool is_space_reservation_complete,
        CachePriorityGuard & queue_guard,
        CacheStateGuard & state_guard) override;

    void shuffle(const CachePriorityGuard::WriteLock &) override;

    PriorityDumpPtr dump(const CachePriorityGuard::ReadLock &) override;

    void pop(const CachePriorityGuard::WriteLock & lock) { remove(queue.begin(), lock); } // NOLINT

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

    FileCachePriorityPtr copy() const { return std::make_unique<LRUFileCachePriority>(max_size, max_elements, description, state); }

    /// See a comment near eviction_pos.
    void resetEvictionPos() override
    {
        std::lock_guard lock(eviction_pos_mutex);
        eviction_pos = LRUQueue::iterator{};
    }

    /// Used only for unit test.
    size_t getEvictionPosCount()
    {
        std::lock_guard lock(eviction_pos_mutex);
        if (eviction_pos == LRUQueue::iterator{})
            return 0;
        return std::distance(queue.begin(), eviction_pos);
    }

protected:
    void holdImpl(
        size_t size,
        size_t elements,
        const CacheStateGuard::Lock & lock) override;

    void releaseImpl(size_t size, size_t elements) override;

    size_t getHoldSize() override { return total_hold_size; }

    size_t getHoldElements() override { return total_hold_elements; }

    /// Used to collect stats for a system table (in private).
    void setCacheUsageStatGuard(std::shared_ptr<CacheUsageStatGuard> guard) override
    {
        cache_usage_stat_guard = guard;
    }

private:
    class LRUIterator;
    using LRUQueue = std::list<EntryPtr>;
    friend class SLRUFileCachePriority;

    LRUQueue queue;
    const std::string description;
    LoggerPtr log;
    StatePtr state;
    /// Eviction position is a pointer used in collectCandidatesForEviction
    /// to track where the last collectCandidatesForEviction stopped.
    /// This is an optimization for concurrently made eviction attempts,
    /// which allows us not to iterate the queue from scratch,
    /// skipping elements which are likely in non-evictable state.
    LRUQueue::iterator eviction_pos TSA_GUARDED_BY(eviction_pos_mutex);
    mutable std::mutex eviction_pos_mutex;
    /// Id of the current priority queue.
    /// Used to find its eviction info in collected eviction info map
    /// (which contains eviction info for several priority queues).
    const size_t queue_id;

    /// Total "hold" size by "IFileCachePriority::HoldSpace"
    /// (updated in holdImpl, releaseImpl).
    std::atomic<size_t> total_hold_size = 0;
    std::atomic<size_t> total_hold_elements = 0;
    std::shared_ptr<CacheUsageStatGuard> cache_usage_stat_guard;

    bool canFit(
        size_t size,
        size_t elements,
        size_t released_size_assumption,
        size_t released_elements_assumption,
        const CacheStateGuard::Lock &,
        const size_t * max_size_ = nullptr,
        const size_t * max_elements_ = nullptr) const;

    LRUQueue::iterator remove(LRUQueue::iterator it, const CachePriorityGuard::WriteLock &);

    void iterate(
        IterateFunc func,
        FileCacheReserveStat & stat,
        const CachePriorityGuard::ReadLock &) override;

    LRUQueue::iterator iterateImpl(
        LRUQueue::iterator start_pos,
        IterateFunc func,
        FileCacheReserveStat & stat,
        InvalidatedEntriesInfos & invalidated_entries,
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

    LRUQueue::iterator getEvictionPos(const CachePriorityGuard::ReadLock &) const;
    void setEvictionPos(LRUQueue::iterator it, const CachePriorityGuard::ReadLock &);
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

    void invalidate() override;

    void incrementSize(size_t size, const CacheStateGuard::Lock &) override;

    void decrementSize(size_t size) override;

    QueueEntryType getType() const override { return QueueEntryType::LRU; }

    LRUQueue::iterator get() const { return iterator; }

private:
    bool assertValid() const;

    LRUFileCachePriority * cache_priority;

    LRUQueue::iterator iterator;
    /// We store entry separately from iterator,
    /// because we want to be able to change its atomic state
    /// without any queue lock - both shared and unique locks - (in invalidate() method).
    /// A non-zero size entry will always stay in the queue by the same iterator
    /// until its state becomes Invalidated and it is removed.
    std::weak_ptr<Entry> entry;
};

}
