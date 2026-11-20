#pragma once

#include <Interpreters/FileCache/LRUFileCachePriority.h>
#include <Common/logger_useful.h>


namespace DB
{

/// Based on the SLRU algorithm implementation.
/// There are two queues: "protected" and "probationary".
/// All cache entries which have been accessed only once, would lie in probationary queue.
/// When entry is accessed more than once, it would go to the protected queue.
/// While the protected queue does not use all of its share, the probationary queue
/// can use the rest (see `lendProtectedCapacity`), so a cache which is not full does not evict.
/// The limits of the two queues always add up to the two shares.
class SLRUFileCachePriority : public IFileCachePriority
{
public:
    class SLRUIterator;

    SLRUFileCachePriority(
        QueueType queue_type_,
        size_t max_size_,
        size_t max_elements_,
        double size_ratio_,
        const std::string & description_ = "none",
        LRUFileCachePriority::StatePtr probationary_state_ = nullptr,
        LRUFileCachePriority::StatePtr protected_state_ = nullptr,
        bool lend_unused_protected_capacity_ = false);

    Type getType() const override { return Type::SLRU; }

    size_t getSize(const CacheStateGuard::Lock & lock) const override;
    size_t getSizeApprox() const override;

    size_t getElementsCount(const CacheStateGuard::Lock &) const override;
    size_t getElementsCountApprox() const override;

    size_t getProtectedSize(const CacheStateGuard::Lock & lock) const { return protected_queue.getSize(lock); }
    size_t getProtectedElementsCount(const CacheStateGuard::Lock & lock) const { return protected_queue.getElementsCount(lock); }
    size_t getProbationarySize(const CacheStateGuard::Lock & lock) const { return probationary_queue.getSize(lock); }
    size_t getProbationaryElementsCount(const CacheStateGuard::Lock & lock) const { return probationary_queue.getElementsCount(lock); }
    size_t getProtectedSizeLimit(const CacheStateGuard::Lock & lock) const { return protected_queue.getSizeLimit(lock); }

    std::string getStateInfoForLog(const CacheStateGuard::Lock & lock) const override;
    void check(const CacheStateGuard::Lock &) const override;

    double getSLRUSizeRatio() const override { return size_ratio; }

    EvictionInfoPtr collectEvictionInfo( /// NOLINT
        size_t size,
        size_t elements,
        IFileCachePriority::Iterator * reservee,
        bool is_total_space_cleanup,
        const IFileCachePriority::OriginInfo & origin_info,
        const CacheStateGuard::Lock &,
        QueueEntryType reservee_queue_type = QueueEntryType::None) override;

    bool canFit( /// NOLINT
        size_t size,
        size_t elements,
        const CacheStateGuard::Lock &,
        IteratorPtr reservee = nullptr,
        const OriginInfo & origin_info = {},
        bool is_initial_load = false) const override;

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
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock *) override;

    /// See `LRUFileCachePriority::sealStructure`. Both sub-queues are sealed, since
    /// every structural operation here lands in one of them.
    void sealStructure()
    {
        probationary_queue.sealStructure();
        protected_queue.sealStructure();
    }

    bool collectCandidatesForEviction( /// NOLINT
        EvictionInfo & eviction_info,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        EvictionCursor eviction_cursor,
        size_t max_candidates_size,
        bool is_total_space_cleanup,
        const OriginInfo & origin_info,
        CacheStateGuard &,
        QueueEntryType reservee_queue_type = QueueEntryType::None) override;

    void iterate(IterateFunc func, FileCacheReserveStat & stat) override;

    bool tryIncreasePriority(
        Iterator & iterator_,
        bool is_space_reservation_complete,
        CacheStateGuard & state_guard) override;

    void shuffle() override;

    void resetEvictionPos(EvictionCursor cursor) override;

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

    FileCachePriorityPtr copy() const;

    /// For tests which put entries into a chosen sub-queue with `addForRestore`.
    CachePriorityGuard & getPriorityGuardForTests() { return getPriorityGuard(); }

protected:
    void setInvalidateNotifier(size_t threshold, std::function<void()> on_invalidate) override
    {
        /// Remember the hook on this priority as well: `OvercommitFileCachePriority`
        /// reads it back when wiring newly created per-user priorities.
        IFileCachePriority::setInvalidateNotifier(threshold, on_invalidate);
        protected_queue.setInvalidateNotifier(threshold, on_invalidate);
        probationary_queue.setInvalidateNotifier(threshold, on_invalidate);
    }

    size_t removeInvalidatedEntries(size_t max_batch) override
    {
        size_t removed = protected_queue.removeInvalidatedEntries(max_batch);
        if (removed < max_batch)
            removed += probationary_queue.removeInvalidatedEntries(max_batch - removed);
        return removed;
    }

    size_t getHoldSize() override { return protected_queue.getHoldSize() + probationary_queue.getHoldSize(); }

    size_t getHoldElements() override { return protected_queue.getHoldElements() + probationary_queue.getHoldElements(); }

    void setCacheUsage(CacheUsagePtr usage) override
    {
        probationary_queue.setCacheUsage(usage);
        protected_queue.setCacheUsage(usage);
    }

    void holdImpl(size_t size, size_t elements, const CacheStateGuard::Lock &) override
    {
        chassert(!elements);
        pending_take_back_size += size;
    }

    void releaseImpl(size_t size, size_t elements) override
    {
        chassert(!elements);
        pending_take_back_size -= size;
    }

private:
    using LRUIterator = LRUFileCachePriority::LRUIterator;
    using LRUQueue = std::list<Entry>;

    std::string description;
    double size_ratio;
    /// Copies share the queue sizes but not the limits, so only a priority that is never copied may move limits.
    const bool lend_unused_protected_capacity;
    LRUFileCachePriority protected_queue;
    LRUFileCachePriority probationary_queue;
    LoggerPtr log;
    SharedEvictionCursor promotion_cursor{EvictionCursor::Promotion};

    /// Lent capacity reserved by protected entries which grow (see `collectEvictionInfo`).
    std::atomic<size_t> pending_take_back_size = 0;
    /// Keys that reservation in an `EvictionInfo`.
    const size_t take_back_queue_id;

    void increasePriority(SLRUIterator & iterator, const CachePriorityGuard::WriteLock & lock);

    /// Moves unused protected capacity to probationary, so that probationary does not evict while protected has room.
    void lendProtectedCapacity(size_t size, size_t elements, const CacheStateGuard::Lock & lock);
    bool canFitInProbationary(size_t size, size_t elements, const CacheStateGuard::Lock & lock) const;
    /// Unused capacity of the protected queue that it can lend.
    size_t getLendableSize(const CacheStateGuard::Lock & lock) const;
    size_t getLendableElements(const CacheStateGuard::Lock & lock) const;
    /// Capacity that the probationary queue currently borrows from the protected queue.
    size_t getLentSize(const CacheStateGuard::Lock & lock) const;
    size_t getLentElements(const CacheStateGuard::Lock & lock) const;
    /// Moves lent capacity back to protected for an entry which grows by `size`, as far as probationary has it free.
    void takeBackLentCapacity(size_t size, const CacheStateGuard::Lock & lock);

    /// With `wait_for_priority_lock` false, returns false as soon as a walk finds a writer
    /// holding or waiting for the priority guard, instead of queueing behind it.
    bool collectCandidatesForEvictionInProtected(
        EvictionInfo & eviction_info,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        EvictionCursor eviction_cursor,
        size_t max_candidates_size,
        bool is_total_space_cleanup,
        const OriginInfo & origin_info,
        CacheStateGuard & state_guard,
        bool wait_for_priority_lock);

    LRUFileCachePriority::LRUIterator addOrThrow(
        EntryPtr entry,
        LRUFileCachePriority & queue,
        const CachePriorityGuard::WriteLock & lock,
        const CacheStateGuard::Lock &);
};

class SLRUFileCachePriority::SLRUIterator : public IFileCachePriority::Iterator
{
    friend class SLRUFileCachePriority;
public:
    SLRUIterator(
        SLRUFileCachePriority * cache_priority_,
        LRUFileCachePriority::LRUIterator && lru_iterator_,
        bool is_protected_);

    QueueEntryType getType() const override { return is_protected ? QueueEntryType::SLRU_Protected : QueueEntryType::SLRU_Probationary; }

    EntryPtr getEntry() const override;

    bool isValid(const CachePriorityGuard::WriteLock &) const override;

    void remove(const CachePriorityGuard::WriteLock &) override;

    void remove() override;

    void invalidate() noexcept override;

    void invalidateBeforeRemove(const CachePriorityGuard::WriteLock &) noexcept override;

    void incrementSize(size_t size, const CacheStateGuard::Lock &) override;

    void decrementSize(size_t size) override;

    CachePriorityGuard & getPriorityGuard() const override { return cache_priority->getPriorityGuard(); }

private:
    bool assertValid() const;

    void setIterator(LRUIterator && iterator_, bool is_protected_, const CacheStateGuard::Lock &);

    SLRUFileCachePriority * cache_priority;
    LRUIterator lru_iterator;
    /// Entry itself is stored by lru_iterator.entry.
    /// We have it as a separate field to use entry without requiring priority write lock
    /// (which will be required if we wanted to get entry from lru_iterator.getEntry()).
    std::weak_ptr<Entry> entry TSA_GUARDED_BY(entry_mutex);
    mutable std::mutex entry_mutex;
    /// Atomic,
    /// but needed only in order to do FileSegment::getInfo() without priority write lock,
    /// which is done for system tables and logging.
    std::atomic<bool> is_protected;
};

}
