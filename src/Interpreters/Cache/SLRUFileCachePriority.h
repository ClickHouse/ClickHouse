#pragma once

#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Common/logger_useful.h>


namespace DB
{

/// Based on the SLRU algorithm implementation, the record with the lowest priority is stored at
/// the head of the queue, and the record with the highest priority is stored at the tail.
class SLRUFileCachePriority : public IFileCachePriority
{
public:
    class SLRUIterator;

    SLRUFileCachePriority(
        size_t max_size_,
        size_t max_elements_,
        double size_ratio_,
        LRUFileCachePriority::StatePtr probationary_state_ = nullptr,
        LRUFileCachePriority::StatePtr protected_state_ = nullptr,
        const std::string & description_ = "none");

    size_t getSize(const CachePriorityGuard::Lock & lock) const override;

    size_t getElementsCount(const CachePriorityGuard::Lock &) const override;

    size_t getSizeApprox() const override;

    size_t getElementsCountApprox() const override;

    std::string getStateInfoForLog(const CachePriorityGuard::Lock & lock) const override;

    void check(const CachePriorityGuard::Lock &) const override;

    bool canFit( /// NOLINT
        size_t size,
        size_t elements,
        const CachePriorityGuard::Lock &,
        IteratorPtr reservee = nullptr,
        bool best_effort = false) const override;

    IteratorPtr add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const UserInfo & user,
        const CachePriorityGuard::Lock &,
        bool is_startup = false) override;

    bool collectCandidatesForEviction(
        size_t size,
        size_t elements,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        const UserID & user_id,
        const CachePriorityGuard::Lock &) override;

    CollectStatus collectCandidatesForEviction(
        size_t desired_size,
        size_t desired_elements_count,
        size_t max_candidates_to_evict,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        const CachePriorityGuard::Lock &) override;

    void shuffle(const CachePriorityGuard::Lock &) override;

    PriorityDumpPtr dump(const CachePriorityGuard::Lock &) override;

    bool modifySizeLimits(size_t max_size_, size_t max_elements_, double size_ratio_, const CachePriorityGuard::Lock &) override;

private:
    double size_ratio;
    LRUFileCachePriority protected_queue;
    LRUFileCachePriority probationary_queue;
    LoggerPtr log;

    void increasePriority(SLRUIterator & iterator, const CachePriorityGuard::Lock & lock);

    void downgrade(IteratorPtr iterator, const CachePriorityGuard::Lock &);

    bool collectCandidatesForEvictionInProtected(
        size_t size,
        size_t elements,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        const UserID & user_id,
        const CachePriorityGuard::Lock & lock);

    LRUFileCachePriority::LRUIterator addOrThrow(
        EntryPtr entry,
        LRUFileCachePriority & queue,
        const CachePriorityGuard::Lock & lock);
};

class SLRUFileCachePriority::SLRUIterator : public IFileCachePriority::Iterator
{
    friend class SLRUFileCachePriority;
public:
    SLRUIterator(
        SLRUFileCachePriority * cache_priority_,
        LRUFileCachePriority::LRUIterator && lru_iterator_,
        bool is_protected_);

    EntryPtr getEntry() const override;

    size_t increasePriority(const CachePriorityGuard::Lock &) override;

    void remove(const CachePriorityGuard::Lock &) override;

    void invalidate() override;

    void incrementSize(size_t size, const CachePriorityGuard::Lock &) override;

    void decrementSize(size_t size) override;

    QueueEntryType getType() const override { return is_protected ? QueueEntryType::SLRU_Protected : QueueEntryType::SLRU_Probationary; }

private:
    void assertValid() const;

    SLRUFileCachePriority * cache_priority;
    LRUFileCachePriority::LRUIterator lru_iterator;
    /// Entry itself is stored by lru_iterator.entry.
    /// We have it as a separate field to use entry without requiring any lock
    /// (which will be required if we wanted to get entry from lru_iterator.getEntry()).
    const std::weak_ptr<Entry> entry;
    /// Atomic,
    /// but needed only in order to do FileSegment::getInfo() without any lock,
    /// which is done for system tables and logging.
    std::atomic<bool> is_protected;
    /// Iterator can me marked as non-movable in case we are reserving
    /// space for it. It means that we start space reservation
    /// and prepare space in probationary queue, then do eviction without lock,
    /// then take the lock again to finalize the eviction and we need to be sure
    /// that the element is still in probationary queue.
    /// Therefore we forbid concurrent priority increase for probationary entries.
    /// Same goes for the downgrade of queue entries from protected to probationary.
    /// (For downgrade there is no explicit check because it will fall into unreleasable state,
    /// e.g. will not be taken for eviction anyway).
    bool movable{true};
};

}
