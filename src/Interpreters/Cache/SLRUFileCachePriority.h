#pragma once

#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Common/logger_useful.h>


namespace DB
{

/// Based on the SLRU algorithm implementation.
/// There are two queues: "protected" and "probationary".
/// All cache entries which have been accessed only once, would lie in probationary queue.
/// When entry is accessed more than once, it would go to the protected queue.
class SLRUFileCachePriority : public IFileCachePriority
{
public:
    class SLRUIterator;

    SLRUFileCachePriority(
        size_t max_size_,
        size_t max_elements_,
        double size_ratio_,
        const std::string & description_,
        LRUFileCachePriority::StatePtr probationary_state_ = nullptr,
        LRUFileCachePriority::StatePtr protected_state_ = nullptr);

    Type getType() const override { return Type::SLRU; }

    size_t getSize(const CacheStateGuard::Lock & lock) const override;
    size_t getSizeApprox() const override;

    size_t getElementsCount(const CacheStateGuard::Lock &) const override;
    size_t getElementsCountApprox() const override;

    std::string getStateInfoForLog(const CacheStateGuard::Lock & lock) const override;
    void check(const CacheStateGuard::Lock &) const override;

    double getSLRUSizeRatio() const override { return size_ratio; }

    EvictionInfo checkEvictionInfo(
        size_t size,
        size_t elements,
        IFileCachePriority::Iterator * reservee,
        const CacheStateGuard::Lock &) override;

    bool canFit( /// NOLINT
        size_t size,
        size_t elements,
        const CacheStateGuard::Lock &,
        IteratorPtr reservee = nullptr,
        bool best_effort = false) const override;

    IteratorPtr add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const UserInfo & user,
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock *,
        bool is_startup = false) override;

    bool collectCandidatesForEviction(
        size_t size,
        size_t elements,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        bool continue_from_last_eviction_pos,
        const UserID & user_id,
        const CachePriorityGuard::ReadLock &) override;

    void iterate(
        IterateFunc func,
        FileCacheReserveStat & stat,
        const CachePriorityGuard::ReadLock &) override;

    bool tryIncreasePriority(
        Iterator & iterator_,
        CachePriorityGuard & queue_guard,
        CacheStateGuard & state_guard) override;

    void shuffle(const CachePriorityGuard::WriteLock &) override;

    void resetEvictionPos(const CachePriorityGuard::ReadLock &) override;

    PriorityDumpPtr dump(const CachePriorityGuard::ReadLock &) override;

    bool modifySizeLimits(
        size_t max_size_,
        size_t max_elements_,
        double size_ratio_,
        const CacheStateGuard::Lock &) override;

    FileCachePriorityPtr copy() const;

private:
    using LRUIterator = LRUFileCachePriority::LRUIterator;
    using LRUQueue = std::list<Entry>;

    std::string description;
    double size_ratio;
    LRUFileCachePriority protected_queue;
    LRUFileCachePriority probationary_queue;
    LoggerPtr log;

    void increasePriority(SLRUIterator & iterator, const CachePriorityGuard::WriteLock & lock);

    void downgrade(IteratorPtr iterator, const CachePriorityGuard::WriteLock &, const CacheStateGuard::Lock &);

    bool collectCandidatesForEvictionInProtected(
        size_t size,
        size_t elements,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        bool continue_from_last_eviction_pos,
        const UserID & user_id,
        const CachePriorityGuard::ReadLock & lock);

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

    EntryPtr getEntry() const override;

    void remove(const CachePriorityGuard::WriteLock &) override;

    void invalidate() override;

    void incrementSize(size_t size, const CacheStateGuard::Lock &) override;

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
