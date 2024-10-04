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
        LRUFileCachePriority::StatePtr protected_state_ = nullptr);

    size_t getSize(const CachePriorityGuard::Lock & lock) const override;

    size_t getElementsCount(const CachePriorityGuard::Lock &) const override;

    size_t getSizeApprox() const override;

    size_t getElementsCountApprox() const override;

    bool canFit( /// NOLINT
        size_t size,
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
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        FinalizeEvictionFunc & finalize_eviction_func,
        const UserID & user_id,
        const CachePriorityGuard::Lock &) override;

    void shuffle(const CachePriorityGuard::Lock &) override;

    PriorityDumpPtr dump(const CachePriorityGuard::Lock &) override;

    bool modifySizeLimits(size_t max_size_, size_t max_elements_, double size_ratio_, const CachePriorityGuard::Lock &) override;

private:
    double size_ratio;
    LRUFileCachePriority protected_queue;
    LRUFileCachePriority probationary_queue;
    LoggerPtr log = getLogger("SLRUFileCachePriority");

    void increasePriority(SLRUIterator & iterator, const CachePriorityGuard::Lock & lock);
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

    void updateSize(int64_t size) override;

    QueueEntryType getType() const override { return is_protected ? QueueEntryType::SLRU_Protected : QueueEntryType::SLRU_Probationary; }

private:
    void assertValid() const;

    SLRUFileCachePriority * cache_priority;
    LRUFileCachePriority::LRUIterator lru_iterator;
    const EntryPtr entry;
    /// Atomic,
    /// but needed only in order to do FileSegment::getInfo() without any lock,
    /// which is done for system tables and logging.
    std::atomic<bool> is_protected;
};

}
