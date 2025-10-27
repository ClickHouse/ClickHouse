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

    EvictionInfoPtr collectEvictionInfo(
        size_t size,
        size_t elements,
        IFileCachePriority::Iterator * reservee,
        bool is_total_space_cleanup,
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
        const EvictionInfo & eviction_info,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        InvalidatedEntriesInfos & invalidated_entries,
        IFileCachePriority::IteratorPtr reservee,
        bool continue_from_last_eviction_pos,
        size_t max_candidates_size,
        bool is_total_space_cleanup,
        const UserID & user_id,
        const CachePriorityGuard::ReadLock &) override;

    void iterate(
        IterateFunc func,
        FileCacheReserveStat & stat,
        const CachePriorityGuard::ReadLock &) override;

    bool tryIncreasePriority(
        Iterator & iterator_,
        bool is_space_reservation_complete,
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

    bool collectCandidatesForEvictionInProtected(
        const EvictionInfo & eviction_info,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        InvalidatedEntriesInfos & invalidated_entries,
        IFileCachePriority::IteratorPtr reservee,
        bool continue_from_last_eviction_pos,
        size_t max_candidates_size,
        bool is_total_space_cleanup,
        const UserID & user_id,
        const CachePriorityGuard::ReadLock & lock);

    LRUFileCachePriority::LRUIterator addOrThrow(
        EntryPtr entry,
        LRUFileCachePriority & queue,
        const CachePriorityGuard::WriteLock & lock,
        const CacheStateGuard::Lock &);

    EvictionInfoPtr collectEvictionInfoImpl(
        size_t size,
        size_t elements,
        IFileCachePriority::Iterator * reservee,
        bool is_total_space_cleanup,
        const CacheStateGuard::Lock & lock,
        std::optional<bool> in_protected);
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

    bool isValid(const CachePriorityGuard::WriteLock &) override;

    void remove(const CachePriorityGuard::WriteLock &) override;

    void invalidate() override;

    void incrementSize(size_t size, const CacheStateGuard::Lock &) override;

    void decrementSize(size_t size) override;

    QueueEntryType getType() const override { return is_protected ? QueueEntryType::SLRU_Protected : QueueEntryType::SLRU_Probationary; }

    /// Can be called only once,
    /// and only if iterator was previously created with an empty entry.
    void setEntry(EntryPtr && entry_, const CacheStateGuard::Lock &);

private:
    bool assertValid() const;

    SLRUFileCachePriority * cache_priority;
    LRUFileCachePriority::LRUIterator lru_iterator;
    /// Entry itself is stored by lru_iterator.entry.
    /// We have it as a separate field to use entry without requiring any lock
    /// (which will be required if we wanted to get entry from lru_iterator.getEntry()).
    std::weak_ptr<Entry> entry TSA_GUARDED_BY(entry_mutex);
    mutable std::mutex entry_mutex;
    /// Atomic,
    /// but needed only in order to do FileSegment::getInfo() without any lock,
    /// which is done for system tables and logging.
    std::atomic<bool> is_protected;
};

}
