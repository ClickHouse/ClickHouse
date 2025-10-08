#pragma once

#include <list>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>
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

    std::string getStateInfoForLog(const CacheStateGuard::Lock & lock) const override;

    EvictionInfo checkEvictionInfo(
        size_t size,
        size_t elements,
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
        bool best_effort = false) override;

    bool collectCandidatesForEviction(
        size_t size,
        size_t elements,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        bool continue_from_last_eviction_pos,
        const UserID & user_id,
        const CachePriorityGuard::ReadLock &) override;

    void shuffle(const CachePriorityGuard::WriteLock &) override;

    struct LRUPriorityDump : public IPriorityDump
    {
        std::vector<FileSegmentInfo> infos;
        explicit LRUPriorityDump(const std::vector<FileSegmentInfo> & infos_) : infos(infos_) {}
        void merge(const LRUPriorityDump & other) { infos.insert(infos.end(), other.infos.begin(), other.infos.end()); }
    };
    PriorityDumpPtr dump(const CachePriorityGuard::ReadLock &) override;

    void pop(const CachePriorityGuard::WriteLock & lock) { remove(queue.begin(), lock); } // NOLINT

    bool modifySizeLimits(
        size_t max_size_,
        size_t max_elements_,
        double size_ratio_,
        const CacheStateGuard::Lock &) override;

    FileCachePriorityPtr copy() const { return std::make_unique<LRUFileCachePriority>(max_size, max_elements, description, state); }

    void resetEvictionPos(const CachePriorityGuard::ReadLock &) override
    {
        std::lock_guard lock(eviction_pos_mutex);
        eviction_pos = queue.end();
    }

    /// Used only for unit test.
    size_t getEvictionPos()
    {
        std::lock_guard lock(eviction_pos_mutex);
        return std::distance(queue.begin(), eviction_pos);
    }

private:
    class LRUIterator;
    using LRUQueue = std::list<EntryPtr>;
    friend class SLRUFileCachePriority;

    LRUQueue queue;
    const std::string description;
    LoggerPtr log;
    StatePtr state;
    LRUQueue::iterator eviction_pos TSA_GUARDED_BY(eviction_pos_mutex);
    std::mutex eviction_pos_mutex;

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

    //void iterate(IterateFunc func, const CachePriorityGuard::Lock &) override;
    LRUQueue::iterator iterateImpl(
        LRUQueue::iterator start_pos,
        IterateFunc func,
        FileCacheReserveStat & stat,
        const CachePriorityGuard::ReadLock &);

    LRUIterator add(
        EntryPtr entry,
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock *);

    LRUIterator move(
        LRUIterator & it,
        LRUFileCachePriority & other,
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock &);

    void increasePriority(LRUQueue::iterator it, const CachePriorityGuard::WriteLock &);

    void holdImpl(
        size_t size,
        size_t elements,
        const CacheStateGuard::Lock & lock) override;

    void releaseImpl(size_t size, size_t elements) override;
    std::string getApproxStateInfoForLog() const;
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

    size_t increasePriority(const CachePriorityGuard::WriteLock &) override;

    void remove(const CachePriorityGuard::WriteLock &) override;

    void invalidate() override;

    void incrementSize(size_t size, const CacheStateGuard::Lock &) override;

    void decrementSize(size_t size) override;

    QueueEntryType getType() const override { return QueueEntryType::LRU; }

private:
    void assertValid() const;

    LRUFileCachePriority * cache_priority;
    mutable LRUQueue::iterator iterator;
};

}
