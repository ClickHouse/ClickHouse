#pragma once

#include <list>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Common/logger_useful.h>
#include "Interpreters/Cache/Guards.h"


namespace DB
{

/// Based on the LRU algorithm implementation, the record with the lowest priority is stored at
/// the head of the queue, and the record with the highest priority is stored at the tail.
class LRUFileCachePriority : public IFileCachePriority
{
protected:
    struct State
    {
        std::atomic<size_t> current_size = 0;
        std::atomic<size_t> current_elements_num = 0;
    };
    using StatePtr = std::shared_ptr<State>;

public:
    LRUFileCachePriority(
        size_t max_size_,
        size_t max_elements_,
        const std::string & description_ = "none",
        StatePtr state_ = nullptr);

    size_t getSize(const CachePriorityGuard::WriteLock &) const override { return state->current_size; }

    size_t getElementsCount(const CachePriorityGuard::WriteLock &) const override { return state->current_elements_num; }

    size_t getSizeApprox() const override { return state->current_size; }

    size_t getElementsCountApprox() const override { return state->current_elements_num; }

    std::string getStateInfoForLog(const CachePriorityGuard::WriteLock & lock) const override;

    bool canFit( /// NOLINT
        size_t size,
        size_t elements,
        const CachePriorityGuard::WriteLock &,
        IteratorPtr reservee = nullptr,
        bool best_effort = false) const override;

    IteratorPtr add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const UserInfo & user,
        const CachePriorityGuard::WriteLock &,
        bool best_effort = false) override;

    EvictionInfo checkEvictionInfo(
        size_t size,
        size_t elements,
        const CachePriorityGuard::WriteLock &) override;

    bool collectCandidatesForEviction(
        size_t size,
        size_t elements,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
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

    bool modifySizeLimits(size_t max_size_, size_t max_elements_, double size_ratio_, const CachePriorityGuard::WriteLock &) override;

    FileCachePriorityPtr copy() const { return std::make_unique<LRUFileCachePriority>(max_size, max_elements, description, state); }

private:
    class LRUIterator;
    using LRUQueue = std::list<EntryPtr>;
    friend class SLRUFileCachePriority;

    LRUQueue queue;
    const std::string description;
    LoggerPtr log;
    StatePtr state;

    void updateElementsCount(int64_t num);
    void updateSize(int64_t size);

    bool canFit(
        size_t size,
        size_t elements,
        size_t released_size_assumption,
        size_t released_elements_assumption,
        const CachePriorityGuard::WriteLock &,
        const size_t * max_size_ = nullptr,
        const size_t * max_elements_ = nullptr) const;

    LRUQueue::iterator remove(LRUQueue::iterator it, const CachePriorityGuard::WriteLock &);

    void iterate(IterateFunc func, const CachePriorityGuard::ReadLock &) override;

    LRUIterator move(LRUIterator & it, LRUFileCachePriority & other, const CachePriorityGuard::WriteLock &);
    LRUIterator add(EntryPtr entry, const CachePriorityGuard::WriteLock &);

    void holdImpl(
        size_t size,
        size_t elements,
        const CachePriorityGuard::WriteLock & lock) override;

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

    void incrementSize(size_t size, const CachePriorityGuard::WriteLock &) override;

    void decrementSize(size_t size) override;

    QueueEntryType getType() const override { return QueueEntryType::LRU; }

private:
    void assertValid() const;

    LRUFileCachePriority * cache_priority;
    mutable LRUQueue::iterator iterator;
};

}
