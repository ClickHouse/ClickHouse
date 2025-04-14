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
        StatePtr state_ = nullptr,
        const std::string & description_ = "none");

    size_t getSize(const CachePriorityGuard::Lock &) const override { return state->current_size; }

    size_t getElementsCount(const CachePriorityGuard::Lock &) const override { return state->current_elements_num; }

    size_t getSizeApprox() const override { return state->current_size; }

    size_t getElementsCountApprox() const override { return state->current_elements_num; }

    std::string getStateInfoForLog(const CachePriorityGuard::Lock & lock) const override;

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
        bool best_effort = false) override;

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

    struct LRUPriorityDump : public IPriorityDump
    {
        std::vector<FileSegmentInfo> infos;
        explicit LRUPriorityDump(const std::vector<FileSegmentInfo> & infos_) : infos(infos_) {}
        void merge(const LRUPriorityDump & other) { infos.insert(infos.end(), other.infos.begin(), other.infos.end()); }
    };
    PriorityDumpPtr dump(const CachePriorityGuard::Lock &) override;

    void pop(const CachePriorityGuard::Lock & lock) { remove(queue.begin(), lock); } // NOLINT

    bool modifySizeLimits(size_t max_size_, size_t max_elements_, double size_ratio_, const CachePriorityGuard::Lock &) override;

    FileCachePriorityPtr copy() const { return std::make_unique<LRUFileCachePriority>(max_size, max_elements, state); }

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
        const CachePriorityGuard::Lock &,
        const size_t * max_size_ = nullptr,
        const size_t * max_elements_ = nullptr) const;

    LRUQueue::iterator remove(LRUQueue::iterator it, const CachePriorityGuard::Lock &);

    enum class IterationResult : uint8_t
    {
        BREAK,
        CONTINUE,
        REMOVE_AND_CONTINUE,
    };
    using IterateFunc = std::function<IterationResult(LockedKey &, const FileSegmentMetadataPtr &)>;
    void iterate(IterateFunc && func, const CachePriorityGuard::Lock &);

    LRUIterator move(LRUIterator & it, LRUFileCachePriority & other, const CachePriorityGuard::Lock &);
    LRUIterator add(EntryPtr entry, const CachePriorityGuard::Lock &);

    using StopConditionFunc = std::function<bool()>;
    void iterateForEviction(
        EvictionCandidates & res,
        FileCacheReserveStat & stat,
        StopConditionFunc stop_condition,
        const CachePriorityGuard::Lock &);

    void holdImpl(
        size_t size,
        size_t elements,
        const CachePriorityGuard::Lock & lock) override;

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

    size_t increasePriority(const CachePriorityGuard::Lock &) override;

    void remove(const CachePriorityGuard::Lock &) override;

    void invalidate() override;

    void incrementSize(size_t size, const CachePriorityGuard::Lock &) override;

    void decrementSize(size_t size) override;

    QueueEntryType getType() const override { return QueueEntryType::LRU; }

private:
    void assertValid() const;

    LRUFileCachePriority * cache_priority;
    mutable LRUQueue::iterator iterator;
};

}
