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
private:
    class LRUFileCacheIterator;
    using LRUQueue = std::list<Entry>;
    using LRUQueueIterator = typename LRUQueue::iterator;
    friend class SLRUFileCachePriority;

public:
    LRUFileCachePriority(size_t max_size_, size_t max_elements_) : IFileCachePriority(max_size_, max_elements_) {}

    size_t getSize(const CacheGuard::Lock &) const override { return current_size; }

    size_t getElementsCount(const CacheGuard::Lock &) const override { return current_elements_num; }

    Iterator add(KeyMetadataPtr key_metadata, size_t offset, size_t size, const CacheGuard::Lock &) override;

    bool collectCandidatesForEviction(
        size_t size,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::Iterator it,
        FinalizeEvictionFunc & finalize_eviction_func,
        const CacheGuard::Lock &) override;

    void shuffle(const CacheGuard::Lock &) override;

    FileSegments dump(const CacheGuard::Lock &) override;

    void pop(const CacheGuard::Lock & lock) { remove(queue.begin(), lock); }

private:
    void updateElementsCount(int64_t num);
    void updateSize(int64_t size);

    LRUQueue queue;
    Poco::Logger * log = &Poco::Logger::get("LRUFileCachePriority");

    std::atomic<size_t> current_size = 0;
    /// current_elements_num is not always equal to queue.size()
    /// because of invalidated entries.
    std::atomic<size_t> current_elements_num = 0;

    LRUQueueIterator remove(LRUQueueIterator it, const CacheGuard::Lock &);

    enum class IterationResult
    {
        BREAK,
        CONTINUE,
        REMOVE_AND_CONTINUE,
    };
    using IterateFunc = std::function<IterationResult(LockedKey &, const FileSegmentMetadataPtr &)>;
    void iterate(IterateFunc && func, const CacheGuard::Lock &);

    size_t increasePriority(LRUQueueIterator it, const CacheGuard::Lock &);
    LRUQueueIterator move(LRUQueueIterator it, LRUFileCachePriority & other, const CacheGuard::Lock &);
    LRUQueueIterator add(Entry && entry, const CacheGuard::Lock &);
    bool canFit(size_t size, const CacheGuard::Lock &) const;
    bool canFit(size_t size, size_t released_size_assumption, size_t released_elements_assumption, const CacheGuard::Lock &) const;
};

class LRUFileCachePriority::LRUFileCacheIterator : public IFileCachePriority::IIterator
{
public:
    LRUFileCacheIterator(
        LRUFileCachePriority * cache_priority_,
        LRUFileCachePriority::LRUQueueIterator queue_iter_);

    const Entry & getEntry() const override { return *queue_iter; }

    size_t increasePriority(const CacheGuard::Lock &) override;

    void remove(const CacheGuard::Lock &) override;

    void invalidate() override;

    void updateSize(int64_t size) override;

private:
    void checkUsable() const;

    LRUFileCachePriority * cache_priority;
    mutable LRUFileCachePriority::LRUQueueIterator queue_iter;
};

}
