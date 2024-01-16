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
class LRUFileCachePriority final : public IFileCachePriority
{
private:
    class LRUIterator;
    using LRUQueue = std::list<EntryPtr>;
    friend class SLRUFileCachePriority;

public:
    LRUFileCachePriority(size_t max_size_, size_t max_elements_) : IFileCachePriority(max_size_, max_elements_) {}

    size_t getSize(const CacheGuard::Lock &) const override { return current_size; }

    size_t getElementsCount(const CacheGuard::Lock &) const override { return current_elements_num; }

    bool canFit(size_t size, const CacheGuard::Lock &) const override;

    IteratorPtr add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const CacheGuard::Lock &,
        bool is_startup = false) override;

    bool collectCandidatesForEviction(
        size_t size,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        FinalizeEvictionFunc & finalize_eviction_func,
        const CacheGuard::Lock &) override;

    void shuffle(const CacheGuard::Lock &) override;

    std::vector<FileSegmentInfo> dump(const CacheGuard::Lock &) override;

    void pop(const CacheGuard::Lock & lock) { remove(queue.begin(), lock); }

    bool modifySizeLimits(size_t max_size_, size_t max_elements_, double size_ratio_, const CacheGuard::Lock &) override;

private:
    void updateElementsCount(int64_t num);
    void updateSize(int64_t size);

    LRUQueue queue;
    Poco::Logger * log = &Poco::Logger::get("LRUFileCachePriority");

    std::atomic<size_t> current_size = 0;
    /// current_elements_num is not always equal to queue.size()
    /// because of invalidated entries.
    std::atomic<size_t> current_elements_num = 0;

    bool canFit(size_t size, size_t released_size_assumption, size_t released_elements_assumption, const CacheGuard::Lock &) const;

    LRUQueue::iterator remove(LRUQueue::iterator it, const CacheGuard::Lock &);

    enum class IterationResult
    {
        BREAK,
        CONTINUE,
        REMOVE_AND_CONTINUE,
    };
    using IterateFunc = std::function<IterationResult(LockedKey &, const FileSegmentMetadataPtr &)>;
    void iterate(IterateFunc && func, const CacheGuard::Lock &);

    LRUIterator move(LRUIterator & it, LRUFileCachePriority & other, const CacheGuard::Lock &);
    LRUIterator add(EntryPtr entry, const CacheGuard::Lock &);
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

    EntryPtr getEntry() const override { return *iterator; }

    size_t increasePriority(const CacheGuard::Lock &) override;

    void remove(const CacheGuard::Lock &) override;

    void invalidate() override;

    void updateSize(int64_t size) override;

    QueueEntryType getType() const override { return QueueEntryType::LRU; }

private:
    void assertValid() const;

    LRUFileCachePriority * cache_priority;
    mutable LRUQueue::iterator iterator;
};

}
