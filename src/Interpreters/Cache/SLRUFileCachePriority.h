#pragma once

#include <list>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Common/logger_useful.h>
#include <Interpreters/Cache/Guards.h>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSizeLimit;
}

namespace DB
{

/// Based on the SLRU algorithm implementation, the record with the lowest priority is stored at
/// the head of the queue, and the record with the highest priority is stored at the tail.
class SLRUFileCachePriority : public IFileCachePriority
{
private:
    class SLRUFileCacheIterator;
    using LRUQueue = std::list<Entry>;
    using SLRUQueueIterator = typename LRUQueue::iterator;

public:
    SLRUFileCachePriority(size_t max_size_, size_t max_elements_, double size_ratio);

    size_t getSize(const CacheGuard::Lock & lock) const override;

    size_t getElementsCount(const CacheGuard::Lock &) const override;

    Iterator add(KeyMetadataPtr key_metadata, size_t offset, size_t size, const CacheGuard::Lock &) override;

    bool collectCandidatesForEviction(
        size_t size,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::Iterator,
        FinalizeEvictionFunc & finalize_eviction_func,
        const CacheGuard::Lock &) override;

    void shuffle(const CacheGuard::Lock &) override;

    FileSegments dump(const CacheGuard::Lock &) override;

private:
    void updateElementsCount(int64_t num, bool is_protected);
    void updateSize(int64_t size, bool is_protected);

    LRUFileCachePriority protected_queue;
    LRUFileCachePriority probationary_queue;

    Poco::Logger * log = &Poco::Logger::get("SLRUFileCachePriority");

    SLRUQueueIterator remove(SLRUQueueIterator it, bool is_protected, const CacheGuard::Lock & lock);
    SLRUQueueIterator increasePriority(SLRUQueueIterator & it, bool is_protected, const CacheGuard::Lock & lock);
};

class SLRUFileCachePriority::SLRUFileCacheIterator : public IFileCachePriority::IIterator
{
    friend class SLRUFileCachePriority;
public:
    SLRUFileCacheIterator(
        SLRUFileCachePriority * cache_priority_,
        SLRUFileCachePriority::SLRUQueueIterator queue_iter_,
        bool is_protected_);

    const Entry & getEntry() const override { return *queue_iter; }

    size_t increasePriority(const CacheGuard::Lock &) override;

    void remove(const CacheGuard::Lock &) override;

    void invalidate() override;

    void updateSize(int64_t size) override;

private:
    void checkUsable() const;

    SLRUFileCachePriority * cache_priority;
    mutable SLRUFileCachePriority::SLRUQueueIterator queue_iter;
    const bool is_protected;
};

}
