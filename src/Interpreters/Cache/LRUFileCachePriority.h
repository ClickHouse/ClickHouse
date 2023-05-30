#pragma once

#include <list>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Common/logger_useful.h>

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

public:
    LRUFileCachePriority(size_t max_size_, size_t max_elements_) : IFileCachePriority(max_size_, max_elements_) {}

    size_t getSize(const CacheGuard::Lock &) const override { return current_size; }

    size_t getElementsCount(const CacheGuard::Lock &) const override { return queue.size(); }

    Iterator add(KeyMetadataPtr key_metadata, size_t offset, size_t size, const CacheGuard::Lock &) override;

    void pop(const CacheGuard::Lock &) override;

    void removeAll(const CacheGuard::Lock &) override;

    void iterate(IterateFunc && func, const CacheGuard::Lock &) override;

private:
    LRUQueue queue;
    Poco::Logger * log = &Poco::Logger::get("LRUFileCachePriority");

    std::atomic<size_t> current_size = 0;

    LRUQueueIterator remove(LRUQueueIterator it);
};

class LRUFileCachePriority::LRUFileCacheIterator : public IFileCachePriority::IIterator
{
public:
    LRUFileCacheIterator(
        LRUFileCachePriority * cache_priority_,
        LRUFileCachePriority::LRUQueueIterator queue_iter_);

    const Entry & getEntry() const override { return *queue_iter; }

    Entry & getEntry() override { return *queue_iter; }

    size_t use(const CacheGuard::Lock &) override;

    Iterator remove(const CacheGuard::Lock &) override;

    void annul() override;

    void updateSize(int64_t size) override;

private:
    LRUFileCachePriority * cache_priority;
    mutable LRUFileCachePriority::LRUQueueIterator queue_iter;
};

}
