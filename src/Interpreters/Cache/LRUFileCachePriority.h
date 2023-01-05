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
    LRUFileCachePriority() = default;

    Iterator add(
        const Key & key,
        size_t offset,
        size_t size,
        KeyTransactionCreatorPtr key_transaction_creator,
        const CachePriorityQueueGuard::Lock &) override;

    void removeAll(const CachePriorityQueueGuard::Lock &) override;

    void iterate(IterateFunc && func, const CachePriorityQueueGuard::Lock &) override;

    size_t getElementsNum(const CachePriorityQueueGuard::Lock &) const override { return queue.size(); }

private:
    LRUQueue queue;
    Poco::Logger * log = &Poco::Logger::get("LRUFileCachePriority");

    LRUQueueIterator remove(LRUQueueIterator it);
};

class LRUFileCachePriority::LRUFileCacheIterator : public IFileCachePriority::IIterator
{
public:
    LRUFileCacheIterator(
        LRUFileCachePriority * cache_priority_,
        LRUFileCachePriority::LRUQueueIterator queue_iter_);

    Entry & operator *() override { return *queue_iter; }
    const Entry & operator *() const override { return *queue_iter; }

    size_t use(const CachePriorityQueueGuard::Lock &) override;

    Iterator remove(const CachePriorityQueueGuard::Lock &) override;

    void incrementSize(ssize_t size, const CachePriorityQueueGuard::Lock &) override;

private:
    LRUFileCachePriority * cache_priority;
    mutable LRUFileCachePriority::LRUQueueIterator queue_iter;
};

};
