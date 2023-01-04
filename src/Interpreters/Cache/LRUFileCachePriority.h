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
    using LRUQueue = std::list<FileCacheRecord>;
    using LRUQueueIterator = typename LRUQueue::iterator;

public:
    LRUFileCachePriority() = default;

    Iterator add(
        const Key & key,
        size_t offset,
        size_t size,
        KeyTransactionCreatorPtr key_transaction_creator,
        const CachePriorityQueueGuard::Lock &) override;

    bool contains(const Key & key, size_t offset, const CachePriorityQueueGuard::Lock &) override;

    void removeAll(const CachePriorityQueueGuard::Lock &) override;

    Iterator getLowestPriorityIterator(const CachePriorityQueueGuard::Lock &) override;

    size_t getElementsNum(const CachePriorityQueueGuard::Lock &) const override;

private:
    LRUQueue queue;
    Poco::Logger * log = &Poco::Logger::get("LRUFileCachePriority");
};

class LRUFileCachePriority::LRUFileCacheIterator : public IFileCachePriority::IIterator
{
public:
    LRUFileCacheIterator(
        LRUFileCachePriority * cache_priority_,
        LRUFileCachePriority::LRUQueueIterator queue_iter_);

    void next(const CachePriorityQueueGuard::Lock &) const override { queue_iter++; }

    bool valid(const CachePriorityQueueGuard::Lock &) const override { return queue_iter != cache_priority->queue.end(); }

    const Key & key() const override { return queue_iter->key; }

    size_t offset() const override { return queue_iter->offset; }

    size_t size() const override { return queue_iter->size; }

    size_t hits() const override { return queue_iter->hits; }

    KeyTransactionPtr createKeyTransaction(const CachePriorityQueueGuard::Lock &) override;

    Iterator remove(const CachePriorityQueueGuard::Lock &) override;

    void incrementSize(ssize_t size_increment, const CachePriorityQueueGuard::Lock &) override;

    size_t use(const CachePriorityQueueGuard::Lock &) override;

private:
    LRUFileCachePriority * cache_priority;
    mutable LRUFileCachePriority::LRUQueueIterator queue_iter;
};

};
