#pragma once

#include <list>
#include <Common/IFileCachePriority.h>

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

    WriteIterator add(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> &) override;

    bool contains(const Key & key, size_t offset, std::lock_guard<std::mutex> &) override;

    void removeAll(std::lock_guard<std::mutex> &) override;

    ReadIterator getLowestPriorityReadIterator(std::lock_guard<std::mutex> &) override;

    WriteIterator getLowestPriorityWriteIterator(std::lock_guard<std::mutex> &) override;

    size_t getElementsNum(std::lock_guard<std::mutex> &) const override;

private:
    LRUQueue queue;
};

class LRUFileCachePriority::LRUFileCacheIterator : public IFileCachePriority::IIterator
{
public:
    LRUFileCacheIterator(LRUFileCachePriority * file_cache_, LRUFileCachePriority::LRUQueueIterator queue_iter_)
        : file_cache(file_cache_), queue_iter(queue_iter_)
    {
    }

    void next() const override { queue_iter++; }

    bool valid() const override { return queue_iter != file_cache->queue.end(); }

    const Key & key() const override { return queue_iter->key; }

    size_t offset() const override { return queue_iter->offset; }

    size_t size() const override { return queue_iter->size; }

    size_t hits() const override { return queue_iter->hits; }

    void remove(std::lock_guard<std::mutex> &) override
    {
        auto remove_iter = queue_iter;
        queue_iter++;
        file_cache->cache_size -= remove_iter->size;
        file_cache->queue.erase(remove_iter);
    }

    void incrementSize(size_t size_increment, std::lock_guard<std::mutex> &) override
    {
        file_cache->cache_size += size_increment;
        queue_iter->size += size_increment;
    }

    void use(std::lock_guard<std::mutex> &) override
    {
        queue_iter->hits++;
        file_cache->queue.splice(file_cache->queue.end(), file_cache->queue, queue_iter);
    }

private:
    mutable LRUFileCachePriority * file_cache;
    mutable LRUFileCachePriority::LRUQueueIterator queue_iter;
};

};
