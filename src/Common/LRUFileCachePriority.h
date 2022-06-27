#pragma once

#include <Common/IFileCachePriority.h>

namespace DB
{

/// Based on the LRU algorithm implementation, the record with the lowest priority is stored at
/// the head of the queue, and the record with the highest priority is stored at the tail.
class LRUFileCachePriority : public IFileCachePriority
{
public:
    using LRUQueue = std::list<FileCacheRecord>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    class LRUFileCacheIterator : public IIterator
    {
    public:
        LRUFileCacheIterator(LRUFileCachePriority * file_cache_, LRUQueueIterator queue_iter_)
            : file_cache(file_cache_), queue_iter(queue_iter_)
        {
        }

        void next() const override { queue_iter++; }

        bool valid() const override { return (file_cache->queue.size() && (queue_iter != file_cache->queue.end())); }

        Key key() const override { return queue_iter->key; }

        size_t offset() const override { return queue_iter->offset; }

        size_t size() const override { return queue_iter->size; }

        size_t hits() const override { return queue_iter->hits; }

        WriteIterator getWriteIterator() const override { return std::make_shared<LRUFileCacheIterator>(file_cache, queue_iter); }

        void seekToLowestPriority() const override { queue_iter = file_cache->queue.begin(); }

        void remove(std::lock_guard<std::mutex> &) override
        {
            file_cache->cache_size -= queue_iter->size;
            file_cache->queue.erase(queue_iter);
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
        mutable LRUQueueIterator queue_iter;
    };

public:
    LRUFileCachePriority() = default;

    WriteIterator add(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> &) override
    {
        auto iter = queue.insert(queue.end(), FileCacheRecord(key, offset, size));
        cache_size += size;
        return std::make_shared<LRUFileCacheIterator>(this, iter);
    }

    bool contains(const Key & key, size_t offset, std::lock_guard<std::mutex> &) override
    {
        for (const auto & record : queue)
        {
            if (key == record.key && offset == record.offset)
                return true;
        }
        return false;
    }

    void removeAll(std::lock_guard<std::mutex> &) override
    {
        queue.clear();
        cache_size = 0;
    }

    ReadIterator getNewIterator(std::lock_guard<std::mutex> &) override
    {
        return std::make_shared<const LRUFileCacheIterator>(this, queue.begin());
    }

    size_t getElementsNum(std::lock_guard<std::mutex> &) const override { return queue.size(); }

    std::string toString(std::lock_guard<std::mutex> &) const override { return {}; }

private:
    LRUQueue queue;
};

};
