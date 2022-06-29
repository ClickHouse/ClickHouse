#include <Common/LRUFileCachePriority.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IFileCachePriority::WriteIterator
LRUFileCachePriority::add(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> &) override
{
#ifndef NDEBUG
    for (const auto & entry : queue)
    {
        if (entry.key == key && entry.offset == offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to add duplicate queue entry to queue. (Key: {}, offset: {}, size: {})",
                entry.key.toString(),
                entry.offset,
                entry.size);
    }
#endif
    auto iter = queue.insert(queue.end(), FileCacheRecord(key, offset, size));
    cache_size += size;
    return std::make_shared<LRUFileCacheIterator>(this, iter);
}

bool LRUFileCachePriority::contains(const Key & key, size_t offset, std::lock_guard<std::mutex> &) override
{
    for (const auto & record : queue)
    {
        if (key == record.key && offset == record.offset)
            return true;
    }
    return false;
}

void LRUFileCachePriority::removeAll(std::lock_guard<std::mutex> &) override
{
    queue.clear();
    cache_size = 0;
}

IFileCachePriority::ReadIterator LRUFileCachePriority::getLowestPriorityReadIterator(std::lock_guard<std::mutex> &) override
{
    return std::make_shared<const LRUFileCacheIterator>(this, queue.begin());
}

IFileCachePriority::WriteIterator LRUFileCachePriority::getLowestPriorityWriteIterator(std::lock_guard<std::mutex> &) override
{
    return std::make_shared<LRUFileCacheIterator>(this, queue.begin());
}

size_t LRUFileCachePriority::getElementsNum(std::lock_guard<std::mutex> &) const override
{
    return queue.size();
}

};
