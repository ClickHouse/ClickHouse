#pragma once
#include <Interpreters/Cache/IFileCachePriority.h>


namespace DB
{

class LockedCachePriority
{
public:
    LockedCachePriority(const CacheGuard::Lock & lock_, IFileCachePriority & priority_queue_)
        : lock(lock_), queue(priority_queue_) {}

    size_t getElementsLimit() const { return queue.max_elements; }

    size_t getSizeLimit() const { return queue.max_size; }

    size_t getSize() const { return queue.getSize(); }

    size_t getElementsCount() const { return queue.getElementsCount(); }

    IFileCachePriority::Iterator add(const FileCacheKey & key, size_t offset, size_t size, std::weak_ptr<KeyMetadata> key_metadata)
    {
        return queue.add(key, offset, size, key_metadata);
    }

    void pop() { queue.pop(); }

    void removeAll() { queue.removeAll(); }

    void iterate(IFileCachePriority::IterateFunc && func) { queue.iterate(std::move(func)); }

private:
    [[maybe_unused]] const CacheGuard::Lock & lock;
    IFileCachePriority & queue;
};

class LockedCachePriorityIterator
{
public:
    LockedCachePriorityIterator(const CacheGuard::Lock & lock_, IFileCachePriority::Iterator & iterator_)
        : lock(lock_), iterator(iterator_) {}

    IFileCachePriority::Entry & operator *() { return **iterator; }
    const IFileCachePriority::Entry & operator *() const { return **iterator; }

    size_t use() { return iterator->use(); }

    void incrementSize(ssize_t size) { return iterator->incrementSize(size); }

    IFileCachePriority::Iterator remove() { return iterator->remove(); }

private:
    [[maybe_unused]] const CacheGuard::Lock & lock;
    IFileCachePriority::Iterator & iterator;
};

using FileCachePriorityPtr = std::unique_ptr<IFileCachePriority>;

}
