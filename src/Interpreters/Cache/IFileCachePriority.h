#pragma once

#include <memory>
#include <mutex>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/Guards.h>

namespace DB
{

class IFileCachePriority;
using FileCachePriorityPtr = std::unique_ptr<IFileCachePriority>;
struct KeyTransaction;
using KeyTransactionPtr = std::shared_ptr<KeyTransaction>;
struct KeyTransactionCreator;
using KeyTransactionCreatorPtr = std::unique_ptr<KeyTransactionCreator>;

/// IFileCachePriority is used to maintain the priority of cached data.
class IFileCachePriority
{
friend class LockedCachePriority;
public:
    using Key = FileCacheKey;
    using KeyAndOffset = FileCacheKeyAndOffset;

    struct Entry
    {
        Key key;
        size_t offset;
        size_t size;
        size_t hits = 0;
        mutable KeyTransactionCreatorPtr key_transaction_creator;

        KeyTransactionPtr createKeyTransaction() const;

        Entry(const Key & key_, size_t offset_, size_t size_,
              KeyTransactionCreatorPtr key_transaction_creator_);
    };

    /// Provides an iterator to traverse the cache priority. Under normal circumstances,
    /// the iterator can only return the records that have been directly swapped out.
    /// For example, in the LRU algorithm, it can traverse all records, but in the LRU-K, it
    /// can only traverse the records in the low priority queue.
    class IIterator
    {
    friend class LockedCachePriorityIterator;
    public:
        virtual ~IIterator() = default;

    protected:
        virtual Entry & operator *() = 0;
        virtual const Entry & operator *() const = 0;

        virtual size_t use() = 0;

        virtual void incrementSize(ssize_t) = 0;

        virtual std::shared_ptr<IIterator> remove() = 0;
    };

    using Iterator = std::shared_ptr<IIterator>;
    using ConstIterator = std::shared_ptr<const IIterator>;

    enum class IterationResult
    {
        BREAK,
        CONTINUE,
        REMOVE_AND_CONTINUE,
    };
    using IterateFunc = std::function<IterationResult(const Entry &)>;

    IFileCachePriority(size_t max_size_, size_t max_elements_) : max_size(max_size_), max_elements(max_elements_) {}

    virtual ~IFileCachePriority() = default;

    size_t getElementsLimit() const { return max_elements; }

    size_t getSizeLimit() const { return max_size; }

protected:
    const size_t max_size = 0;
    const size_t max_elements = 0;

    virtual size_t getSize() const = 0;

    virtual size_t getElementsCount() const = 0;

    virtual Iterator add(
        const Key & key, size_t offset, size_t size, KeyTransactionCreatorPtr key_transaction_creator) = 0;

    virtual void pop() = 0;

    virtual void removeAll() = 0;

    virtual void iterate(IterateFunc && func) = 0;
};

class LockedCachePriority
{
public:
    LockedCachePriority(CacheGuard::LockPtr lock_, IFileCachePriority & priority_queue_)
        : lock(lock_), queue(priority_queue_) {}

    size_t getElementsLimit() const { return queue.max_elements; }

    size_t getSizeLimit() const { return queue.max_size; }

    size_t getSize() const { return queue.getSize(); }

    size_t getElementsCount() const { return queue.getElementsCount(); }

    IFileCachePriority::Iterator add(const FileCacheKey & key, size_t offset, size_t size, KeyTransactionCreatorPtr key_transaction_creator) { return queue.add(key, offset, size, std::move(key_transaction_creator)); }

    void pop() { queue.pop(); }

    void removeAll() { queue.removeAll(); }

    void iterate(IFileCachePriority::IterateFunc && func) { queue.iterate(std::move(func)); }

private:
    CacheGuard::LockPtr lock;
    IFileCachePriority & queue;
};

class LockedCachePriorityIterator
{
public:
    LockedCachePriorityIterator(CacheGuard::LockPtr lock_, IFileCachePriority::Iterator & iterator_)
        : lock(lock_), iterator(iterator_) {}

    IFileCachePriority::Entry & operator *() { return **iterator; }
    const IFileCachePriority::Entry & operator *() const { return **iterator; }

    size_t use() { return iterator->use(); }

    void incrementSize(ssize_t size) { return iterator->incrementSize(size); }

    IFileCachePriority::Iterator remove() { return iterator->remove(); }

private:
    CacheGuard::LockPtr lock;
    IFileCachePriority::Iterator & iterator;
};

using FileCachePriorityPtr = std::unique_ptr<IFileCachePriority>;

};
