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
public:
    class IIterator;
    using Key = FileCacheKey;
    using KeyAndOffset = FileCacheKeyAndOffset;
    using Iterator = std::shared_ptr<IIterator>;
    using ConstIterator = std::shared_ptr<const IIterator>;
    using Lock = CachePriorityQueueGuard::Lock;

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

    /// It provides an iterator to traverse the cache priority. Under normal circumstances,
    /// the iterator can only return the records that have been directly swapped out.
    /// For example, in the LRU algorithm, it can traverse all records, but in the LRU-K, it
    /// can only traverse the records in the low priority queue.
    class IIterator
    {
    public:
        virtual ~IIterator() = default;

        virtual Entry & operator *() = 0;
        virtual const Entry & operator *() const = 0;

        /// Mark a cache record as recently used, it will update the priority
        /// of the cache record according to different cache algorithms.
        /// Return result hits count.
        virtual size_t use(const CachePriorityQueueGuard::Lock &) = 0;

        virtual void incrementSize(ssize_t, const CachePriorityQueueGuard::Lock &) = 0;

        /// Remove current cached record. Return iterator to the next value.
        virtual Iterator remove(const CachePriorityQueueGuard::Lock &) = 0;
    };

    virtual ~IFileCachePriority() = default;

    size_t getCacheSize(const CachePriorityQueueGuard::Lock &) const { return cache_size; }

    virtual size_t getElementsNum(const CachePriorityQueueGuard::Lock &) const = 0;

    /// Lock current priority queue. All methods must be called under this lock.
    CachePriorityQueueGuard::Lock lock() { return guard.lock(); }

    /// Add a cache record that did not exist before, and throw a
    /// logical exception if the cache block already exists.
    virtual Iterator add(
        const Key & key,
        size_t offset,
        size_t size,
        KeyTransactionCreatorPtr key_transaction_creator,
        const CachePriorityQueueGuard::Lock &) = 0;

    virtual void removeAll(const CachePriorityQueueGuard::Lock &) = 0;


    enum class IterationResult
    {
        BREAK,
        CONTINUE,
        REMOVE_AND_CONTINUE,
    };
    using IterateFunc = std::function<IterationResult(const Entry &)>;
    virtual void iterate(IterateFunc && func, const CachePriorityQueueGuard::Lock &) = 0;


protected:
    CachePriorityQueueGuard guard;

    size_t max_cache_size = 0;
    size_t cache_size = 0;
};

using FileCachePriorityPtr = std::unique_ptr<IFileCachePriority>;

};
