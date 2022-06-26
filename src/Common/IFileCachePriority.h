#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Common/IFileCache.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class IFileCachePriority;
using FileCachePriorityPtr = std::shared_ptr<IFileCachePriority>;

/// IFileCachePriority is used to maintain the priority of cached data.
class IFileCachePriority
{
public:
    struct Key
    {
        UInt128 key;
        String toString() const;

        Key() = default;
        explicit Key(const UInt128 & key_) : key(key_) { }

        bool operator==(const Key & other) const { return key == other.key; }
    };

    class IIterator;
    friend class IIterator;
    using Iterator = std::shared_ptr<IIterator>;

    struct FileCacheRecord
    {
        Key key;
        size_t offset;
        size_t size;
        size_t hits = 0;

        FileCacheRecord(const Key & key_, size_t offset_, size_t size_) : key(key_), offset(offset_), size(size_) { }
    };

    /// It provides an iterator to traverse the cache priority. Under normal circumstances,
    /// the iterator can only return the records that have been directly swapped out.
    /// For example, in the LRU algorithm, it can traverse all records, but in the LRU-K, it
    /// can only traverse the records in the low priority queue.
    class IIterator
    {
    public:
        virtual ~IIterator() = default;

        virtual void next() { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support next() for IIterator."); }

        virtual bool valid() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support valid() for IIterator."); }

        ///  Mark a cache record as recently used, it will update the priority
        /// of the cache record according to different cache algorithms.
        virtual void use(std::lock_guard<std::mutex> &)
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support use() for IIterator.");
        }

        /// Deletes an existing cached record.
        virtual void remove(std::lock_guard<std::mutex> &)
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support remove() for IIterator.");
        }

        virtual Iterator getSnapshot() { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support getSnapshot() for IIterator."); }

        virtual void incrementSize(size_t, std::lock_guard<std::mutex> &)
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support incrementSize() for IIterator.");
        }

        virtual Key key() const = 0;

        virtual size_t offset() const = 0;

        virtual size_t size() const = 0;

        virtual size_t hits() const = 0;
    };

public:
    virtual ~IFileCachePriority() = default;

    /// Add a cache record that did not exist before, and throw a
    /// logical exception if the cache block already exists.
    virtual Iterator add(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock) = 0;

    /// Query whether a cache record exists. If it exists, return true. If not, return false.
    virtual bool contains(const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock) = 0;

    virtual void removeAll(std::lock_guard<std::mutex> & cache_lock) = 0;

    /// Returns an iterator pointing to the lowest priority cached record.
    /// We can traverse all cached records through the iterator's next().
    virtual Iterator getNewIterator(std::lock_guard<std::mutex> & cache_lock) = 0;

    virtual size_t getElementsNum(std::lock_guard<std::mutex> & cache_lock) const = 0;

    size_t getCacheSize(std::lock_guard<std::mutex> &) const { return cache_size; }

    virtual std::string toString(std::lock_guard<std::mutex> & cache_lock) const = 0;

protected:
    size_t max_cache_size = 0;
    size_t cache_size = 0;
};
};
