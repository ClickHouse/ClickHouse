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
struct KeyMetadata;
using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;

/// IFileCachePriority is used to maintain the priority of cached data.
class IFileCachePriority
{
friend class LockedCachePriority;
public:
    using Key = FileCacheKey;
    using KeyAndOffset = FileCacheKeyAndOffset;

    struct Entry
    {
        Entry(const Key & key_, size_t offset_, size_t size_, std::weak_ptr<KeyMetadata> key_metadata_)
            : key(key_) , offset(offset_) , size(size_) , key_metadata(key_metadata_) {}

        KeyMetadataPtr getKeyMetadata() const { return key_metadata.lock(); }

        Key key;
        size_t offset;
        size_t size;
        size_t hits = 0;
        mutable std::weak_ptr<KeyMetadata> key_metadata;
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

    virtual Iterator add(const Key & key, size_t offset, size_t size, std::weak_ptr<KeyMetadata> key_metadata) = 0;

    virtual void pop() = 0;

    virtual void removeAll() = 0;

    virtual void iterate(IterateFunc && func) = 0;
};

};
