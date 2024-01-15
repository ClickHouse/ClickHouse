#pragma once

#include <memory>
#include <mutex>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>

namespace DB
{

/// IFileCachePriority is used to maintain the priority of cached data.
class IFileCachePriority : private boost::noncopyable
{
public:
    using Key = FileCacheKey;
    using KeyAndOffset = FileCacheKeyAndOffset;

    struct Entry
    {
        Entry(const Key & key_, size_t offset_, size_t size_, KeyMetadataPtr key_metadata_)
            : key(key_), offset(offset_), size(size_), key_metadata(key_metadata_) {}

        Entry(const Entry & other)
            : key(other.key), offset(other.offset), size(other.size.load()), hits(other.hits), key_metadata(other.key_metadata) {}

        const Key key;
        const size_t offset;
        std::atomic<size_t> size;
        size_t hits = 0;
        const KeyMetadataPtr key_metadata;
    };

    /// Provides an iterator to traverse the cache priority. Under normal circumstances,
    /// the iterator can only return the records that have been directly swapped out.
    /// For example, in the LRU algorithm, it can traverse all records, but in the LRU-K, it
    /// can only traverse the records in the low priority queue.
    class IIterator
    {
    public:
        virtual ~IIterator() = default;

        virtual size_t use(const CacheGuard::Lock &) = 0;

        virtual void remove(const CacheGuard::Lock &) = 0;

        virtual const Entry & getEntry() const = 0;

        virtual Entry & getEntry() = 0;

        virtual void invalidate() = 0;

        virtual void updateSize(int64_t size) = 0;
    };

    using Iterator = std::shared_ptr<IIterator>;
    using ConstIterator = std::shared_ptr<const IIterator>;

    enum class IterationResult
    {
        BREAK,
        CONTINUE,
        REMOVE_AND_CONTINUE,
    };
    using IterateFunc = std::function<IterationResult(LockedKey &, const FileSegmentMetadataPtr &)>;

    IFileCachePriority(size_t max_size_, size_t max_elements_) : max_size(max_size_), max_elements(max_elements_) {}

    virtual ~IFileCachePriority() = default;

    size_t getElementsLimit() const { return max_elements; }

    size_t getSizeLimit() const { return max_size; }

    virtual size_t getSize(const CacheGuard::Lock &) const = 0;

    virtual size_t getElementsCount(const CacheGuard::Lock &) const = 0;

    virtual Iterator add(
        KeyMetadataPtr key_metadata, size_t offset, size_t size, const CacheGuard::Lock &) = 0;

    virtual void pop(const CacheGuard::Lock &) = 0;

    virtual void removeAll(const CacheGuard::Lock &) = 0;

    /// From lowest to highest priority.
    virtual void iterate(IterateFunc && func, const CacheGuard::Lock &) = 0;

    virtual void shuffle(const CacheGuard::Lock &) = 0;

private:
    const size_t max_size = 0;
    const size_t max_elements = 0;
};

}
