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
struct LockedKey;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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

        const Key key;
        const size_t offset;
        size_t size;
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

        virtual std::shared_ptr<IIterator> remove(const CacheGuard::Lock &) = 0;

        virtual const Entry & getEntry() const = 0;

        virtual Entry & getEntry() = 0;

        virtual void annul() = 0;

        virtual void updateSize(ssize_t size) = 0;
    };

    using Iterator = std::shared_ptr<IIterator>;
    using ConstIterator = std::shared_ptr<const IIterator>;

    enum class IterationResult
    {
        BREAK,
        CONTINUE,
        REMOVE_AND_CONTINUE,
    };
    using IterateFunc = std::function<IterationResult(const Entry &, LockedKey &)>;

    IFileCachePriority(size_t max_size_, size_t max_elements_) : max_size(max_size_), max_elements(max_elements_) {}

    virtual ~IFileCachePriority() = default;

    size_t getElementsLimit() const { return max_elements; }

    size_t getSizeLimit() const { return max_size; }

    virtual size_t getSize(const CacheGuard::Lock &) const = 0;

    virtual size_t getElementsCount(const CacheGuard::Lock &) const = 0;

    virtual Iterator add(
        const Key & key, size_t offset, size_t size,
        KeyMetadataPtr key_metadata, const CacheGuard::Lock &) = 0;

    virtual void pop(const CacheGuard::Lock &) = 0;

    virtual void removeAll(const CacheGuard::Lock &) = 0;

    virtual void iterate(IterateFunc && func, const CacheGuard::Lock &) = 0;

private:
    const size_t max_size = 0;
    const size_t max_elements = 0;
};

};
