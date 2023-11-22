#pragma once

#include <memory>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>

namespace DB
{
struct FileCacheReserveStat;
class EvictionCandidates;

/// IFileCachePriority is used to maintain the priority of cached data.
class IFileCachePriority : private boost::noncopyable
{
public:
    using Key = FileCacheKey;
    using KeyAndOffset = FileCacheKeyAndOffset;

    struct Entry
    {
        Entry(const Key & key_, size_t offset_, size_t size_, KeyMetadataPtr key_metadata_);
        Entry(const Entry & other);

        const Key key;
        const size_t offset;
        const KeyMetadataPtr key_metadata;

        std::atomic<size_t> size;
        size_t hits = 0;
    };

    class IIterator
    {
    public:
        virtual ~IIterator() = default;

        virtual const Entry & getEntry() const = 0;

        virtual size_t increasePriority(const CacheGuard::Lock &) = 0;

        virtual void updateSize(int64_t size) = 0;

        virtual void remove(const CacheGuard::Lock &) = 0;

        virtual void invalidate() = 0;
    };
    using Iterator = std::shared_ptr<IIterator>;

    IFileCachePriority(size_t max_size_, size_t max_elements_);

    virtual ~IFileCachePriority() = default;

    size_t getElementsLimit() const { return max_elements; }

    size_t getSizeLimit() const { return max_size; }

    virtual size_t getSize(const CacheGuard::Lock &) const = 0;

    virtual size_t getElementsCount(const CacheGuard::Lock &) const = 0;

    virtual Iterator add(KeyMetadataPtr key_metadata, size_t offset, size_t size, const CacheGuard::Lock &) = 0;

    virtual void shuffle(const CacheGuard::Lock &) = 0;

    virtual FileSegments dump(const CacheGuard::Lock &) = 0;

    using FinalizeEvictionFunc = std::function<void(const CacheGuard::Lock & lk)>;

    virtual bool collectCandidatesForEviction(
        size_t size,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::Iterator it,
        FinalizeEvictionFunc & finalize_eviction_func,
        const CacheGuard::Lock &) = 0;

protected:
    const size_t max_size = 0;
    const size_t max_elements = 0;
};

}
