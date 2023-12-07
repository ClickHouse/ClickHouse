#pragma once

#include <memory>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Interpreters/Cache/FileSegmentInfo.h>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>

namespace DB
{
struct FileCacheReserveStat;
class EvictionCandidates;

class IFileCachePriority : private boost::noncopyable
{
public:
    using Key = FileCacheKey;

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

    class Iterator
    {
    public:
        virtual ~Iterator() = default;

        virtual const Entry & getEntry() const = 0;

        virtual size_t increasePriority(const CacheGuard::Lock &) = 0;

        virtual void updateSize(int64_t size) = 0;

        virtual void remove(const CacheGuard::Lock &) = 0;

        virtual void invalidate() = 0;
    };
    using IteratorPtr = std::shared_ptr<Iterator>;

    IFileCachePriority(size_t max_size_, size_t max_elements_);

    virtual ~IFileCachePriority() = default;

    size_t getElementsLimit() const { return max_elements; }

    size_t getSizeLimit() const { return max_size; }

    virtual size_t getSize(const CacheGuard::Lock &) const = 0;

    virtual size_t getElementsCount(const CacheGuard::Lock &) const = 0;

    /// Throws exception if there is not enough size to fit it.
    virtual IteratorPtr add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const CacheGuard::Lock &,
        bool is_startup = false) = 0;

    virtual bool canFit(size_t size, const CacheGuard::Lock &) const = 0;

    virtual void shuffle(const CacheGuard::Lock &) = 0;

    struct QueueEntryDump
    {
        FileSegmentInfo info;
        bool is_protected = false;
    };
    using QueueEntriesDumps = std::vector<QueueEntryDump>;
    virtual QueueEntriesDumps dump(FileCache & cache, const CacheGuard::Lock &) = 0;

    using FinalizeEvictionFunc = std::function<void(const CacheGuard::Lock & lk)>;
    virtual bool collectCandidatesForEviction(
        size_t size,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        FinalizeEvictionFunc & finalize_eviction_func,
        const CacheGuard::Lock &) = 0;

protected:
    const size_t max_size = 0;
    const size_t max_elements = 0;
};

}
