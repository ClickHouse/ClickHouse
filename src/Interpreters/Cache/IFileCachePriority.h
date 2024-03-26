#pragma once

#include <memory>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Interpreters/Cache/FileSegmentInfo.h>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>
#include <Interpreters/Cache/UserInfo.h>

namespace DB
{
struct FileCacheReserveStat;
class EvictionCandidates;

class IFileCachePriority : private boost::noncopyable
{
public:
    using Key = FileCacheKey;
    using QueueEntryType = FileCacheQueueEntryType;
    using UserInfo = FileCacheUserInfo;
    using UserID = UserInfo::UserID;

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
    using EntryPtr = std::shared_ptr<Entry>;

    class Iterator
    {
    public:
        virtual ~Iterator() = default;

        virtual EntryPtr getEntry() const = 0;

        virtual size_t increasePriority(const CachePriorityGuard::Lock &) = 0;

        virtual void updateSize(int64_t size) = 0;

        virtual void remove(const CachePriorityGuard::Lock &) = 0;

        virtual void invalidate() = 0;

        virtual QueueEntryType getType() const = 0;
    };
    using IteratorPtr = std::shared_ptr<Iterator>;

    virtual ~IFileCachePriority() = default;

    size_t getElementsLimit(const CachePriorityGuard::Lock &) const { return max_elements; }

    size_t getSizeLimit(const CachePriorityGuard::Lock &) const { return max_size; }

    virtual size_t getSize(const CachePriorityGuard::Lock &) const = 0;

    virtual size_t getSizeApprox() const = 0;

    virtual size_t getElementsCount(const CachePriorityGuard::Lock &) const = 0;

    virtual size_t getElementsCountApprox() const = 0;

    /// Throws exception if there is not enough size to fit it.
    virtual IteratorPtr add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const UserInfo & user,
        const CachePriorityGuard::Lock &,
        bool best_effort = false) = 0;

    /// `reservee` is the entry for which are reserving now.
    /// It does not exist, if it is the first space reservation attempt
    /// for the corresponding file segment.
    virtual bool canFit( /// NOLINT
        size_t size,
        const CachePriorityGuard::Lock &,
        IteratorPtr reservee = nullptr,
        bool best_effort = false) const = 0;

    virtual void shuffle(const CachePriorityGuard::Lock &) = 0;

    struct IPriorityDump
    {
        virtual ~IPriorityDump() = default;
    };
    using PriorityDumpPtr = std::shared_ptr<IPriorityDump>;

    virtual PriorityDumpPtr dump(const CachePriorityGuard::Lock &) = 0;

    using FinalizeEvictionFunc = std::function<void(const CachePriorityGuard::Lock & lk)>;
    virtual bool collectCandidatesForEviction(
        size_t size,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        FinalizeEvictionFunc & finalize_eviction_func,
        const UserID & user_id,
        const CachePriorityGuard::Lock &) = 0;

    virtual bool modifySizeLimits(size_t max_size_, size_t max_elements_, double size_ratio_, const CachePriorityGuard::Lock &) = 0;

protected:
    IFileCachePriority(size_t max_size_, size_t max_elements_);

    size_t max_size = 0;
    size_t max_elements = 0;
};

}
