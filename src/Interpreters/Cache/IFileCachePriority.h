#pragma once

#include <memory>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Interpreters/Cache/FileSegmentInfo.h>
#include <Interpreters/Cache/Guards.h>
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

        std::string toString() const { return fmt::format("{}:{}:{}", key, offset, size); }

        bool isEvicting(const CachePriorityGuard::Lock &) const { return evicting; }
        bool isEvicting(const LockedKey &) const { return evicting; }
        /// This does not look good to have isEvicting with two options for locks,
        /// but still it is valid as we do setEvicting always under both of them.
        /// (Well, not always - only always for setting it to True,
        /// but for False we have lower guarantees and allow a logical race,
        /// physical race is not possible because the value is atomic).
        /// We can avoid this ambiguity for isEvicting by introducing
        /// a separate lock `EntryGuard::Lock`, it will make this part of code more coherent,
        /// but it will introduce one more mutex while it is avoidable.
        /// Introducing one more mutex just for coherency does not win the trade-off (isn't it?).
        void setEvictingFlag(const LockedKey &, const CachePriorityGuard::Lock &) const
        {
            auto prev = evicting.exchange(true, std::memory_order_relaxed);
            chassert(!prev);
            UNUSED(prev);
        }

        void resetEvictingFlag() const
        {
            auto prev = evicting.exchange(false, std::memory_order_relaxed);
            chassert(prev);
            UNUSED(prev);
        }

    private:
        mutable std::atomic<bool> evicting = false;
    };
    using EntryPtr = std::shared_ptr<Entry>;

    class Iterator
    {
    public:
        virtual ~Iterator() = default;

        virtual EntryPtr getEntry() const = 0;

        virtual size_t increasePriority(const CachePriorityGuard::Lock &) = 0;

        /// Note: IncrementSize unlike decrementSize requires a cache lock, because
        /// it requires more consistency guarantees for eviction.

        virtual void incrementSize(size_t size, const CachePriorityGuard::Lock &) = 0;

        virtual void decrementSize(size_t size) = 0;

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

    virtual std::string getStateInfoForLog(const CachePriorityGuard::Lock &) const = 0;

    virtual void check(const CachePriorityGuard::Lock &) const;

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
        size_t elements,
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

    /// Collect eviction candidates sufficient to free `size` bytes
    /// and `elements` elements from cache.
    virtual bool collectCandidatesForEviction(
        size_t size,
        size_t elements,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IteratorPtr reservee,
        const UserID & user_id,
        const CachePriorityGuard::Lock &) = 0;

    /// Collect eviction candidates sufficient to have `desired_size`
    /// and `desired_elements_num` as current cache state.
    /// Collect no more than `max_candidates_to_evict` elements.
    /// Return SUCCESS status if the first condition is satisfied.
    enum class CollectStatus
    {
        SUCCESS,
        CANNOT_EVICT,
        REACHED_MAX_CANDIDATES_LIMIT,
    };
    virtual CollectStatus collectCandidatesForEviction(
        size_t desired_size,
        size_t desired_elements_count,
        size_t max_candidates_to_evict,
        FileCacheReserveStat & stat,
        EvictionCandidates & candidates,
        const CachePriorityGuard::Lock &) = 0;

    virtual bool modifySizeLimits(
        size_t max_size_,
        size_t max_elements_,
        double size_ratio_,
        const CachePriorityGuard::Lock &) = 0;

    /// A space holder implementation, which allows to take hold of
    /// some space in cache given that this space was freed.
    /// Takes hold of the space in constructor and releases it in destructor.
    struct HoldSpace : private boost::noncopyable
    {
        HoldSpace(
            size_t size_,
            size_t elements_,
            IFileCachePriority & priority_,
            const CachePriorityGuard::Lock & lock)
            : size(size_), elements(elements_), priority(priority_)
        {
            priority.holdImpl(size, elements, lock);
        }

        void release()
        {
            if (released)
                return;
            released = true;
            priority.releaseImpl(size, elements);
        }

        ~HoldSpace()
        {
            if (!released)
                release();
        }

    private:
        const size_t size;
        const size_t elements;
        IFileCachePriority & priority;
        bool released = false;
    };
    using HoldSpacePtr = std::unique_ptr<HoldSpace>;

protected:
    IFileCachePriority(size_t max_size_, size_t max_elements_);

    virtual void holdImpl(size_t /* size */, size_t /* elements */, const CachePriorityGuard::Lock &) {}

    virtual void releaseImpl(size_t /* size */, size_t /* elements */) {}

    std::atomic<size_t> max_size = 0;
    std::atomic<size_t> max_elements = 0;
};

}
