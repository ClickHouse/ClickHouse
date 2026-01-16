#pragma once

#include <Core/Types.h>
#include <Interpreters/Cache/FileSegmentInfo.h>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>
#include <Interpreters/Cache/UserInfo.h>

#include <atomic>
#include <memory>

#include <fmt/ranges.h>

namespace DB
{
struct FileCacheReserveStat;
class EvictionCandidates;
class EvictionInfo;
using EvictionInfoPtr = std::unique_ptr<EvictionInfo>;
struct CacheUsageStatGuard;


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
        std::atomic<size_t> hits = 0;
        std::atomic<bool> invalidated = false;

        std::string toString() const;

        bool isEvictingUnlocked() const { return evicting; }
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
        void setEvictingFlag(const LockedKey &) const
        {
            auto prev = evicting.exchange(true, std::memory_order_relaxed);
            chassert(!prev, fmt::format("Evicting flag not reset for key {} offset {}", key, offset));
            UNUSED(prev);
        }

        void setRemoved(const CachePriorityGuard::WriteLock &) { removed = true; evicting = false; }
        bool isRemoved(const CachePriorityGuard::WriteLock &) const { return removed; }

        void resetEvictingFlag() const
        {
            auto prev = evicting.exchange(false, std::memory_order_relaxed);
            chassert(prev);
            UNUSED(prev);
        }

    private:
        mutable std::atomic<bool> evicting = false;
        bool removed = false;
    };
    using EntryPtr = std::shared_ptr<Entry>;

    class Iterator
    {
    public:
        virtual ~Iterator() = default;

        virtual EntryPtr getEntry() const = 0;

        /// Note: IncrementSize unlike decrementSize requires a cache lock, because
        /// it requires more consistency guarantees for eviction.

        virtual void incrementSize(size_t size, const CacheStateGuard::Lock &) = 0;

        virtual void decrementSize(size_t size) = 0;

        virtual bool isValid(const CachePriorityGuard::WriteLock &) = 0;

        virtual void remove(const CachePriorityGuard::WriteLock &) = 0;

        virtual void invalidate() = 0;

        virtual QueueEntryType getType() const = 0;

        virtual const Iterator * getNestedOrThis() const { return this; }
        virtual Iterator * getNestedOrThis() { return this; }

        virtual void check(const CacheStateGuard::Lock &) const {}
    };
    using IteratorPtr = std::shared_ptr<Iterator>;

    struct InvalidatedEntryInfo
    {
        /// Iterator becomes invalid when entry is removed
        /// so we also save the entry here.
        IFileCachePriority::EntryPtr entry;
        IFileCachePriority::IteratorPtr iterator;
    };
    using InvalidatedEntriesInfos = std::vector<InvalidatedEntryInfo>;

    virtual ~IFileCachePriority() = default;

    enum class Type
    {
        LRU,
        SLRU,
        LRU_OVERCOMMIT,
        SLRU_OVERCOMMIT,
    };
    virtual Type getType() const = 0;

    size_t getSizeLimit(const CacheStateGuard::Lock &) const { return max_size; }
    size_t getSizeLimitApprox() const { return max_size.load(std::memory_order_relaxed); }

    size_t getElementsLimit(const CacheStateGuard::Lock &) const { return max_elements; }
    size_t getElementsLimitApprox() const { return max_elements.load(std::memory_order_relaxed); }

    virtual bool isOvercommitEviction() const { return false; }

    virtual size_t getSize(const CacheStateGuard::Lock &) const = 0;
    virtual size_t getSizeApprox() const = 0;

    virtual size_t getElementsCount(const CacheStateGuard::Lock &) const = 0;
    virtual size_t getElementsCountApprox() const = 0;

    virtual double getSLRUSizeRatio() const { return 0; }

    virtual std::string getStateInfoForLog(const CacheStateGuard::Lock &) const = 0;
    virtual void check(const CacheStateGuard::Lock &) const;

    virtual EvictionInfoPtr collectEvictionInfo(
        size_t size,
        size_t elements,
        IFileCachePriority::Iterator * reservee,
        bool is_total_space_cleanup,
        bool is_dynamic_resize,
        const IFileCachePriority::UserInfo & user,
        const CacheStateGuard::Lock &) = 0;

    enum class IterationResult : uint8_t
    {
        BREAK,
        CONTINUE,
    };

    using IterateFunc = std::function<IterationResult(LockedKey &, const FileSegmentMetadataPtr &)>;
    virtual void iterate(
        IterateFunc func,
        FileCacheReserveStat & stat,
        const CachePriorityGuard::ReadLock &) = 0;

    /// Throws exception if there is not enough size to fit it.
    virtual IteratorPtr add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const UserInfo & user,
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock *,
        bool best_effort = false) = 0;

    /// `reservee` is the entry for which are reserving now.
    /// It does not exist, if it is the first space reservation attempt
    /// for the corresponding file segment.
    virtual bool canFit( /// NOLINT
        size_t size,
        size_t elements,
        const CacheStateGuard::Lock &,
        IteratorPtr reservee = nullptr,
        bool best_effort = false) const = 0;

    virtual bool tryIncreasePriority(
        Iterator & iterator,
        bool is_space_reservation_complete,
        CachePriorityGuard & queue_guard,
        CacheStateGuard & state_guard) = 0;

    virtual void shuffle(const CachePriorityGuard::WriteLock &) = 0;

    struct IPriorityDump
    {
        virtual ~IPriorityDump() = default;
    };
    using PriorityDumpPtr = std::shared_ptr<IPriorityDump>;

    virtual PriorityDumpPtr dump(const CachePriorityGuard::ReadLock &) = 0;

    /// Collect eviction candidates sufficient to free `size` bytes
    /// and `elements` elements from cache.
    virtual bool collectCandidatesForEviction(
        const EvictionInfo & eviction_info,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        InvalidatedEntriesInfos & invalidated_entries,
        IteratorPtr reservee,
        bool continue_from_last_eviction_pos,
        size_t max_candidates_size,
        bool is_total_space_cleanup,
        const UserInfo & user,
        CachePriorityGuard &,
        CacheStateGuard &) = 0;

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

    virtual bool modifySizeLimits(
        size_t max_size_,
        size_t max_elements_,
        double size_ratio_,
        const CacheStateGuard::Lock &) = 0;

    virtual void resetEvictionPos() = 0;

    static void removeEntries(const std::vector<InvalidatedEntryInfo> & entries, const CachePriorityGuard::WriteLock &);

    struct UsageStat
    {
        size_t size;
        size_t elements;
    };
    virtual std::unordered_map<std::string, UsageStat> getUsageStatPerClient();

    struct HoldSpace;
    using HoldSpacePtr = std::unique_ptr<HoldSpace>;
    /// A space holder implementation, which allows to take hold of
    /// some space in cache given that this space was freed.
    /// Takes hold of the space in constructor and releases it in destructor.
    struct HoldSpace : private boost::noncopyable
    {
        HoldSpace(
            size_t size_,
            size_t elements_,
            IFileCachePriority & priority_,
            const CacheStateGuard::Lock & lock)
            : size(size_), elements(elements_), priority(priority_)
        {
            priority.holdImpl(size, elements, lock);
        }

        void release(const CacheStateGuard::Lock &) { releaseUnlocked(); }

        void merge(HoldSpacePtr other)
        {
            size += other->size;
            elements += other->elements;
            other->size = other->elements = 0;
        }

        ~HoldSpace()
        {
            if (!released)
                releaseUnlocked();
        }

        size_t size;
        size_t elements;
    private:
        IFileCachePriority & priority;
        bool released = false;

        void releaseUnlocked()
        {
            if (released || (!size && !elements))
                return;
            released = true;
            priority.releaseImpl(size, elements);
        }
    };

    virtual size_t getHoldSize() = 0;

    virtual size_t getHoldElements() = 0;

    virtual void setCacheUsageStatGuard(std::shared_ptr<CacheUsageStatGuard>) {}

protected:
    IFileCachePriority(size_t max_size_, size_t max_elements_);

    virtual void holdImpl(size_t /* size */, size_t /* elements */, const CacheStateGuard::Lock &) {}
    virtual void releaseImpl(size_t /* size */, size_t /* elements */) {}

    std::atomic<size_t> max_size = 0;
    std::atomic<size_t> max_elements = 0;
};

}
