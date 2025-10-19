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

        std::string toString() const
        { return fmt::format("{}:{}:{} (invalidated: {}, evicting: {})", key, offset, size.load(), invalidated.load(), evicting.load()); }

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
            chassert(!prev);
            UNUSED(prev);
        }

        void setRemoved(const CachePriorityGuard::WriteLock &) { removed = true; }
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
    };
    using IteratorPtr = std::shared_ptr<Iterator>;

    struct UsageStat
    {
        size_t size;
        size_t elements;
    };
    virtual std::unordered_map<std::string, UsageStat> getUsageStatPerClient();

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

        ~HoldSpace()
        {
            if (!released)
                releaseUnlocked();
        }

        const size_t size;
        const size_t elements;
    private:
        IFileCachePriority & priority;
        bool released = false;

        void releaseUnlocked()
        {
            if (released)
                return;
            released = true;
            priority.releaseImpl(size, elements);
        }
    };
    using HoldSpacePtr = std::unique_ptr<HoldSpace>;


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

    virtual size_t getSize(const CacheStateGuard::Lock &) const = 0;
    virtual size_t getSizeApprox() const = 0;

    virtual size_t getElementsCount(const CacheStateGuard::Lock &) const = 0;
    virtual size_t getElementsCountApprox() const = 0;

    virtual double getSLRUSizeRatio() const { return 0; }

    virtual std::string getStateInfoForLog(const CacheStateGuard::Lock &) const = 0;
    virtual void check(const CacheStateGuard::Lock &) const;

    struct QueueEvictionInfo
    {
        size_t size_to_evict = 0;
        size_t elements_to_evict = 0;
        HoldSpacePtr hold_space;

        std::string formatForLog() const;
        bool requiresEviction() const { return size_to_evict || elements_to_evict; }
        bool hasHoldSpace() const { return hold_space != nullptr; }
        void releaseHoldSpace(const CacheStateGuard::Lock & lock) const;
    };
    using QueueID = size_t;
    struct EvictionInfo : public std::map<QueueID, QueueEvictionInfo>, private boost::noncopyable
    {
        EvictionInfo() = default;
        explicit EvictionInfo(QueueID queue_id, QueueEvictionInfo && info)
        {
            add(queue_id, std::move(info));
        }

        ~EvictionInfo()
        {
            if (on_finish_func)
                on_finish_func();
        }

        void setOnFinishFunc(std::function<void()> func) { on_finish_func = func; }

        void add(QueueID queue_id, QueueEvictionInfo && info)
        {
            size_to_evict += info.size_to_evict;
            elements_to_evict += info.elements_to_evict;
            emplace(queue_id, std::move(info));
        }

        size_t size_to_evict = 0;
        size_t elements_to_evict = 0;

        std::string formatForLog() const;
        bool requiresEviction() const { return size_to_evict || elements_to_evict; }
        bool hasHoldSpace() const
        {
            for (const auto & [_, elem] : *this)
                if (elem.hasHoldSpace())
                    return true;
            return false;
        }

        void releaseHoldSpace(const CacheStateGuard::Lock & lock) const
        {
            for (const auto & [_, elem] : *this)
                elem.releaseHoldSpace(lock);
        }

        std::function<void()> on_finish_func;
    };
    using EvictionInfoPtr = std::unique_ptr<EvictionInfo>;

    virtual EvictionInfoPtr collectEvictionInfo(
        size_t size,
        size_t elements,
        IFileCachePriority::Iterator * reservee,
        bool is_total_space_cleanup,
        const CacheStateGuard::Lock &) = 0;

    enum class IterationResult : uint8_t
    {
        BREAK,
        CONTINUE,
        //REMOVE_AND_CONTINUE,
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
        IteratorPtr reservee,
        bool continue_from_last_eviction_pos,
        size_t max_candidates_size,
        bool is_total_space_cleanup,
        const UserID & user_id,
        const CachePriorityGuard::ReadLock &) = 0;

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

    virtual void resetEvictionPos(const CachePriorityGuard::ReadLock &) = 0;

    struct InvalidatedEntryInfo
    {
        /// Iterator becomes invalid when entry is removed
        /// so we also save the entry here.
        IFileCachePriority::EntryPtr entry;
        IFileCachePriority::IteratorPtr iterator;
    };
    static void removeEntries(const std::vector<InvalidatedEntryInfo> & entries, const CachePriorityGuard::WriteLock &);

protected:
    IFileCachePriority(size_t max_size_, size_t max_elements_);

    virtual void holdImpl(size_t /* size */, size_t /* elements */, const CacheStateGuard::Lock &) {}
    virtual void releaseImpl(size_t /* size */, size_t /* elements */) {}

    std::atomic<size_t> max_size = 0;
    std::atomic<size_t> max_elements = 0;
};

}
