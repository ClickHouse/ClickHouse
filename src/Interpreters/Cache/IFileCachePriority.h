#pragma once

#include <Interpreters/Cache/FileCacheOriginInfo.h>
#include <Core/Types.h>
#include <Interpreters/Cache/FileSegmentInfo.h>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>

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
    using OriginInfo = FileCacheOriginInfo;
    using UserID = OriginInfo::UserID;

    struct Entry
    {
        Entry(const Key & key_, size_t offset_, size_t size_, KeyMetadataPtr key_metadata_);
        Entry(const Entry & other);

        const Key key;
        const size_t offset;
        const KeyMetadataPtr key_metadata;

        std::atomic<size_t> size;
        std::atomic<size_t> hits = 0;

        std::string toString(const std::string & prefix = "") const;

        enum class State
        {
            Active,
            /// Entry is collected for eviction via IFileCachePriority::collectEvictionCandidates
            /// and is being or soon will be removed from filesystem.
            Evicting,
            /// Can only be set in SLRU eviciton policy during moves
            /// in between protected/probationary queues.
            Moving,
            /// Has size 0, will never get non-zero size and must soon be removed from queue.
            Invalidated,
            /// Removed from queue completely.
            Removed,
        };

        /// Be aware that by default this method has relaxed guarantees.
        /// See which locks are used in setter methods below if stronger guarantees are needed.
        State getState() const { return state.load(std::memory_order_relaxed); }

        void setEvictingFlag(const LockedKey &)
        {
            [[maybe_unused]] auto prev = state.exchange(State::Evicting, std::memory_order_relaxed);
            chassert(
                prev == State::Active,
                printUnexpectedState(prev, "Active", "Evicting"));
        }

        void setMovingFlag(const LockedKey &)
        {
            [[maybe_unused]] auto prev = state.exchange(State::Moving, std::memory_order_relaxed);
            chassert(
                prev == State::Active,
                printUnexpectedState(prev, "Active", "Moving"));
        }

        void setRemoved(const CachePriorityGuard::WriteLock &)
        {
            [[maybe_unused]] auto prev = state.exchange(State::Removed, std::memory_order_relaxed);
            chassert(
                prev == State::Active || prev == State::Evicting || prev == State::Invalidated,
                printUnexpectedState(prev, "Active or Evicting or Invalidated", "Removed"));
        }

        void setInvalidatedFlag()
        {
            [[maybe_unused]] auto prev = state.exchange(State::Invalidated);
            /// Active in case of FileCache::remove
            /// Evicting in case of FileCache::tryReserve
            /// Moving in case of SLRU queue moves
            chassert(
                prev == State::Active || prev == State::Evicting || prev == State::Moving,
                printUnexpectedState(prev, "Active or Moving or Evicting", "Invalidated"));
        }

        void resetFlag(State from_state, State to_state = State::Active)
        {
            [[maybe_unused]] auto prev = state.exchange(to_state, std::memory_order_relaxed);
            chassert(
                prev == from_state,
                printUnexpectedState(prev, magic_enum::enum_name(from_state), fmt::format("{}", magic_enum::enum_name(to_state))));
        }

    private:
        std::string printUnexpectedState(
            State prev_state, std::string_view expected_state, std::string type) const
        {
            return fmt::format(
                "Previous state is {}, but expected state to be {} while setting {} flag for {}",
                magic_enum::enum_name(prev_state), expected_state, type, toString());
        }

        std::atomic<State> state = State::Active;
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

        virtual bool isValid(const CachePriorityGuard::WriteLock &) const = 0;

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
        /// so we also save the entry here to be able to check validity of the iterator.
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

    virtual size_t getSize(const CacheStateGuard::Lock &) const = 0;
    virtual size_t getSizeApprox() const = 0;

    virtual size_t getElementsCount(const CacheStateGuard::Lock &) const = 0;
    virtual size_t getElementsCountApprox() const = 0;

    virtual bool isOvercommitEviction() const { return false; }
    virtual double getSLRUSizeRatio() const { return 0; }

    virtual std::string getStateInfoForLog(const CacheStateGuard::Lock &) const = 0;
    /// Check correctness of cache state.
    virtual void check(const CacheStateGuard::Lock &) const;

    virtual EvictionInfoPtr collectEvictionInfo(
        size_t size,
        size_t elements,
        IFileCachePriority::Iterator * reservee,
        bool is_total_space_cleanup,
        const IFileCachePriority::OriginInfo & origin,
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
        const OriginInfo & origin_info = {},
        bool best_effort = false) const = 0;

    virtual bool tryIncreasePriority(
        Iterator & iterator,
        bool is_space_reservation_complete,
        CachePriorityGuard & queue_guard,
        CacheStateGuard & state_guard) = 0;

    virtual void shuffle(const CachePriorityGuard::WriteLock &) = 0;

    struct IPriorityDump
    {
        std::vector<FileSegmentInfo> infos;
        IPriorityDump() = default;
        explicit IPriorityDump(const std::vector<FileSegmentInfo> & infos_) : infos(infos_) {}
        void merge(const IPriorityDump & other) { infos.insert(infos.end(), other.infos.begin(), other.infos.end()); }
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
        const OriginInfo & origin_info,
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

    /// Compute eviction info needed to resize the cache to the given limits.
    /// Unlike collectEvictionInfo which takes total amounts to evict,
    /// this method takes desired limits and computes per-sub-queue eviction
    /// correctly for priority types with internal structure (e.g., SLRU).
    virtual EvictionInfoPtr collectEvictionInfoForResize(
        size_t desired_max_size,
        size_t desired_max_elements,
        const OriginInfo & origin_info,
        const CacheStateGuard::Lock & lock) = 0;

    virtual void resetEvictionPos() = 0;

    /// Remove given queue entries for the queue.
    /// Used to cleanup invalidated queue entries.
    static void removeEntries(const std::vector<InvalidatedEntryInfo> & entries, const CachePriorityGuard::WriteLock &);

    struct UsageStat
    {
        size_t size;
        size_t elements;
    };
    virtual std::unordered_map<std::string, UsageStat> getUsageStatPerClient();

    class HoldSpace;
    using HoldSpacePtr = std::unique_ptr<HoldSpace>;
    /// A space holder implementation, which allows to take hold of
    /// some space in cache given that this space was freed.
    /// Takes hold of the space in constructor and releases it in destructor.
    class HoldSpace : private boost::noncopyable
    {
    public:
        HoldSpace(
            size_t size_,
            size_t elements_,
            IFileCachePriority & priority_,
            const CacheStateGuard::Lock & lock)
            : size(size_), elements(elements_), priority(priority_)
        {
            priority.holdImpl(size, elements, lock);
        }

        size_t getSize() const { return size; }

        size_t getElements() const { return elements; }

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

    private:
        size_t size;
        size_t elements;
        IFileCachePriority & priority;
        bool released = false;

        void releaseUnlocked()
        {
            if (released || (!size && !elements))
                return;
            released = true;
            priority.releaseImpl(size, elements);
            size = elements = 0;
        }
    };

    virtual size_t getHoldSize() = 0;

    virtual size_t getHoldElements() = 0;

    virtual void setCacheUsageStatGuard(std::shared_ptr<CacheUsageStatGuard>) {}

protected:
    IFileCachePriority(size_t max_size_, size_t max_elements_);

    virtual void holdImpl(size_t /* size */, size_t /* elements */, const CacheStateGuard::Lock &) {}
    /// No lock is required in releaseImpl unlike holdImpl,
    /// because for releasing hold space we do not need strong guarantees.
    virtual void releaseImpl(size_t /* size */, size_t /* elements */) {}

    std::atomic<size_t> max_size = 0;
    std::atomic<size_t> max_elements = 0;
};

using IFileCachePriorityPtr = std::unique_ptr<IFileCachePriority>;

}
