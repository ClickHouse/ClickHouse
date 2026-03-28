#pragma once

#include <memory>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileSegmentInfo.h>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Common/logger_useful.h>


namespace DB
{

/**
 * Wrapper for IFilecachePriority that keeps two IFilecachePriority inside:
 * Data: for `.bin`, `.mrk` and etc files
 * System: for indexes, `.json`, `.txt` files.
 * Such separations might be performance-useful.
 */
class SplitFileCachePriority : public IFileCachePriority
{
public:
    class SplitIterator;
    using CachePriorityCreatorFunction
        = std::function<IFileCachePriorityPtr(size_t max_size, size_t max_elements, double size_ratio, size_t overcommit_eviction_evict_step, String description)>;
    using IFileCachePriorityPtr = std::unique_ptr<IFileCachePriority>;
    using SegmentType = FileSegmentKeyType;
    using PriorityPerType = std::unordered_map<SegmentType, IFileCachePriorityPtr>;

    SplitFileCachePriority(
        CachePriorityCreatorFunction creator_function,
        size_t max_size_,
        size_t max_elements_,
        double size_ratio,
        double system_segment_size_ratio_,
        const std::string & description_ = "none");

    Type getType() const override { return priorities_holder.at(SegmentType::Data)->getType(); }

    size_t getSize(const CacheStateGuard::Lock &) const override;
    size_t getSizeApprox() const override;

    size_t getElementsCount(const CacheStateGuard::Lock &) const override;
    size_t getElementsCountApprox() const override;

    std::string getStateInfoForLog(const CacheStateGuard::Lock & lock) const override;

    bool canFit( /// NOLINT
        size_t size,
        size_t elements,
        const CacheStateGuard::Lock &,
        IteratorPtr reservee = nullptr,
        const OriginInfo & origin_info = {},
        bool best_effort = false) const override;

    IteratorPtr add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock *,
        bool best_effort = false) override;

    bool tryIncreasePriority(
        Iterator & iterator,
        bool is_space_reservation_complete,
        CachePriorityGuard & queue_guard,
        CacheStateGuard & state_guard) override;

    EvictionInfoPtr collectEvictionInfo(
        size_t size,
        size_t elements,
        IFileCachePriority::Iterator * reservee,
        bool is_total_space_cleanup,
        const IFileCachePriority::OriginInfo & origin,
        const CacheStateGuard::Lock &) override;

    bool collectCandidatesForEviction(
        const EvictionInfo & eviction_info,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        InvalidatedEntriesInfos & invalidated_entries,
        IFileCachePriority::IteratorPtr reservee,
        bool continue_from_last_eviction_pos,
        size_t max_candidates_size,
        bool is_total_space_cleanup,
        const OriginInfo & origin_info,
        CachePriorityGuard &,
        CacheStateGuard &) override;

    void iterate(
        IterateFunc func,
        FileCacheReserveStat & stat,
        const CachePriorityGuard::ReadLock & lock) override;

    void shuffle(const CachePriorityGuard::WriteLock &) override;

    PriorityDumpPtr dump(const CachePriorityGuard::ReadLock &) override;

    bool modifySizeLimits(
        size_t max_size_,
        size_t max_elements_,
        double size_ratio_,
        const CacheStateGuard::Lock &) override;

    EvictionInfoPtr collectEvictionInfoForResize(
        size_t desired_max_size,
        size_t desired_max_elements,
        const OriginInfo & origin_info,
        const CacheStateGuard::Lock & lock) override;

    void resetEvictionPos() override;

protected:
    size_t getHoldSize() override;

    size_t getHoldElements() override;

private:
    SegmentType getPriorityType(const SegmentType & segment_type) const;

    PriorityPerType priorities_holder;
    double system_segment_size_ratio;
    size_t max_data_segment_size;
    size_t max_data_segment_elements;
    size_t max_system_segment_size;
    size_t max_system_segment_elements;

    LoggerPtr log;
};


class SplitFileCachePriority::SplitIterator : public IFileCachePriority::Iterator
{
    friend class SLRUFileCachePriority;

public:
    SplitIterator(
        IFileCachePriority * inner_cache_priority,
        IteratorPtr iterator_,
        FileSegmentKeyType type);

    EntryPtr getEntry() const override;

    void remove(const CachePriorityGuard::WriteLock &) override;

    void invalidate() override;

    void incrementSize(size_t size, const CacheStateGuard::Lock &) override;

    void decrementSize(size_t size) override;

    QueueEntryType getType() const override
    {
        return type == FileSegmentKeyType::Data
            ? QueueEntryType::SplitCache_Data
            : QueueEntryType::SplitCache_System;
    }

    const FileSegmentKeyType type;

private:
    void assertValid() const;

    IFileCachePriority * cache_priority;
    IFileCachePriority::IteratorPtr iterator;
    const std::weak_ptr<Entry> entry;
};

}
