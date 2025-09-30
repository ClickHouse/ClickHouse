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

///  Wrapper for IFilecachePriority that keeps two IFilecachePriority inside:
///  Data: for `.bin`, `.mrk` and etc files
///  System: for indexes, `.json`, `.txt` files.
///  Such separations might be performance-useful.
class SplitFileCachePriority : public IFileCachePriority
{
public:
    class SplitIterator;
    using CachePriorityCreatorFunction
        = std::function<IFileCachePriorityPtr(size_t max_size, size_t max_elements, double size_ratio, String description)>;
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

    size_t getSize(const CachePriorityGuard::Lock &) const override;

    size_t getElementsCount(const CachePriorityGuard::Lock &) const override;

    size_t getSizeApprox() const override;

    size_t getElementsCountApprox() const override;

    std::string getStateInfoForLog(const CachePriorityGuard::Lock & lock) const override;


    bool canFit( /// NOLINT
        size_t size,
        size_t elements,
        const CachePriorityGuard::Lock &,
        IteratorPtr,
        bool) const override;

    IteratorPtr
    add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const OriginInfo & origin,
        const CachePriorityGuard::Lock &,
        bool best_effort = false) override;

    bool collectCandidatesForEviction(
        size_t size,
        size_t elements,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        const OriginInfo & origin,
        const CachePriorityGuard::Lock &) override;

    CollectStatus collectCandidatesForEviction(
        size_t desired_size,
        size_t desired_elements_count,
        size_t max_candidates_to_evict,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        const CachePriorityGuard::Lock &) override;

    void shuffle(const CachePriorityGuard::Lock &) override;

    PriorityDumpPtr dump(const CachePriorityGuard::Lock &) override;


    bool modifySizeLimits(size_t max_size_, size_t max_elements_, double size_ratio_, const CachePriorityGuard::Lock &) override;

    void iterate(IterateFunc func, const CachePriorityGuard::Lock & lock) override
    {
        priorities_holder[SegmentType::Data]->iterate(func, lock);
        priorities_holder[SegmentType::System]->iterate(func, lock);
    }

private:
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
    SplitIterator(IFileCachePriority * inner_cache_priority, IteratorPtr iterator_, FileSegmentKeyType type);

    EntryPtr getEntry() const override;

    size_t increasePriority(const CachePriorityGuard::Lock &) override;

    void remove(const CachePriorityGuard::Lock &) override;

    void invalidate() override;

    void incrementSize(size_t size, const CachePriorityGuard::Lock &) override;

    void decrementSize(size_t size) override;

    QueueEntryType getType() const override
    {
        return type == FileSegmentKeyType::Data ? QueueEntryType::SplitCache_Data : QueueEntryType::SplitCache_System;
    }

    const FileSegmentKeyType type;

private:
    void assertValid() const;

    IFileCachePriority * cache_priority;
    IFileCachePriority::IteratorPtr iterator;
    const std::weak_ptr<Entry> entry;
};

}
