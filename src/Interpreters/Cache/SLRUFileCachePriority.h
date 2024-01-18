#pragma once

#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Common/logger_useful.h>


namespace DB
{

/// Based on the SLRU algorithm implementation, the record with the lowest priority is stored at
/// the head of the queue, and the record with the highest priority is stored at the tail.
class SLRUFileCachePriority : public IFileCachePriority
{
public:
    class SLRUIterator;

    SLRUFileCachePriority(size_t max_size_, size_t max_elements_, double size_ratio_);

    size_t getSize(const CacheGuard::Lock & lock) const override;

    size_t getElementsCount(const CacheGuard::Lock &) const override;

    bool canFit(size_t size, const CacheGuard::Lock &) const override;

    IteratorPtr add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const CacheGuard::Lock &,
        bool is_startup = false) override;

    bool collectCandidatesForEviction(
        size_t size,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        IFileCachePriority::IteratorPtr reservee,
        FinalizeEvictionFunc & finalize_eviction_func,
        const CacheGuard::Lock &) override;

    void shuffle(const CacheGuard::Lock &) override;

    std::vector<FileSegmentInfo> dump(const CacheGuard::Lock &) override;

    bool modifySizeLimits(size_t max_size_, size_t max_elements_, double size_ratio_, const CacheGuard::Lock &) override;

private:
    double size_ratio;
    LRUFileCachePriority protected_queue;
    LRUFileCachePriority probationary_queue;
    Poco::Logger * log = &Poco::Logger::get("SLRUFileCachePriority");

    void increasePriority(SLRUIterator & iterator, const CacheGuard::Lock & lock);
};

class SLRUFileCachePriority::SLRUIterator : public IFileCachePriority::Iterator
{
    friend class SLRUFileCachePriority;
public:
    SLRUIterator(
        SLRUFileCachePriority * cache_priority_,
        LRUFileCachePriority::LRUIterator && lru_iterator_,
        bool is_protected_);

    EntryPtr getEntry() const override;

    size_t increasePriority(const CacheGuard::Lock &) override;

    void remove(const CacheGuard::Lock &) override;

    void invalidate() override;

    void updateSize(int64_t size) override;

    QueueEntryType getType() const override { return is_protected ? QueueEntryType::SLRU_Protected : QueueEntryType::SLRU_Probationary; }

private:
    void assertValid() const;

    SLRUFileCachePriority * cache_priority;
    LRUFileCachePriority::LRUIterator lru_iterator;
    const EntryPtr entry;
    bool is_protected;
};

}
