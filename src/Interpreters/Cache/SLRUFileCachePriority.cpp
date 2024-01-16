#include <Interpreters/Cache/SLRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/EvictionCandidates.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace
{
    size_t getRatio(size_t total, double ratio)
    {
        return static_cast<size_t>(total * std::clamp(ratio, 0.0, 1.0));
    }
}

SLRUFileCachePriority::SLRUFileCachePriority(
    size_t max_size_,
    size_t max_elements_,
    double size_ratio_)
    : IFileCachePriority(max_size_, max_elements_)
    , size_ratio(size_ratio_)
    , protected_queue(LRUFileCachePriority(getRatio(max_size_, size_ratio), getRatio(max_elements_, size_ratio)))
    , probationary_queue(LRUFileCachePriority(getRatio(max_size_, 1 - size_ratio), getRatio(max_elements_, 1 - size_ratio)))
{
    LOG_DEBUG(
        log, "Using probationary queue size: {}, protected queue size: {}",
        probationary_queue.max_size, protected_queue.max_elements);
}

size_t SLRUFileCachePriority::getSize(const CacheGuard::Lock & lock) const
{
    return protected_queue.getSize(lock) + probationary_queue.getSize(lock);
}

size_t SLRUFileCachePriority::getElementsCount(const CacheGuard::Lock & lock) const
{
    return protected_queue.getElementsCount(lock) + probationary_queue.getElementsCount(lock);
}

bool SLRUFileCachePriority::canFit(size_t size, const CacheGuard::Lock & lock) const
{
    return probationary_queue.canFit(size, lock) || protected_queue.canFit(size, lock);
}

IFileCachePriority::IteratorPtr SLRUFileCachePriority::add( /// NOLINT
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const CacheGuard::Lock & lock,
    bool is_startup)
{
    if (is_startup)
    {
        /// If it is server startup, we put entries in any queue it will fit in,
        /// but with preference for probationary queue,
        /// because we do not know the distribution between queues after server restart.
        if (probationary_queue.canFit(size, lock))
        {
            auto lru_iterator = probationary_queue.add(std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata), lock);
            return std::make_shared<SLRUIterator>(this, std::move(lru_iterator), false);
        }
        else
        {
            auto lru_iterator = protected_queue.add(std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata), lock);
            return std::make_shared<SLRUIterator>(this, std::move(lru_iterator), true);
        }
    }
    else
    {
        auto lru_iterator = probationary_queue.add(std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata), lock);
        return std::make_shared<SLRUIterator>(this, std::move(lru_iterator), false);
    }
}

bool SLRUFileCachePriority::collectCandidatesForEviction(
    size_t size,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    IFileCachePriority::IteratorPtr reservee,
    FinalizeEvictionFunc & finalize_eviction_func,
    const CacheGuard::Lock & lock)
{
    /// If `it` is nullptr, then it is the first space reservation attempt
    /// for a corresponding file segment, so it will be directly put into probationary queue.
    if (!reservee)
    {
        return probationary_queue.collectCandidatesForEviction(size, stat, res, reservee, finalize_eviction_func, lock);
    }

    /// If `it` not nullptr (e.g. is already in some queue),
    /// we need to check in which queue (protected/probationary) it currently is
    /// (in order to know where we need to free space).
    if (!assert_cast<SLRUIterator *>(reservee.get())->is_protected)
    {
        return probationary_queue.collectCandidatesForEviction(size, stat, res, reservee, finalize_eviction_func, lock);
    }

    /// Entry is in protected queue.
    /// Check if we have enough space in protected queue to fit a new size of entry.
    /// `size` is the increment to the current entry.size we want to increase.
    if (protected_queue.canFit(size, lock))
        return true;

    /// If not enough space - we need to "downgrade" lowest priority entries from protected
    /// queue to probationary queue.
    /// The amount of such "downgraded" entries is equal to the amount
    /// required to make space for additionary `size` bytes for entry.
    auto downgrade_candidates = std::make_shared<EvictionCandidates>();
    FileCacheReserveStat downgrade_stat;
    FinalizeEvictionFunc noop;

    if (!protected_queue.collectCandidatesForEviction(size, downgrade_stat, *downgrade_candidates, reservee, noop, lock))
        return false;

    const size_t size_to_downgrade = downgrade_stat.stat.releasable_size;

    if (!probationary_queue.canFit(size_to_downgrade, lock)
        && !probationary_queue.collectCandidatesForEviction(size_to_downgrade, stat, res, reservee, noop, lock))
        return false;

    finalize_eviction_func = [=, this](const CacheGuard::Lock & lk) mutable
    {
        for (const auto & [key, key_candidates] : *downgrade_candidates)
        {
            for (const auto & candidate : key_candidates.candidates)
            {
                auto * candidate_it = assert_cast<SLRUIterator *>(candidate->getQueueIterator().get());
                candidate_it->lru_iterator = probationary_queue.move(candidate_it->lru_iterator, protected_queue, lk);
                candidate_it->is_protected = false;
            }
        }
    };

    return true;
}

void SLRUFileCachePriority::increasePriority(SLRUIterator & iterator, const CacheGuard::Lock & lock)
{
    /// If entry is already in protected queue,
    /// we only need to increase its priority within the protected queue.
    if (iterator.is_protected)
    {
        iterator.lru_iterator.increasePriority(lock);
        return;
    }

    /// Entry is in probationary queue.
    /// We need to move it to protected queue.
    const size_t size = iterator.getEntry()->size;
    if (size > protected_queue.getSizeLimit(lock))
    {
        /// Entry size is bigger than the whole protected queue limit.
        /// This is only possible if protected_queue_size_limit is less than max_file_segment_size,
        /// which is not possible in any realistic cache configuration.
        iterator.lru_iterator.increasePriority(lock);
        return;
    }

    /// Check if there is enough space in protected queue to move entry there.
    /// If not - we need to "downgrade" lowest priority entries from protected
    /// queue to probationary queue.
    EvictionCandidates downgrade_candidates;
    FileCacheReserveStat downgrade_stat;
    FinalizeEvictionFunc noop;

    if (!protected_queue.collectCandidatesForEviction(size, downgrade_stat, downgrade_candidates, {}, noop, lock))
    {
        /// We cannot make space for entry to be moved to protected queue
        /// (not enough releasable file segments).
        /// Then just increase its priority within probationary queue.
        iterator.lru_iterator.increasePriority(lock);
        return;
    }

    /// The amount of such "downgraded" entries is equal to the amount
    /// required to make space for entry we want to insert.
    const size_t size_to_downgrade = downgrade_stat.stat.releasable_count;
    size_t size_to_free = 0;
    if (size_to_downgrade && size_to_downgrade > size)
        size_to_free = size_to_downgrade - size;

    /// Now we need to check if those "downgrade" candidates can actually
    /// be moved to probationary queue.
    EvictionCandidates eviction_candidates;
    FileCacheReserveStat stat;

    if (size_to_free)
    {
        if (!probationary_queue.collectCandidatesForEviction(size_to_free, stat, eviction_candidates, {}, noop, lock))
        {
            /// "downgrade" candidates cannot be moved to probationary queue,
            /// so entry cannot be moved to protected queue as well.
            /// Then just increase its priority within probationary queue.
            iterator.lru_iterator.increasePriority(lock);
            return;
        }
        /// Make space for "downgrade" candidates.
        eviction_candidates.evict(nullptr, lock);
    }

    /// All checks passed, now we can move downgrade candidates to
    /// probationary queue and our entry to protected queue.
    EntryPtr entry = iterator.getEntry();
    iterator.lru_iterator.remove(lock);

    for (const auto & [key, key_candidates] : downgrade_candidates)
    {
        for (const auto & candidate : key_candidates.candidates)
        {
            auto * candidate_it = assert_cast<SLRUIterator *>(candidate->getQueueIterator().get());
            candidate_it->lru_iterator = probationary_queue.move(candidate_it->lru_iterator, protected_queue, lock);
            candidate_it->is_protected = false;
        }
    }

    iterator.lru_iterator = protected_queue.add(entry, lock);
    iterator.is_protected = true;
}

std::vector<FileSegmentInfo> SLRUFileCachePriority::dump(const CacheGuard::Lock & lock)
{
    auto res = probationary_queue.dump(lock);
    auto part_res = protected_queue.dump(lock);
    res.insert(res.end(), part_res.begin(), part_res.end());
    return res;
}

void SLRUFileCachePriority::shuffle(const CacheGuard::Lock & lock)
{
    protected_queue.shuffle(lock);
    probationary_queue.shuffle(lock);
}

bool SLRUFileCachePriority::modifySizeLimits(
    size_t max_size_, size_t max_elements_, double size_ratio_, const CacheGuard::Lock & lock)
{
    if (max_size == max_size_ && max_elements == max_elements_ && size_ratio == size_ratio_)
        return false; /// Nothing to change.

    protected_queue.modifySizeLimits(getRatio(max_size_, size_ratio_), getRatio(max_elements_, size_ratio_), 0, lock);
    probationary_queue.modifySizeLimits(getRatio(max_size_, 1 - size_ratio_), getRatio(max_elements_, 1 - size_ratio_), 0, lock);

    max_size = max_size_;
    max_elements = max_elements_;
    size_ratio = size_ratio_;
    return true;
}

SLRUFileCachePriority::SLRUIterator::SLRUIterator(
    SLRUFileCachePriority * cache_priority_,
    LRUFileCachePriority::LRUIterator && lru_iterator_,
    bool is_protected_)
    : cache_priority(cache_priority_)
    , lru_iterator(lru_iterator_)
    , entry(lru_iterator.getEntry())
    , is_protected(is_protected_)
{
}

SLRUFileCachePriority::EntryPtr SLRUFileCachePriority::SLRUIterator::getEntry() const
{
    chassert(entry == lru_iterator.getEntry());
    return entry;
}

size_t SLRUFileCachePriority::SLRUIterator::increasePriority(const CacheGuard::Lock & lock)
{
    assertValid();
    cache_priority->increasePriority(*this, lock);
    return getEntry()->hits;
}

void SLRUFileCachePriority::SLRUIterator::updateSize(int64_t size)
{
    assertValid();
    lru_iterator.updateSize(size);
}

void SLRUFileCachePriority::SLRUIterator::invalidate()
{
    assertValid();
    lru_iterator.invalidate();
}

void SLRUFileCachePriority::SLRUIterator::remove(const CacheGuard::Lock & lock)
{
    assertValid();
    lru_iterator.remove(lock);
}

void SLRUFileCachePriority::SLRUIterator::assertValid() const
{
    lru_iterator.assertValid();
}

}
