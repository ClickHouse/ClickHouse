#include <Interpreters/Cache/SLRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/EvictionCandidates.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    size_t getRatio(size_t total, double ratio)
    {
        return std::lround(total * std::clamp(ratio, 0.0, 1.0));
    }
}

SLRUFileCachePriority::SLRUFileCachePriority(
    size_t max_size_,
    size_t max_elements_,
    double size_ratio_,
    LRUFileCachePriority::StatePtr probationary_state_,
    LRUFileCachePriority::StatePtr protected_state_)
    : IFileCachePriority(max_size_, max_elements_)
    , size_ratio(size_ratio_)
    , protected_queue(LRUFileCachePriority(getRatio(max_size_, size_ratio),
                                           getRatio(max_elements_, size_ratio),
                                           protected_state_,
                                           "protected"))
    , probationary_queue(LRUFileCachePriority(getRatio(max_size_, 1 - size_ratio),
                                              getRatio(max_elements_, 1 - size_ratio),
                                              probationary_state_,
                                              "probationary"))
{
    LOG_DEBUG(
        log, "Probationary queue {} in size and {} in elements. "
        "Protected queue {} in size and {} in elements",
        probationary_queue.max_size, probationary_queue.max_elements,
        protected_queue.max_size, protected_queue.max_elements);
}

size_t SLRUFileCachePriority::getSize(const CachePriorityGuard::Lock & lock) const
{
    return protected_queue.getSize(lock) + probationary_queue.getSize(lock);
}

size_t SLRUFileCachePriority::getElementsCount(const CachePriorityGuard::Lock & lock) const
{
    return protected_queue.getElementsCount(lock) + probationary_queue.getElementsCount(lock);
}

size_t SLRUFileCachePriority::getSizeApprox() const
{
    return protected_queue.getSizeApprox() + probationary_queue.getSizeApprox();
}

size_t SLRUFileCachePriority::getElementsCountApprox() const
{
    return protected_queue.getElementsCountApprox() + probationary_queue.getElementsCountApprox();
}

bool SLRUFileCachePriority::canFit( /// NOLINT
    size_t size,
    size_t elements,
    const CachePriorityGuard::Lock & lock,
    IteratorPtr reservee,
    bool best_effort) const
{
    if (best_effort)
        return probationary_queue.canFit(size, elements, lock) || protected_queue.canFit(size, elements, lock);

    if (reservee)
    {
        const auto * slru_iterator = assert_cast<SLRUIterator *>(reservee.get());
        if (slru_iterator->is_protected)
            return protected_queue.canFit(size, elements, lock);
        else
            return probationary_queue.canFit(size, elements, lock);
    }
    else
        return probationary_queue.canFit(size, elements, lock);
}

IFileCachePriority::IteratorPtr SLRUFileCachePriority::add( /// NOLINT
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const UserInfo &,
    const CachePriorityGuard::Lock & lock,
    bool is_startup)
{
    IteratorPtr iterator;
    if (is_startup)
    {
        /// If it is server startup, we put entries in any queue it will fit in,
        /// but with preference for probationary queue,
        /// because we do not know the distribution between queues after server restart.
        if (probationary_queue.canFit(size, 1, lock))
        {
            auto lru_iterator = probationary_queue.add(std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata), lock);
            iterator = std::make_shared<SLRUIterator>(this, std::move(lru_iterator), false);
        }
        else
        {
            auto lru_iterator = protected_queue.add(std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata), lock);
            iterator = std::make_shared<SLRUIterator>(this, std::move(lru_iterator), true);
        }
    }
    else
    {
        auto lru_iterator = probationary_queue.add(std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata), lock);
        iterator = std::make_shared<SLRUIterator>(this, std::move(lru_iterator), false);
    }

    if (getSize(lock) > max_size || getElementsCount(lock) > max_elements)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Went beyond limits. Added {} for {} element ({}:{}). Current state: {}",
                        size, iterator->getType(), key_metadata->key, offset, getStateInfoForLog(lock));
    }

    return iterator;
}

bool SLRUFileCachePriority::collectCandidatesForEviction(
    size_t size,
    size_t elements,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    IFileCachePriority::IteratorPtr reservee,
    const UserID & user_id,
    const CachePriorityGuard::Lock & lock)
{
    /// If `it` is nullptr, then it is the first space reservation attempt
    /// for a corresponding file segment, so it will be directly put into probationary queue.
    if (!reservee)
    {
        return probationary_queue.collectCandidatesForEviction(size, elements, stat, res, reservee, user_id, lock);
    }

    /// If `it` not nullptr (e.g. is already in some queue),
    /// we need to check in which queue (protected/probationary) it currently is
    /// (in order to know where we need to free space).
    if (!assert_cast<SLRUIterator *>(reservee.get())->is_protected)
    {
        return probationary_queue.collectCandidatesForEviction(size, elements, stat, res, reservee, user_id, lock);
    }

    /// Entry is in protected queue.
    /// Check if we have enough space in protected queue to fit a new size of entry.
    /// `size` is the increment to the current entry.size we want to increase.
    /// Here `elements` is 0, because entry is already in the protected queue.
    return collectCandidatesForEvictionInProtected(size, elements, stat, res, reservee, user_id, lock);
}

bool SLRUFileCachePriority::collectCandidatesForEvictionInProtected(
    size_t size,
    size_t elements,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    IFileCachePriority::IteratorPtr reservee,
    const UserID & user_id,
    const CachePriorityGuard::Lock & lock)
{
    if (protected_queue.canFit(size, elements, lock))
    {
        return true;
    }

    /// If not enough space - we need to "downgrade" lowest priority entries
    /// from protected queue to probationary queue.

    auto downgrade_candidates = std::make_shared<EvictionCandidates>();
    FileCacheReserveStat downgrade_stat;
    if (!protected_queue.collectCandidatesForEviction(size, elements, downgrade_stat, *downgrade_candidates, reservee, user_id, lock))
    {
        return false;
    }
    else
        chassert(downgrade_candidates->size() > 0);

    if (!probationary_queue.collectCandidatesForEviction(
            downgrade_stat.total_stat.releasable_size, downgrade_stat.total_stat.releasable_count,
            stat, res, reservee, user_id, lock))
    {
        return false;
    }

    auto downgrade_func = [=, this](const CachePriorityGuard::Lock & lk)
    {
        LOG_TEST(log, "Downgrading {} elements from protected to probationary. "
                 "Total size: {}",
                 downgrade_candidates->size(), downgrade_stat.total_stat.releasable_size);

        for (const auto & [key, key_candidates] : *downgrade_candidates)
        {
            for (const auto & candidate : key_candidates.candidates)
                downgrade(candidate->getQueueIterator(), lk);
        }
    };

    if (res.size() > 0)
    {
        /// Downgrade from protected to probationary only after
        /// we free up space in probationary (in order to fit these downgrade candidates).
        res.setFinalizeEvictionFunc(std::move(downgrade_func));
    }
    else
    {
        /// Enough space in probationary queue already to fit our downgrade candidates.
        downgrade_func(lock);
    }

    return true;
}

void SLRUFileCachePriority::downgrade(IteratorPtr iterator, const CachePriorityGuard::Lock & lock)
{
    auto * candidate_it = assert_cast<SLRUIterator *>(iterator.get());
    if (!candidate_it->is_protected)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot downgrade {}: it is already in probationary queue",
                        candidate_it->getEntry()->toString());
    }

    const size_t entry_size = candidate_it->entry->size;
    if (!probationary_queue.canFit(entry_size, 1, lock))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot downgrade {}: not enough space: {}",
                        candidate_it->getEntry()->toString(),
                        probationary_queue.getStateInfoForLog(lock));
    }

    candidate_it->lru_iterator = probationary_queue.move(candidate_it->lru_iterator, protected_queue, lock);
    candidate_it->is_protected = false;
}

void SLRUFileCachePriority::increasePriority(SLRUIterator & iterator, const CachePriorityGuard::Lock & lock)
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
    const size_t entry_size = iterator.getEntry()->size;
    if (entry_size > protected_queue.getSizeLimit(lock))
    {
        /// Entry size is bigger than the whole protected queue limit.
        /// This is only possible if protected_queue_size_limit is less than max_file_segment_size,
        /// which is not possible in any realistic cache configuration.
        iterator.lru_iterator.increasePriority(lock);
        return;
    }

    EntryPtr entry = iterator.getEntry();
    /// We need to remove the entry from probationary first
    /// in order to make space for downgrade from protected.
    iterator.lru_iterator.remove(lock);

    EvictionCandidates eviction_candidates;
    FileCacheReserveStat stat;
    try
    {
        /// Check if there is enough space in protected queue to move entry there.
        /// If not - we need to "downgrade" lowest priority entries from protected
        /// queue to probationary queue.
        ///
        if (!collectCandidatesForEvictionInProtected(
                entry->size, 1, stat, eviction_candidates, nullptr, FileCache::getInternalUser().user_id, lock))
        {
            /// "downgrade" candidates cannot be moved to probationary queue,
            /// so entry cannot be moved to protected queue as well.
            /// Then just increase its priority within probationary queue.
            iterator.lru_iterator = addOrThrow(entry, probationary_queue, lock);
            return;
        }

        eviction_candidates.evict();
        eviction_candidates.finalize(nullptr, lock);
    }
    catch (...)
    {
        iterator.lru_iterator = addOrThrow(entry, probationary_queue, lock);
        throw;
    }

    iterator.lru_iterator = addOrThrow(entry, protected_queue, lock);
    iterator.is_protected = true;
}

LRUFileCachePriority::LRUIterator SLRUFileCachePriority::addOrThrow(
    EntryPtr entry, LRUFileCachePriority & queue, const CachePriorityGuard::Lock & lock)
{
    try
    {
        return queue.add(entry, lock);
    }
    catch (...)
    {
        const auto initial_exception = getCurrentExceptionMessage(true);
        try
        {
            /// We cannot allow a situation that a file exists on filesystem, but
            /// there is no corresponding entry in priority queue for it,
            /// because it will mean that cache became inconsistent.
            /// So let's try to fix the situation.
            auto metadata = entry->key_metadata->tryLock();
            chassert(metadata);
            if (metadata)
            {
                auto segment_metadata = metadata->tryGetByOffset(entry->offset);
                metadata->removeFileSegment(entry->offset, segment_metadata->file_segment->lock());
            }
        }
        catch (...)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Unexpected exception: {} (Initial exception: {}). Cache will become inconsistent",
                            getCurrentExceptionMessage(true), initial_exception);
        }

        /// Let's try to catch such cases in CI.
        chassert(false);
        throw;
    }
}

IFileCachePriority::PriorityDumpPtr SLRUFileCachePriority::dump(const CachePriorityGuard::Lock & lock)
{
    auto res = dynamic_pointer_cast<LRUFileCachePriority::LRUPriorityDump>(probationary_queue.dump(lock));
    auto part_res = dynamic_pointer_cast<LRUFileCachePriority::LRUPriorityDump>(protected_queue.dump(lock));
    res->merge(*part_res);
    return res;
}

void SLRUFileCachePriority::shuffle(const CachePriorityGuard::Lock & lock)
{
    protected_queue.shuffle(lock);
    probationary_queue.shuffle(lock);
}

bool SLRUFileCachePriority::modifySizeLimits(
    size_t max_size_, size_t max_elements_, double size_ratio_, const CachePriorityGuard::Lock & lock)
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
    return entry;
}

size_t SLRUFileCachePriority::SLRUIterator::increasePriority(const CachePriorityGuard::Lock & lock)
{
    assertValid();
    cache_priority->increasePriority(*this, lock);
    cache_priority->check(lock);
    return getEntry()->hits;
}

void SLRUFileCachePriority::SLRUIterator::incrementSize(size_t size, const CachePriorityGuard::Lock & lock)
{
    assertValid();
    lru_iterator.incrementSize(size, lock);
}

void SLRUFileCachePriority::SLRUIterator::decrementSize(size_t size)
{
    assertValid();
    lru_iterator.decrementSize(size);
}

void SLRUFileCachePriority::SLRUIterator::invalidate()
{
    assertValid();
    lru_iterator.invalidate();
}

void SLRUFileCachePriority::SLRUIterator::remove(const CachePriorityGuard::Lock & lock)
{
    assertValid();
    lru_iterator.remove(lock);
}

void SLRUFileCachePriority::SLRUIterator::assertValid() const
{
    lru_iterator.assertValid();
}

std::string SLRUFileCachePriority::getStateInfoForLog(const CachePriorityGuard::Lock & lock) const
{
    return fmt::format("total size {}/{}, elements {}/{}, "
                       "probationary queue size {}/{}, elements {}/{}, "
                       "protected queue size {}/{}, elements {}/{}",
                       getSize(lock), max_size, getElementsCount(lock), max_elements,
                       probationary_queue.getSize(lock), probationary_queue.max_size,
                       probationary_queue.getElementsCount(lock), probationary_queue.max_elements,
                       protected_queue.getSize(lock), protected_queue.max_size,
                       protected_queue.getElementsCount(lock), protected_queue.max_elements);
}

void SLRUFileCachePriority::check(const CachePriorityGuard::Lock & lock) const
{
    probationary_queue.check(lock);
    protected_queue.check(lock);
    IFileCachePriority::check(lock);
}

}
