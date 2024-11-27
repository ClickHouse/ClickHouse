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
    LRUFileCachePriority::StatePtr protected_state_,
    const std::string & description_)
    : IFileCachePriority(max_size_, max_elements_)
    , size_ratio(size_ratio_)
    , protected_queue(LRUFileCachePriority(getRatio(max_size_, size_ratio),
                                           getRatio(max_elements_, size_ratio),
                                           protected_state_,
                                           description_ + ", protected"))
    , probationary_queue(LRUFileCachePriority(getRatio(max_size_, 1 - size_ratio),
                                              getRatio(max_elements_, 1 - size_ratio),
                                              probationary_state_,
                                              description_ + ", probationary"))
    , log(getLogger("SLRUFileCachePriority(" + description_ + ")"))
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
        return probationary_queue.canFit(size, elements, lock);
    }
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
        if (probationary_queue.canFit(size, /* elements */1, lock))
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
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Violated cache limits. "
            "Added {} for {} element ({}:{}). Current state: {}",
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
    /// If `reservee` is nullptr, then it is the first space reservation attempt
    /// for a corresponding file segment, so it will be directly put into probationary queue.
    if (!reservee)
    {
        return probationary_queue.collectCandidatesForEviction(size, elements, stat, res, reservee, user_id, lock);
    }

    auto * slru_iterator = assert_cast<SLRUIterator *>(reservee.get());
    bool success = false;

    /// If `reservee` is not nullptr (e.g. is already in some queue),
    /// we need to check in which queue (protected/probationary) it currently is
    /// (in order to know where we need to free space).
    if (!slru_iterator->is_protected)
    {
        chassert(slru_iterator->lru_iterator.cache_priority == &probationary_queue);
        success = probationary_queue.collectCandidatesForEviction(size, elements, stat, res, reservee, user_id, lock);
    }
    else
    {
        chassert(slru_iterator->lru_iterator.cache_priority == &protected_queue);
        /// Entry is in protected queue.
        /// Check if we have enough space in protected queue to fit a new size of entry.
        /// `size` is the increment to the current entry.size we want to increase.
        success = collectCandidatesForEvictionInProtected(size, elements, stat, res, reservee, user_id, lock);
    }

    /// We eviction_candidates (res) set is non-empty and
    /// space reservation was successful, we will do eviction from filesystem
    /// which is executed without a cache priority lock.
    /// So we made space reservation from a certain queue (protected or probationary),
    /// but we should remember that there is an increasePriority operation
    /// which can be called concurrently to space reservation.
    /// This operation can move elements from one queue to another.
    /// We need to make sure that this does not happen for the elements
    /// which are in the process of unfinished space reservation.
    if (success && res.size() > 0)
    {
        slru_iterator->movable = false;
        res.onFinalize([=](const CachePriorityGuard::Lock &){ slru_iterator->movable = true; });
    }
    return success;
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

    /// We can have no downgrade candidates because cache size could
    /// reduce concurrently because of lock-free cache entries invalidation.
    if (downgrade_candidates->size() == 0)
    {
        return true;
    }

    if (!probationary_queue.collectCandidatesForEviction(
            downgrade_stat.total_stat.releasable_size, downgrade_stat.total_stat.releasable_count,
            stat, res, reservee, user_id, lock))
    {
        return false;
    }

    auto downgrade_func = [=, this](const CachePriorityGuard::Lock & lk)
    {
        for (const auto & [key, key_candidates] : *downgrade_candidates)
        {
            for (const auto & candidate : key_candidates.candidates)
                downgrade(candidate->getQueueIterator(), lk);
        }
    };

    if (res.size() > 0)
    {
        LOG_TEST(log, "Setting up delayed downgrade for {} elements "
                 "from protected to probationary. Total size: {}",
                 downgrade_candidates->size(), downgrade_stat.total_stat.releasable_size);

        /// Downgrade from protected to probationary only after
        /// we free up space in probationary (in order to fit these downgrade candidates).
        res.onFinalize(std::move(downgrade_func));
    }
    else
    {
        LOG_TEST(log, "Downgrading {} elements from protected to probationary. "
                 "Total size: {}",
                 downgrade_candidates->size(), downgrade_stat.total_stat.releasable_size);

        /// Enough space in probationary queue already to fit our downgrade candidates.
        downgrade_func(lock);
    }

    return true;
}

IFileCachePriority::CollectStatus SLRUFileCachePriority::collectCandidatesForEviction(
    size_t desired_size,
    size_t desired_elements_count,
    size_t max_candidates_to_evict,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    const CachePriorityGuard::Lock & lock)
{
    const auto desired_probationary_size = getRatio(desired_size, 1 - size_ratio);
    const auto desired_probationary_elements_num = getRatio(desired_elements_count, 1 - size_ratio);

    FileCacheReserveStat probationary_stat;
    const auto probationary_desired_size_status = probationary_queue.collectCandidatesForEviction(
        desired_probationary_size, desired_probationary_elements_num,
        max_candidates_to_evict, probationary_stat, res, lock);

    stat += probationary_stat;

    LOG_TEST(log, "Collected {} to evict from probationary queue "
             "with total size: {} (result: {}). "
             "Desired size: {}, desired elements count: {}, current state: {}",
             probationary_stat.total_stat.releasable_count,
             probationary_stat.total_stat.releasable_size, res.size(),
             desired_probationary_size, desired_probationary_elements_num,
             probationary_queue.getStateInfoForLog(lock));

    chassert(!max_candidates_to_evict || res.size() <= max_candidates_to_evict);
    chassert(res.size() == stat.total_stat.releasable_count);

    if (probationary_desired_size_status == CollectStatus::REACHED_MAX_CANDIDATES_LIMIT)
        return probationary_desired_size_status;

    const auto desired_protected_size = getRatio(desired_size, size_ratio);
    const auto desired_protected_elements_num = getRatio(desired_elements_count, size_ratio);

    FileCacheReserveStat protected_stat;
    const auto protected_desired_size_status = protected_queue.collectCandidatesForEviction(
        desired_protected_size, desired_protected_elements_num,
        max_candidates_to_evict - res.size(), protected_stat, res, lock);

    stat += protected_stat;

    LOG_TEST(log, "Collected {} to evict from protected queue "
             "with total size: {} (result: {}). "
             "Desired size: {}, desired elements count: {}, current state: {}",
             protected_stat.total_stat.releasable_count,
             protected_stat.total_stat.releasable_size, res.size(),
             desired_protected_size, desired_protected_elements_num,
             protected_queue.getStateInfoForLog(lock));

    if (probationary_desired_size_status == CollectStatus::SUCCESS)
        return protected_desired_size_status;
    return probationary_desired_size_status;
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
    if (!candidate_it->movable)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot downgrade {}: it is non-movable",
                        candidate_it->getEntry()->toString());
    }

    const size_t entry_size = candidate_it->getEntry()->size;
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

    if (!iterator.movable)
    {
        iterator.lru_iterator.increasePriority(lock);
        return;
    }

    chassert(iterator.lru_iterator.cache_priority == &probationary_queue);

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
                entry->size, /* elements */1, stat, eviction_candidates, nullptr, FileCache::getInternalUser().user_id, lock))
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
    auto entry_ptr = entry.lock();
    if (!entry_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Entry pointer expired");
    return entry_ptr;
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
