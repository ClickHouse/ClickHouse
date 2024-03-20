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
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    IFileCachePriority::IteratorPtr reservee,
    const UserID & user_id,
    bool & reached_size_limit,
    bool & reached_elements_limit,
    const CachePriorityGuard::Lock & lock)
{
    /// If `it` is nullptr, then it is the first space reservation attempt
    /// for a corresponding file segment, so it will be directly put into probationary queue.
    if (!reservee)
    {
        return probationary_queue.collectCandidatesForEviction(
            size, stat, res, reservee, user_id, reached_size_limit, reached_elements_limit, lock);
    }

    /// If `it` not nullptr (e.g. is already in some queue),
    /// we need to check in which queue (protected/probationary) it currently is
    /// (in order to know where we need to free space).
    if (!assert_cast<SLRUIterator *>(reservee.get())->is_protected)
    {
        return probationary_queue.collectCandidatesForEviction(
            size, stat, res, reservee, user_id, reached_size_limit, reached_elements_limit, lock);
    }

    /// Entry is in protected queue.
    /// Check if we have enough space in protected queue to fit a new size of entry.
    /// `size` is the increment to the current entry.size we want to increase.
    /// Here `elements` is 0, because entry is already in the protected queue.
    if (protected_queue.canFit(size, /* elements */0, lock))
    {
        return true;
    }

    /// If not enough space - we need to "downgrade" lowest priority entries from protected
    /// queue to probationary queue.
    /// The amount of such "downgraded" entries is equal to the amount
    /// required to make space for additionary `size` bytes for entry.
    auto downgrade_candidates = std::make_shared<EvictionCandidates>();

    if (!protected_queue.collectCandidatesForEviction(
            size, stat, *downgrade_candidates, reservee,
            user_id, reached_size_limit, reached_elements_limit, lock))
    {
        return false;
    }

    const size_t size_to_downgrade = stat.stat.releasable_size;
    const size_t elements_to_downgrade = stat.stat.releasable_count;

    chassert(size_to_downgrade);

    FileCacheReserveStat probationary_stat;
    bool downgrade_reached_size_limit = false;
    bool downgrade_reached_elements_limit = false;
    if (!probationary_queue.canFit(size_to_downgrade, elements_to_downgrade, lock)
        && !probationary_queue.collectCandidatesForEviction(
            size_to_downgrade, probationary_stat, res, reservee, user_id,
            downgrade_reached_size_limit, downgrade_reached_elements_limit, lock))
    {
        return false;
    }

    if (downgrade_candidates->size() == 0)
    {
        return true;
    }

    const bool downgrade_after_eviction = res.size() > 0;
    auto take_space_hold = [&]()
    {
        const size_t hold_size = downgrade_reached_size_limit
            ? size_to_downgrade > probationary_stat.stat.releasable_size ? size - probationary_stat.stat.releasable_size : 0
            : size_to_downgrade;

        const size_t hold_elements = downgrade_reached_elements_limit
            ? elements_to_downgrade > probationary_stat.stat.releasable_count ? size - probationary_stat.stat.releasable_count : 0
            : elements_to_downgrade;

        return std::make_shared<HoldSpace>(
            hold_size, hold_elements, QueueEntryType::SLRU_Probationary, probationary_queue, lock);
    };

    auto downgrade_func = [=, holder = downgrade_after_eviction ? take_space_hold() : nullptr, this]
        (const CachePriorityGuard::Lock & lk)
    {
        if (holder)
            holder->release();

        LOG_TEST(log, "Downgrading {} elements from protected to probationary. "
                 "Total size: {}",
                 downgrade_candidates->size(), stat.stat.releasable_size);

        for (const auto & [key, key_candidates] : *downgrade_candidates)
        {
            for (const auto & candidate : key_candidates.candidates)
                downgrade(candidate->getQueueIterator(), lk);
        }
    };

    if (downgrade_after_eviction)
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

    /// Check if there is enough space in protected queue to move entry there.
    /// If not - we need to "downgrade" lowest priority entries from protected
    /// queue to probationary queue.
    EvictionCandidates downgrade_candidates;
    FileCacheReserveStat downgrade_stat;

    bool reached_size_limit_noop;
    bool reached_elements_limit_noop;

    if (!protected_queue.collectCandidatesForEviction(
            entry_size, downgrade_stat, downgrade_candidates, {}, "",
            reached_size_limit_noop, reached_elements_limit_noop, lock))
    {
        /// We cannot make space for entry to be moved to protected queue
        /// (not enough releasable file segments).
        /// Then just increase its priority within probationary queue.
        iterator.lru_iterator.increasePriority(lock);
        return;
    }

    const size_t downgrade_size = downgrade_stat.stat.releasable_size;
    const size_t downgrade_count = downgrade_stat.stat.releasable_count;

    /// Then we need to free up space in probationary for downgrade candidates,
    /// but we take into account that we'll remove reservee from probationary
    /// at the same time, so recalculate size which we need to free.
    size_t size_to_free = 0;
    if (downgrade_size && downgrade_size > entry_size)
        size_to_free = downgrade_size - entry_size;

    LOG_TEST(log, "Will free up {} from probationary", size_to_free);

    /// Now we need to check if those "downgrade" candidates can actually
    /// be moved to probationary queue.
    EvictionCandidates eviction_candidates;
    FileCacheReserveStat stat;

    if (size_to_free)
    {
        if (!probationary_queue.collectCandidatesForEviction(
                size_to_free, stat, eviction_candidates, {}, {},
                reached_size_limit_noop, reached_elements_limit_noop, lock))
        {
            /// "downgrade" candidates cannot be moved to probationary queue,
            /// so entry cannot be moved to protected queue as well.
            /// Then just increase its priority within probationary queue.
            iterator.lru_iterator.increasePriority(lock);
            return;
        }
        /// Make space for "downgrade" candidates.
        eviction_candidates.evict();
        eviction_candidates.finalize(nullptr, lock);
    }

    /// All checks passed, now we can move downgrade candidates to
    /// probationary queue and our entry to protected queue.

    EntryPtr entry = iterator.getEntry();
    /// We need to remove the entry from probationary first
    /// in order to make space for downgrade from protected.
    iterator.lru_iterator.remove(lock);

    if (!probationary_queue.canFit(downgrade_size, downgrade_count, lock))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot downgrade {} elements by total size of {}: not enough space: {}",
                        downgrade_size, downgrade_count,
                        probationary_queue.getStateInfoForLog(lock));
    }

    for (const auto & [key, key_candidates] : downgrade_candidates)
    {
        LOG_TEST(log, "Downgrading {} elements from protected to probationary. "
                 "Total size: {}, current probationary state: {}",
                 downgrade_candidates.size(), downgrade_size,
                 probationary_queue.getStateInfoForLog(lock));

        for (const auto & candidate : key_candidates.candidates)
            downgrade(candidate->getQueueIterator(), lock);
    }

    iterator.lru_iterator = protected_queue.add(entry, lock);
    iterator.is_protected = true;
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

void SLRUFileCachePriority::holdImpl(
    size_t size,
    size_t elements,
    QueueEntryType queue_entry_type,
    const CachePriorityGuard::Lock & lock)
{
    switch (queue_entry_type)
    {
        case QueueEntryType::SLRU_Protected:
        {
            protected_queue.holdImpl(size, elements, queue_entry_type, lock);
            break;
        }
        case QueueEntryType::SLRU_Probationary:
        {
            probationary_queue.holdImpl(size, elements, queue_entry_type, lock);
            break;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Unexpected queue entry type: {}", queue_entry_type);
    }
}

void SLRUFileCachePriority::releaseImpl(size_t size, size_t elements, QueueEntryType queue_entry_type)
{
    switch (queue_entry_type)
    {
        case QueueEntryType::SLRU_Protected:
        {
            protected_queue.releaseImpl(size, elements, queue_entry_type);
            break;
        }
        case QueueEntryType::SLRU_Probationary:
        {
            probationary_queue.releaseImpl(size, elements, queue_entry_type);
            break;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Unexpected queue entry type: {}", queue_entry_type);
    }
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
