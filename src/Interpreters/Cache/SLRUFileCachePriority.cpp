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
        return static_cast<size_t>(total * std::clamp(ratio, 0.0, 1.0));
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
    , protected_queue(LRUFileCachePriority(getRatio(max_size_, size_ratio), getRatio(max_elements_, size_ratio), protected_state_))
    , probationary_queue(LRUFileCachePriority(getRatio(max_size_, 1 - size_ratio), getRatio(max_elements_, 1 - size_ratio), probationary_state_))
{
    LOG_DEBUG(
        log, "Using probationary queue size: {}, protected queue size: {}",
        probationary_queue.max_size, protected_queue.max_elements);
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
    if (is_startup)
    {
        /// If it is server startup, we put entries in any queue it will fit in,
        /// but with preference for probationary queue,
        /// because we do not know the distribution between queues after server restart.
        if (probationary_queue.canFit(size, 1, lock))
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
    if (protected_queue.canFit(size, 1, lock))
        return true;

    /// If not enough space - we need to "downgrade" lowest priority entries from protected
    /// queue to probationary queue.
    /// The amount of such "downgraded" entries is equal to the amount
    /// required to make space for additionary `size` bytes for entry.
    auto downgrade_candidates = std::make_shared<EvictionCandidates>();
    FileCacheReserveStat downgrade_stat;

    if (!protected_queue.collectCandidatesForEviction(
            size, downgrade_stat, *downgrade_candidates, reservee, user_id, reached_size_limit, reached_elements_limit, lock))
        return false;

    const size_t size_to_downgrade = downgrade_stat.stat.releasable_size;

    bool reached_size_limit_noop;
    bool reached_elements_limit_noop;
    if (!probationary_queue.canFit(size_to_downgrade, 1, lock)
        && !probationary_queue.collectCandidatesForEviction(
            size_to_downgrade, stat, res, reservee, user_id, reached_size_limit_noop, reached_elements_limit_noop, lock))
        return false;

    res.setFinalizeEvictionFunc([=, this](const CachePriorityGuard::Lock & lk) mutable
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
    });

    return true;
}

EvictionCandidates SLRUFileCachePriority::collectCandidatesForEviction(
    size_t desired_size,
    size_t desired_elements_count,
    size_t max_candidates_to_evict,
    FileCacheReserveStat & stat,
    const CachePriorityGuard::Lock & lock)
{
    if (!max_candidates_to_evict)
        return {};

    const auto desired_probationary_size = getRatio(desired_size, 1 - size_ratio);
    const auto desired_probationary_elements_num = getRatio(desired_elements_count, 1 - size_ratio);

    auto res = probationary_queue.collectCandidatesForEviction(
        desired_probationary_size, desired_probationary_elements_num, max_candidates_to_evict, stat, lock);

    chassert(res.size() <= max_candidates_to_evict);
    chassert(res.size() == stat.stat.releasable_count);

    if (res.size() == max_candidates_to_evict)
        return res;

    const auto desired_protected_size = getRatio(max_size, size_ratio);
    const auto desired_protected_elements_num = getRatio(max_elements, size_ratio);

    auto res_add = protected_queue.collectCandidatesForEviction(
        desired_protected_size, desired_protected_elements_num, max_candidates_to_evict - res.size(), stat, lock);
    res.add(res_add, lock);
    return res;
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

    bool reached_size_limit_noop;
    bool reached_elements_limit_noop;

    if (!protected_queue.collectCandidatesForEviction(
            size, downgrade_stat, downgrade_candidates, {}, "", reached_size_limit_noop, reached_elements_limit_noop, lock))
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
        if (!probationary_queue.collectCandidatesForEviction(
                size_to_free, stat, eviction_candidates, {}, {}, reached_size_limit_noop, reached_elements_limit_noop, lock))
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

void SLRUFileCachePriority::modifySizeLimits(
    size_t max_size_,
    size_t max_elements_,
    double size_ratio_,
    const CachePriorityGuard::Lock & lock)
{
    if (max_size == max_size_ && max_elements == max_elements_ && size_ratio == size_ratio_)
        return; /// Nothing to change.

    protected_queue.modifySizeLimits(getRatio(max_size_, size_ratio_), getRatio(max_elements_, size_ratio_), 0, lock);
    probationary_queue.modifySizeLimits(getRatio(max_size_, 1 - size_ratio_), getRatio(max_elements_, 1 - size_ratio_), 0, lock);

    max_size = max_size_;
    max_elements = max_elements_;
    size_ratio = size_ratio_;
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

void SLRUFileCachePriority::holdImpl(size_t size, size_t elements, IteratorPtr reservee, const CachePriorityGuard::Lock & lock)
{
    if (!canFit(size, elements, lock, reservee))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot hold space {}", size);

    if (reservee)
    {
        const auto * slru_iterator = assert_cast<SLRUIterator *>(reservee.get());
        if (slru_iterator->is_protected)
            return protected_queue.holdImpl(size, elements, reservee, lock);
        else
            return probationary_queue.holdImpl(size, elements, reservee, lock);
    }
    else
        return probationary_queue.holdImpl(size, elements, reservee, lock);
}

void SLRUFileCachePriority::releaseImpl(size_t size, size_t elements, IteratorPtr reservee)
{
    if (reservee)
    {
        const auto * slru_iterator = assert_cast<SLRUIterator *>(reservee.get());
        if (slru_iterator->is_protected)
            return protected_queue.releaseImpl(size, elements, reservee);
        else
            return probationary_queue.releaseImpl(size, elements, reservee);
    }
    else
        return probationary_queue.releaseImpl(size, elements, reservee);
}
}
