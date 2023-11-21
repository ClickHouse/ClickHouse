#include <Interpreters/Cache/SLRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>
#include <pcg-random/pcg_random.hpp>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSize;
    extern const Metric FilesystemCacheElements;
}

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
        return static_cast<size_t>(total * std::max(0.0, std::min(1.0, ratio)));
    }
}

SLRUFileCachePriority::SLRUFileCachePriority(
    size_t max_size_,
    size_t max_elements_,
    double size_ratio)
    : IFileCachePriority(max_size_, max_elements_)
    , protected_queue(LRUFileCachePriority(getRatio(max_size_, size_ratio), getRatio(max_elements_, size_ratio)))
    , probationary_queue(LRUFileCachePriority(getRatio(max_size_, 1 - size_ratio), getRatio(max_elements_, 1 - size_ratio)))
{
    LOG_DEBUG(
        log, "Using probationary queue size: {}, protected queue size: {}",
        probationary_queue.getSizeLimit(), protected_queue.getSizeLimit());
}

size_t SLRUFileCachePriority::getSize(const CacheGuard::Lock & lock) const
{
    return protected_queue.getSize(lock) + probationary_queue.getSize(lock);
}

size_t SLRUFileCachePriority::getElementsCount(const CacheGuard::Lock & lock) const
{
    return protected_queue.getElementsCount(lock) + probationary_queue.getElementsCount(lock);
}

IFileCachePriority::Iterator SLRUFileCachePriority::add(
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const CacheGuard::Lock & lock)
{
    return probationary_queue.add(key_metadata, offset, size, lock);
}

SLRUFileCachePriority::SLRUQueueIterator
SLRUFileCachePriority::remove(SLRUQueueIterator it, bool is_protected, const CacheGuard::Lock & lock)
{
    if (is_protected)
        return protected_queue.remove(it, lock);
    else
        return probationary_queue.remove(it, lock);
}

void SLRUFileCachePriority::updateSize(int64_t size, bool is_protected)
{
    if (is_protected)
        protected_queue.updateSize(size);
    else
        probationary_queue.updateSize(size);
}

void SLRUFileCachePriority::updateElementsCount(int64_t num, bool is_protected)
{
    if (is_protected)
        protected_queue.updateElementsCount(num);
    else
        probationary_queue.updateElementsCount(num);
}

bool SLRUFileCachePriority::collectCandidatesForEviction(
    size_t size,
    FileCacheReserveStat & stat,
    IFileCachePriority::EvictionCandidates & res,
    IFileCachePriority::Iterator it,
    FinalizeEvictionFunc & finalize_eviction_func,
    const CacheGuard::Lock & lock)
{
    bool is_protected = false;
    if (it)
        is_protected = assert_cast<SLRUFileCacheIterator *>(it.get())->is_protected;

    if (!is_protected)
    {
        return probationary_queue.collectCandidatesForEviction(size, stat, res, it, finalize_eviction_func, lock);
    }

    auto downgrade_candidates = std::make_shared<IFileCachePriority::EvictionCandidates>();
    FileCacheReserveStat downgrade_stat;
    FinalizeEvictionFunc noop;

    if (!protected_queue.collectCandidatesForEviction(size, downgrade_stat, *downgrade_candidates, it, noop, lock))
        return false;

    if (!probationary_queue.collectCandidatesForEviction(downgrade_stat.stat.releasable_size, stat, res, it, noop, lock))
        return false;

    finalize_eviction_func = [=, lk = &lock, this]() mutable
    {
        for (const auto & [key, key_candidates] : *downgrade_candidates)
        {
            for (const auto & candidate : key_candidates.candidates)
            {
                auto * candidate_it = assert_cast<SLRUFileCacheIterator *>(candidate->getQueueIterator().get());
                probationary_queue.move(candidate_it->queue_iter, protected_queue, *lk);
            }
        }
    };

    return true;
}

SLRUFileCachePriority::SLRUQueueIterator
SLRUFileCachePriority::increasePriority(SLRUQueueIterator & it, bool is_protected, const CacheGuard::Lock & lock)
{
    if (is_protected)
    {
        protected_queue.increasePriority(it, lock);
        return it;
    }

    if (it->size > protected_queue.getSizeLimit())
    {
        /// This is only possible if protected_queue_size_limit is less than max_file_segment_size,
        /// which is not possible in any realistic cache configuration.
        return {};
    }

    IFileCachePriority::EvictionCandidates downgrade_candidates;
    FileCacheReserveStat downgrade_stat;
    FinalizeEvictionFunc noop;

    if (!protected_queue.collectCandidatesForEviction(it->size, downgrade_stat, downgrade_candidates, {}, noop, lock))
    {
        probationary_queue.increasePriority(it, lock);
        return it;
    }

    IFileCachePriority::EvictionCandidates eviction_candidates;
    FileCacheReserveStat stat;

    if (it->size < downgrade_stat.stat.releasable_size
        && !probationary_queue.collectCandidatesForEviction(
            downgrade_stat.stat.releasable_size - it->size, stat, eviction_candidates, {}, noop, lock))
    {
        probationary_queue.increasePriority(it, lock);
        return it;
    }

    eviction_candidates.evict(lock);

    for (const auto & [key, key_candidates] : downgrade_candidates)
    {
        for (const auto & candidate : key_candidates.candidates)
        {
            auto * candidate_it = assert_cast<SLRUFileCacheIterator *>(candidate->getQueueIterator().get());
            probationary_queue.move(candidate_it->queue_iter, protected_queue, lock);
        }
    }

    return protected_queue.move(it, probationary_queue, lock);
}

FileSegments SLRUFileCachePriority::dump(const CacheGuard::Lock & lock)
{
    auto res = probationary_queue.dump(lock);
    auto part_res = protected_queue.dump(lock);
    res.insert(res.end(), part_res.begin(), part_res.end());
    return res;
}

SLRUFileCachePriority::SLRUFileCacheIterator::SLRUFileCacheIterator(
    SLRUFileCachePriority * cache_priority_,
    SLRUFileCachePriority::SLRUQueueIterator queue_iter_,
    bool is_protected_)
    : cache_priority(cache_priority_)
    , queue_iter(queue_iter_)
    , is_protected(is_protected_)
{
}

void SLRUFileCachePriority::SLRUFileCacheIterator::remove(const CacheGuard::Lock & lock)
{
    checkUsable();
    cache_priority->remove(queue_iter, is_protected, lock);
    queue_iter = SLRUQueueIterator{};
}

void SLRUFileCachePriority::SLRUFileCacheIterator::invalidate()
{
    checkUsable();

    LOG_TEST(
        cache_priority->log,
        "Invalidating entry in SLRU queue. Key: {}, offset: {}, previous size: {}",
        queue_iter->key, queue_iter->offset, queue_iter->size);

    cache_priority->updateSize(-queue_iter->size, is_protected);
    cache_priority->updateElementsCount(-1, is_protected);
    queue_iter->size = 0;
}

void SLRUFileCachePriority::SLRUFileCacheIterator::updateSize(int64_t size)
{
    checkUsable();

    LOG_TEST(
        cache_priority->log,
        "Update size with {} in SLRU queue for key: {}, offset: {}, previous size: {}",
        size, queue_iter->key, queue_iter->offset, queue_iter->size);

    cache_priority->updateSize(size, is_protected);
    queue_iter->size += size;
}

size_t SLRUFileCachePriority::SLRUFileCacheIterator::increasePriority(const CacheGuard::Lock & lock)
{
    checkUsable();
    queue_iter = cache_priority->increasePriority(queue_iter, is_protected, lock);
    return ++queue_iter->hits;
}

void SLRUFileCachePriority::SLRUFileCacheIterator::checkUsable() const
{
    if (queue_iter == SLRUQueueIterator{})
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to use invalid iterator");
}

void SLRUFileCachePriority::shuffle(const CacheGuard::Lock & lock)
{
    protected_queue.shuffle(lock);
    probationary_queue.shuffle(lock);
}

}
