#include <algorithm>
#include <cstdio>
#include <Interpreters/Cache/EvictionCandidates.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/SplitFileCachePriority.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace
{
size_t getRatio(size_t total, double ratio)
{
    return std::lround(total * std::clamp(ratio, 0.0, 1.0));
}
}

SplitFileCachePriority::SplitFileCachePriority(
    CachePriorityCreatorFunction creator_function,
    size_t max_size_,
    size_t max_elements_,
    double size_ratio,
    double system_segment_size_ratio_,
    const std::string & description_)
    : IFileCachePriority(max_size_, max_elements_)
    , system_segment_size_ratio(system_segment_size_ratio_)
    , max_data_segment_size(getRatio(max_size_, (1 - system_segment_size_ratio)))
    , max_data_segment_elements(getRatio(max_elements_, (1 - system_segment_size_ratio)))
    , max_system_segment_size(getRatio(max_size_, system_segment_size_ratio))
    , max_system_segment_elements(getRatio(max_elements_, system_segment_size_ratio))
    , log(getLogger("SplitFileCachePriority(" + description_ + ")"))
{
    priorities_holder.insert(
        {SegmentType::Data,
         creator_function(
             max_size_,
             max_elements_,
             size_ratio * (1 - system_segment_size_ratio),
             description_ + "_" + getKeyTypePrefix(SegmentType::Data))});
    priorities_holder.insert(
        {SegmentType::System,
         creator_function(
             max_size_,
             max_elements_,
             size_ratio * system_segment_size_ratio,
             description_ + "_" + getKeyTypePrefix(SegmentType::System))});
}

size_t SplitFileCachePriority::getSize(const CachePriorityGuard::Lock & lock) const
{
    return priorities_holder.at(SegmentType::Data)->getSize(lock) + priorities_holder.at(SegmentType::System)->getSize(lock);
}

size_t SplitFileCachePriority::getElementsCount(const CachePriorityGuard::Lock & lock) const
{
    return priorities_holder.at(SegmentType::Data)->getElementsCount(lock)
        + priorities_holder.at(SegmentType::System)->getElementsCount(lock);
}

size_t SplitFileCachePriority::getSizeApprox() const
{
    return priorities_holder.at(SegmentType::Data)->getSizeApprox() + priorities_holder.at(SegmentType::System)->getSizeApprox();
}

size_t SplitFileCachePriority::getElementsCountApprox() const
{
    return priorities_holder.at(SegmentType::Data)->getSizeApprox() + priorities_holder.at(SegmentType::System)->getSizeApprox();
}

std::string SplitFileCachePriority::getStateInfoForLog(const CachePriorityGuard::Lock & lock) const
{
    return "DataPriority: " + priorities_holder.at(SegmentType::Data)->getStateInfoForLog(lock)
        + " SystemPriority: " + priorities_holder.at(SegmentType::System)->getStateInfoForLog(lock);
}

void SplitFileCachePriority::shuffle(const CachePriorityGuard::Lock & lock)
{
    priorities_holder[SegmentType::Data]->shuffle(lock);
    priorities_holder[SegmentType::System]->shuffle(lock);
}

IFileCachePriority::PriorityDumpPtr SplitFileCachePriority::dump(const CachePriorityGuard::Lock & lock)
{
    auto data_dump = priorities_holder.at(SegmentType::Data)->dump(lock);
    auto system_dump = priorities_holder.at(SegmentType::System)->dump(lock);
    data_dump->merge(*system_dump);
    return data_dump;
}

bool SplitFileCachePriority::modifySizeLimits(
    size_t max_size_, size_t max_elements_, double size_ratio_, const CachePriorityGuard::Lock & lock)
{
    if (max_size == max_size_ && max_elements == max_elements_ && system_segment_size_ratio == size_ratio_)
        return false; /// Nothing to change.

    max_data_segment_elements = getRatio(max_elements_, (1 - system_segment_size_ratio));
    max_data_segment_size = getRatio(max_size_, (1 - system_segment_size_ratio));

    max_system_segment_elements = getRatio(max_elements_, system_segment_size_ratio);
    max_system_segment_size = getRatio(max_size_, system_segment_size_ratio);


    priorities_holder.at(SegmentType::Data)
        ->modifySizeLimits(max_size_, max_elements_, size_ratio_ * (1 - system_segment_size_ratio), lock);
    priorities_holder.at(SegmentType::System)->modifySizeLimits(max_size_, max_elements_, size_ratio_ * system_segment_size_ratio, lock);
    return true;
}


IFileCachePriority::IteratorPtr SplitFileCachePriority::
    add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const OriginInfo & origin,
        const CachePriorityGuard::Lock & lock,
        bool best_effort)
{
    if (origin.segment_type == SegmentType::Data || origin.segment_type == SegmentType::General)
        return priorities_holder.at(SegmentType::Data)->add(key_metadata, offset, size, origin, lock, best_effort);
    else
        return priorities_holder.at(SegmentType::System)->add(key_metadata, offset, size, origin, lock, best_effort);
}

bool SplitFileCachePriority::canFit( /// NOLINT
    size_t size,
    size_t elements,
    const CachePriorityGuard::Lock & lock,
    IteratorPtr,
    bool) const
{
    auto current_cache_size = getSize(lock);
    auto current_cache_elements = getElementsCount(lock);
    return (!max_size || (max_size.load() - current_cache_size >= size))
        && (!max_elements || (max_elements.load() - current_cache_elements >= elements));
}


bool SplitFileCachePriority::collectCandidatesForEviction(
    size_t size,
    size_t elements,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    IFileCachePriority::IteratorPtr reservee,
    bool continue_from_last_eviction_pos,
    const OriginInfo & origin,
    const CachePriorityGuard::Lock & lock)
{
    auto current_system_size = priorities_holder.at(FileSegmentKeyType::System)->getSize(lock);
    auto current_system_elements = priorities_holder.at(FileSegmentKeyType::System)->getElementsCount(lock);

    size_t to_evict_from_system_size
        = (current_system_size > max_system_segment_size) ? current_system_size - max_system_segment_size : 0ull;
    to_evict_from_system_size = std::min(to_evict_from_system_size, size);

    size_t to_evict_from_system_elements
        = (current_system_elements > max_data_segment_elements) ? max_data_segment_elements - current_system_elements : 0ull;
    to_evict_from_system_elements = std::min(to_evict_from_system_elements, elements);
    bool system_collection_status = true;
    if (to_evict_from_system_size || to_evict_from_system_elements)
    {
        FileCacheReserveStat system_stat;
        system_collection_status = priorities_holder.at(FileSegmentKeyType::System)
                                       ->collectCandidatesForEviction(
                                           to_evict_from_system_size + max_data_segment_size,
                                           to_evict_from_system_elements + max_data_segment_elements,
                                           system_stat,
                                           res,
                                           reservee,
                                           continue_from_last_eviction_pos,
                                           origin,
                                           lock);
        stat += system_stat;
        size -= to_evict_from_system_size;
        elements -= to_evict_from_system_elements;
        LOG_TEST(
            log,
            "Collected {} to evict from system priority"
            "with total size: {} (result: {}). "
            "Desired size: {}, desired elements count: {}, current state: {}",
            system_stat.total_stat.releasable_count,
            system_stat.total_stat.releasable_size,
            res.size(),
            size,
            elements,
            priorities_holder.at(FileSegmentKeyType::Data)->getStateInfoForLog(lock));
    }
    bool data_collection_status = true;
    if (size || elements)
    {
        FileCacheReserveStat data_stat;
        data_collection_status = priorities_holder.at(FileSegmentKeyType::Data)
                                     ->collectCandidatesForEviction(
                                         size + max_system_segment_size,
                                         elements + max_system_segment_elements,
                                         data_stat,
                                         res,
                                         reservee,
                                         continue_from_last_eviction_pos,
                                         origin,
                                         lock);
        stat += data_stat;
        LOG_TEST(
            log,
            "Collected {} to evict from data priority"
            "with total size: {} (result: {}). "
            "Desired size: {}, desired elements count: {}, current state: {}",
            data_stat.total_stat.releasable_count,
            data_stat.total_stat.releasable_size,
            res.size(),
            size,
            elements,
            priorities_holder.at(FileSegmentKeyType::Data)->getStateInfoForLog(lock));
    }

    return data_collection_status && system_collection_status;
}

IFileCachePriority::CollectStatus SplitFileCachePriority::collectCandidatesForEviction(
    size_t desired_size,
    size_t desired_elements_count,
    size_t max_candidates_to_evict,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    const CachePriorityGuard::Lock & lock)
{
    const auto new_system_size = getRatio(desired_size, system_segment_size_ratio);
    const auto new_system_elements = getRatio(desired_elements_count, system_segment_size_ratio);

    const auto curr_system_size = priorities_holder.at(FileSegmentKeyType::System)->getSize(lock);
    const auto curr_system_elements = priorities_holder.at(FileSegmentKeyType::System)->getElementsCount(lock);

    CollectStatus system_size_status = CollectStatus::SUCCESS;
    if (curr_system_size > new_system_size || curr_system_elements > new_system_elements)
    {
        FileCacheReserveStat system_stat;

        system_size_status
            = priorities_holder.at(FileSegmentKeyType::System)
                  ->collectCandidatesForEviction(new_system_size, new_system_elements, max_candidates_to_evict, system_stat, res, lock);

        stat += system_stat;
        LOG_TEST(
            log,
            "Collected {} to evict from system priority"
            "with total size: {} (result: {}). "
            "Desired size: {}, desired elements count: {}, current state: {}",
            system_stat.total_stat.releasable_count,
            system_stat.total_stat.releasable_size,
            res.size(),
            max_system_segment_size,
            max_system_segment_elements,
            priorities_holder.at(FileSegmentKeyType::System)->getStateInfoForLog(lock));

        if (system_size_status == CollectStatus::REACHED_MAX_CANDIDATES_LIMIT)
            return system_size_status;
    }

    FileCacheReserveStat data_stat;
    auto new_data_size = getRatio(desired_size, (1 - system_segment_size_ratio));
    auto new_data_elements = getRatio(desired_elements_count, (1 - system_segment_size_ratio));

    const auto data_size_status
        = priorities_holder.at(FileSegmentKeyType::Data)
              ->collectCandidatesForEviction(new_data_size, new_data_elements, max_candidates_to_evict - res.size(), data_stat, res, lock);

    stat += data_stat;

    LOG_TEST(
        log,
        "Collected {} to evict from data queue "
        "with total size: {} (result: {}). "
        "Desired size: {}, desired elements count: {}, current state: {}",
        data_stat.total_stat.releasable_count,
        data_stat.total_stat.releasable_size,
        res.size(),
        new_data_size,
        new_data_elements,
        priorities_holder.at(FileSegmentKeyType::Data)->getStateInfoForLog(lock));

    if (data_size_status == CollectStatus::SUCCESS)
        return system_size_status;

    return data_size_status;
}

void SplitFileCachePriority::resetEvictionPos(const CachePriorityGuard::Lock & lock)
{
    priorities_holder[SegmentType::Data]->resetEvictionPos(lock);
    priorities_holder[SegmentType::System]->resetEvictionPos(lock);
}

SplitFileCachePriority::SplitIterator::SplitIterator(
    IFileCachePriority * inner_cache_priority, IteratorPtr iterator_, FileSegmentKeyType type_)
    : type(type_)
    , cache_priority(inner_cache_priority)
    , iterator(iterator_)
    , entry(iterator->getEntry())
{
}

IFileCachePriority::EntryPtr SplitFileCachePriority::SplitIterator::getEntry() const
{
    return iterator->getEntry();
}

size_t SplitFileCachePriority::SplitIterator::increasePriority(const CachePriorityGuard::Lock & lock)
{
    return iterator->increasePriority(lock);
}

void SplitFileCachePriority::SplitIterator::remove(const CachePriorityGuard::Lock & lock)
{
    iterator->remove(lock);
}


void SplitFileCachePriority::SplitIterator::invalidate()
{
    iterator->invalidate();
}

void SplitFileCachePriority::SplitIterator::incrementSize(size_t size, const CachePriorityGuard::Lock & lock)
{
    iterator->incrementSize(size, lock);
}

void SplitFileCachePriority::SplitIterator::decrementSize(size_t size)
{
    iterator->decrementSize(size);
}


}
