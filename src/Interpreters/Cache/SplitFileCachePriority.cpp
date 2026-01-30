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
    return std::lround(static_cast<double>(total) * std::clamp(ratio, 0.0, 1.0));
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
             max_data_segment_size,
             max_data_segment_elements,
             size_ratio,
             0, // Overcommit available only for CH Cloud
             description_ + "_" + getKeyTypePrefix(SegmentType::Data))});
    priorities_holder.insert(
        {SegmentType::System,
         creator_function(
             max_system_segment_size,
             max_system_segment_elements,
             size_ratio,
             0, // Overcommit available only for CH Cloud
             description_ + "_" + getKeyTypePrefix(SegmentType::System))});
}

SplitFileCachePriority::SegmentType
SplitFileCachePriority::getPriorityType(const SegmentType & segment_type) const
{
    return segment_type == SegmentType::Data || segment_type == SegmentType::General
        ? SegmentType::Data
        : SegmentType::System;
}

size_t SplitFileCachePriority::getSize(const CacheStateGuard::Lock & lock) const
{
    return priorities_holder.at(SegmentType::Data)->getSize(lock)
        + priorities_holder.at(SegmentType::System)->getSize(lock);
}

size_t SplitFileCachePriority::getElementsCount(const CacheStateGuard::Lock & lock) const
{
    return priorities_holder.at(SegmentType::Data)->getElementsCount(lock)
        + priorities_holder.at(SegmentType::System)->getElementsCount(lock);
}

size_t SplitFileCachePriority::getSizeApprox() const
{
    return priorities_holder.at(SegmentType::Data)->getSizeApprox()
        + priorities_holder.at(SegmentType::System)->getSizeApprox();
}

size_t SplitFileCachePriority::getElementsCountApprox() const
{
    return priorities_holder.at(SegmentType::Data)->getSizeApprox()
        + priorities_holder.at(SegmentType::System)->getSizeApprox();
}

std::string SplitFileCachePriority::getStateInfoForLog(const CacheStateGuard::Lock & lock) const
{
    return "DataPriority: " + priorities_holder.at(SegmentType::Data)->getStateInfoForLog(lock)
        + " SystemPriority: " + priorities_holder.at(SegmentType::System)->getStateInfoForLog(lock);
}

void SplitFileCachePriority::shuffle(const CachePriorityGuard::WriteLock & lock)
{
    priorities_holder[SegmentType::Data]->shuffle(lock);
    priorities_holder[SegmentType::System]->shuffle(lock);
}

IFileCachePriority::PriorityDumpPtr SplitFileCachePriority::dump(
    const CachePriorityGuard::ReadLock & lock)
{
    auto data_dump = priorities_holder.at(SegmentType::Data)->dump(lock);
    auto system_dump = priorities_holder.at(SegmentType::System)->dump(lock);
    data_dump->merge(*system_dump);
    return data_dump;
}

void SplitFileCachePriority::iterate(
    IterateFunc func,
    FileCacheReserveStat & stat,
    const CachePriorityGuard::ReadLock & lock)
{
    priorities_holder[SegmentType::Data]->iterate(func, stat, lock);
    priorities_holder[SegmentType::System]->iterate(func, stat, lock);
}

bool SplitFileCachePriority::modifySizeLimits(
    size_t max_size_,
    size_t max_elements_,
    double size_ratio_,
    const CacheStateGuard::Lock & lock)
{
    if (max_size == max_size_
        && max_elements == max_elements_
        && system_segment_size_ratio == size_ratio_)
        return false; /// Nothing to change.

    max_data_segment_elements = getRatio(max_elements_, (1 - system_segment_size_ratio));
    max_data_segment_size = getRatio(max_size_, (1 - system_segment_size_ratio));

    max_system_segment_elements = getRatio(max_elements_, system_segment_size_ratio);
    max_system_segment_size = getRatio(max_size_, system_segment_size_ratio);

    priorities_holder.at(SegmentType::Data)->modifySizeLimits(
        max_data_segment_size, max_data_segment_elements, size_ratio_, lock);

    priorities_holder.at(SegmentType::System)->modifySizeLimits(
        max_system_segment_size, max_system_segment_elements, size_ratio_, lock);

    return true;
}


IFileCachePriority::IteratorPtr SplitFileCachePriority::add( /// NOLINT
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const CachePriorityGuard::WriteLock & write_lock,
    const CacheStateGuard::Lock * state_lock,
    bool best_effort)
{
    const auto type = getPriorityType(key_metadata->origin.segment_type);
    return priorities_holder.at(type)->add(
        key_metadata, offset, size, write_lock, state_lock, best_effort);
}

bool SplitFileCachePriority::canFit( /// NOLINT
    size_t size,
    size_t elements,
    const CacheStateGuard::Lock & lock,
    IteratorPtr reservee,
    const OriginInfo & origin_info,
    bool best_effort) const
{
    const auto type = getPriorityType(origin_info.segment_type);
    return priorities_holder.at(type)->canFit(
        size, elements, lock, reservee, origin_info, best_effort);
}

EvictionInfoPtr SplitFileCachePriority::collectEvictionInfo(
    size_t size,
    size_t elements,
    IFileCachePriority::Iterator * reservee,
    bool is_total_space_cleanup,
    bool is_dynamic_resize,
    const IFileCachePriority::OriginInfo & origin_info,
    const CacheStateGuard::Lock & lock)
{
    const auto type = getPriorityType(origin_info.segment_type);
    return priorities_holder.at(type)->collectEvictionInfo(
        size, elements, reservee, is_total_space_cleanup, is_dynamic_resize,
        origin_info, lock);
}

bool SplitFileCachePriority::collectCandidatesForEviction(
    const EvictionInfo & eviction_info,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    InvalidatedEntriesInfos & invalidated_entries,
    IFileCachePriority::IteratorPtr reservee,
    bool continue_from_last_eviction_pos,
    size_t max_candidates_size,
    bool is_total_space_cleanup,
    const OriginInfo & origin_info,
    CachePriorityGuard & priority_guard,
    CacheStateGuard & state_guard)
{
    const auto type = getPriorityType(origin_info.segment_type);
    return priorities_holder.at(type)->collectCandidatesForEviction(
        eviction_info,
        stat,
        res,
        invalidated_entries,
        reservee,
        continue_from_last_eviction_pos,
        max_candidates_size,
        is_total_space_cleanup,
        origin_info,
        priority_guard,
        state_guard);
}

bool SplitFileCachePriority::tryIncreasePriority(
    Iterator & iterator,
    bool is_space_reservation_complete,
    CachePriorityGuard & queue_guard,
    CacheStateGuard & state_guard)
{
    const auto type = getPriorityType(iterator.getEntry()->key_metadata->origin.segment_type);
    return priorities_holder.at(type)->tryIncreasePriority(
        iterator, is_space_reservation_complete, queue_guard, state_guard);
}

void SplitFileCachePriority::resetEvictionPos()
{
    priorities_holder[SegmentType::Data]->resetEvictionPos();
    priorities_holder[SegmentType::System]->resetEvictionPos();
}

size_t SplitFileCachePriority::getHoldSize()
{
    return priorities_holder[SegmentType::Data]->getHoldSize()
        + priorities_holder[SegmentType::System]->getHoldSize();
}

size_t SplitFileCachePriority::getHoldElements()
{
    return priorities_holder[SegmentType::Data]->getHoldElements()
        + priorities_holder[SegmentType::System]->getHoldElements();
}

SplitFileCachePriority::SplitIterator::SplitIterator(
    IFileCachePriority * inner_cache_priority,
    IteratorPtr iterator_,
    FileSegmentKeyType type_)
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

void SplitFileCachePriority::SplitIterator::remove(const CachePriorityGuard::WriteLock & lock)
{
    iterator->remove(lock);
}

void SplitFileCachePriority::SplitIterator::invalidate()
{
    iterator->invalidate();
}

void SplitFileCachePriority::SplitIterator::incrementSize(
    size_t size,
    const CacheStateGuard::Lock & lock)
{
    iterator->incrementSize(size, lock);
}

void SplitFileCachePriority::SplitIterator::decrementSize(size_t size)
{
    iterator->decrementSize(size);
}


}
