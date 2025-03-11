#include <Interpreters/Cache/QueryConditionCache.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include "Interpreters/Cache/FileSegmentInfo.h"

namespace ProfileEvents
{
    extern const Event QueryConditionCacheHits;
    extern const Event QueryConditionCacheMisses;
};

namespace DB
{

QueryConditionCache::QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : cache(cache_policy, max_size_in_bytes, 0, size_ratio)
{
}

std::optional<QueryConditionCache::MatchingMarks> QueryConditionCache::read(const UUID & table_id, const String & part_name, size_t condition_hash)
{
    Key key = {table_id, part_name, condition_hash};

    if (auto entry = cache.get(key))
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheHits);

        std::lock_guard lock(entry->mutex);
        return {entry->matching_marks};
    }

    ProfileEvents::increment(ProfileEvents::QueryConditionCacheMisses);

    return std::nullopt;
}

void QueryConditionCache::write(size_t condition_hash, const MarkRangesInfoPtr & mark_info)
{
    Key key = {mark_info->table_uuid, mark_info->part_name, condition_hash};

    auto load_func = [&](){ return std::make_shared<Entry>(mark_info->marks_count); };
    auto [entry, _] = cache.getOrSet(key, load_func);

    chassert(mark_info->marks_count == entry->matching_marks.size());

    /// Set MarkRanges to false, so there is no need to read these marks again later.
    {
        std::lock_guard lock(entry->mutex);
        for (const auto & mark_range : mark_info->mark_ranges)
            std::fill(entry->matching_marks.begin() + mark_range.begin, entry->matching_marks.begin() + mark_range.end, false);

        if (mark_info->has_final_mark)
            entry->matching_marks[mark_info->marks_count - 1] = false;

        LOG_DEBUG(
            logger,
            "table_id: {}, part_name: {}, condition_hash: {}, marks_count: {}, has_final_mark: {}, (ranges: {})",
            mark_info->table_uuid,
            mark_info->part_name,
            condition_hash,
            mark_info->marks_count,
            mark_info->has_final_mark,
            toString(mark_info->mark_ranges));
    }
}

void QueryConditionCache::clear()
{
    cache.clear();
}

void QueryConditionCache::setMaxSizeInBytes(size_t max_size_in_bytes)
{
    cache.setMaxSizeInBytes(max_size_in_bytes);
}

bool QueryConditionCache::Key::operator==(const Key & other) const
{
    return table_id == other.table_id && part_name == other.part_name && condition_hash == other.condition_hash;
}

QueryConditionCache::Entry::Entry(size_t mark_count)
    : matching_marks(mark_count, true) /// by default, all marks potentially are potential matches
{
}

size_t QueryConditionCache::KeyHasher::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.table_id);
    hash.update(key.part_name);
    hash.update(key.condition_hash);
    return hash.get64();
}

size_t QueryConditionCache::QueryConditionCacheEntryWeight::operator()(const Entry & entry) const
{
    /// Estimate the memory size of `std::vector<bool>`, for bool values, only 1 bit per element.
    size_t dynamic_memory = (entry.matching_marks.capacity() + 7) / 8; /// Round up to bytes.
    return sizeof(decltype(entry.matching_marks)) + dynamic_memory;
}
}
