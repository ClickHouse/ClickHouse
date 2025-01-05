#include <Interpreters/Cache/QueryConditionCache.h>
#include <Storages/MergeTree/MergeTreeData.h>

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

void QueryConditionCache::write(const UUID & table_id, const String & part_name, size_t condition_hash, const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark)
{
    Key key = {table_id, part_name, condition_hash};

    auto load_func = [&](){ return std::make_shared<Entry>(marks_count); };
    auto [entry, _] = cache.getOrSet(key, load_func);

    chassert(marks_count == entry->matching_marks.size());

    /// Set MarkRanges to false, so there is no need to read these marks again later.
    {
        std::lock_guard lock(entry->mutex);
        for (const auto & mark_range : mark_ranges)
            std::fill(entry->matching_marks.begin() + mark_range.begin, entry->matching_marks.begin() + mark_range.end, false);

        if (has_final_mark)
            entry->matching_marks[marks_count - 1] = false;
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

}
