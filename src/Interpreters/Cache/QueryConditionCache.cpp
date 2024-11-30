#include <Interpreters/Cache/QueryConditionCache.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace ProfileEvents
{
    extern const Event QueryConditionCacheHits;
    extern const Event QueryConditionCacheMisses;
};

namespace DB
{

size_t QueryConditionCache::KeyHasher::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.table_id);
    hash.update(key.part_name);
    hash.update(key.condition_id);
    return hash.get64();
}

std::optional<MarkFilter> QueryConditionCache::read(const UUID & table_id, const String & part_name, size_t condition_id)
{
    Key key{table_id, part_name, condition_id};

    if (auto entry = cache.get(key))
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheHits);

        std::lock_guard lock(entry->mutex);
        return std::make_optional(entry->mark_filter);
    }

    ProfileEvents::increment(ProfileEvents::QueryConditionCacheMisses);

    return std::nullopt;
}

void QueryConditionCache::write(const UUID & table_id, const String & part_name, size_t condition_id, const MarkRanges & mark_ranges, size_t marks_count)
{
    Key key{table_id, part_name, condition_id};

    auto [entry, _] = cache.getOrSet(key, [&]()
    {
        return std::make_shared<Entry>(marks_count);
    });

    chassert(marks_count == entry->mark_filter.size());

    /// Set MarkRanges to false, so there is no need to read these marks again later.
    {
        std::lock_guard lock(entry->mutex);
        for (const auto & mark_range : mark_ranges)
            std::fill(entry->mark_filter.begin() + mark_range.begin, entry->mark_filter.begin() + mark_range.end, false);
    }
}

}
