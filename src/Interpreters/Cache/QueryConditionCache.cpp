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
    hash.update(key.condition);
    return hash.get64();
}

std::optional<MarkFilter> QueryConditionCache::read(const MergeTreeDataPartPtr & data_part, const String & condition)
{
    if (!data_part)
        return std::nullopt;

    auto table = data_part->storage.getStorageID();
    Key key{table.uuid, data_part->name, condition};

    if (auto entry = cache.get(key))
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheHits);

        std::lock_guard lock(entry->mutex);
        return std::make_optional(entry->mark_filter);
    }

    ProfileEvents::increment(ProfileEvents::QueryConditionCacheMisses);

    return std::nullopt;
}

void QueryConditionCache::write(const MergeTreeDataPartPtr & data_part, const String & condition, const MarkRanges & mark_ranges)
{
    if (!data_part || mark_ranges.empty())
        return;

    auto table = data_part->storage.getStorageID();
    Key key{table.uuid, data_part->name, condition};

    size_t count = data_part->index_granularity.getMarksCount();
    auto [entry, _] = cache.getOrSet(key, [&]()
    {
        return std::make_shared<Entry>(count);
    });

    chassert(count == entry->mark_filter.size());

    /// Set MarkRanges to false, so there is no need to read these marks again later.
    {
        std::lock_guard lock(entry->mutex);
        for (const auto & mark_range : mark_ranges)
            std::fill(entry->mark_filter.begin() + mark_range.begin, entry->mark_filter.begin() + mark_range.end, false);
    }
}

}
