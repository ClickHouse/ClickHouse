#include <Interpreters/Cache/QueryConditionCache.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>

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

void QueryConditionCache::write(
    const UUID & table_id, const String & part_name, size_t condition_hash,
    const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark)
{
    Key key = {table_id, part_name, condition_hash};

    auto load_func = [&](){ return std::make_shared<Entry>(marks_count); };
    auto [entry, inserted] = cache.getOrSet(key, load_func);

    std::lock_guard lock(entry->mutex);

    chassert(marks_count == entry->matching_marks.size());

    /// The input mark ranges are the areas which the scan can skip later on.
    for (const auto & mark_range : mark_ranges)
        std::fill(entry->matching_marks.begin() + mark_range.begin, entry->matching_marks.begin() + mark_range.end, false);

    if (has_final_mark)
        entry->matching_marks[marks_count - 1] = false;

    LOG_DEBUG(
        logger,
        "{} entry for table_id: {}, part_name: {}, condition_hash: {}, marks_count: {}, has_final_mark: {}, ranges: {}",
        inserted ? "Inserted" : "Updated",
        table_id,
        part_name,
        condition_hash,
        marks_count,
        has_final_mark,
        toString(mark_ranges));
}

std::optional<QueryConditionCache::MatchingMarks> QueryConditionCache::read(const UUID & table_id, const String & part_name, size_t condition_hash)
{
    Key key = {table_id, part_name, condition_hash};

    if (auto entry = cache.get(key))
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheHits);

        std::shared_lock lock(entry->mutex);

        LOG_DEBUG(
            logger,
            "Read entry for table_uuid: {}, part: {}, condition_hash: {}, ranges: {}",
            table_id,
            part_name,
            condition_hash,
            toString(entry->matching_marks));

        return {entry->matching_marks};
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheMisses);

        LOG_DEBUG(
            logger,
            "Could not find entry for table_uuid: {}, part: {}, condition_hash: {}",
            table_id,
            part_name,
            condition_hash);

        return {};
    }

}

std::vector<QueryConditionCache::Cache::KeyMapped> QueryConditionCache::dump() const
{
    return cache.dump();
}

void QueryConditionCache::clear()
{
    cache.clear();
}

void QueryConditionCache::setMaxSizeInBytes(size_t max_size_in_bytes)
{
    cache.setMaxSizeInBytes(max_size_in_bytes);
}

size_t QueryConditionCache::maxSizeInBytes()
{
    return cache.maxSizeInBytes();
}

bool QueryConditionCache::Key::operator==(const Key & other) const
{
    return table_id == other.table_id
        && part_name == other.part_name
        && condition_hash == other.condition_hash;
}

QueryConditionCache::Entry::Entry(size_t mark_count)
    : matching_marks(mark_count, true) /// by default, all marks potentially are potential matches, i.e. we can't skip them
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
    /// Estimate the memory size of `std::vector<bool>` (it uses bit-packing internally)
    size_t dynamic_memory = (entry.matching_marks.capacity() + 7) / 8; /// round up to bytes.
    return dynamic_memory + sizeof(decltype(entry.matching_marks));
}
}
