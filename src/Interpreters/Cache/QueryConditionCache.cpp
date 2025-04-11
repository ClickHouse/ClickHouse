#include <mutex>
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

void QueryConditionCache::write(const QueryConditionCacheKey & key, const MatchingMarks & matching_marks)
{
    cache.set(key, std::make_shared<MatchingMarks>(matching_marks));
}

MatchingMarksPtr QueryConditionCache::read(const UUID & table_id, const String & part_name, size_t condition_hash)
{
    QueryConditionCacheKey key = {table_id, part_name, condition_hash};

    if (auto entry = cache.get(key))
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheHits);
        LOG_TRACE(
            logger,
            "Read entry for table_uuid: {}, part: {}, condition_hash: {}, ranges: {}",
            table_id,
            part_name,
            condition_hash,
            toString(*entry));

        return entry;
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheMisses);

        LOG_TRACE(
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

bool QueryConditionCacheKey::operator==(const QueryConditionCacheKey & other) const
{
    return table_id == other.table_id
        && part_name == other.part_name
        && condition_hash == other.condition_hash;
}

size_t QueryConditionCacheKeyHasher::operator()(const QueryConditionCacheKey & key) const
{
    SipHash hash;
    hash.update(key.table_id);
    hash.update(key.part_name);
    hash.update(key.condition_hash);
    return hash.get64();
}

size_t QueryConditionCache::QueryConditionCacheEntryWeight::operator()(const MatchingMarks & entry) const
{
    /// Estimate the memory size of `std::vector<bool>` (it uses bit-packing internally)
    size_t dynamic_memory = (entry.capacity() + 7) / 8; /// round up to bytes.
    return dynamic_memory + sizeof(decltype(entry));
}

void QueryConditionCacheWriter::buffer(const UUID & table_id, const String & part_name, const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark)
{
    std::lock_guard<std::mutex> lock(mutex);

    QueryConditionCacheKey key{table_id, part_name, condition_hash};
    if (auto it = buffered.find(key); it != buffered.end())
    {
        // it->second->appendMarkRanges(mark_ranges_info->mark_ranges);
        MatchingMarks & matching_marks = it->second;

        chassert(marks_count == matching_marks.size());

        /// The input mark ranges are the areas which the scan can skip later on.
        for (const auto & mark_range : mark_ranges)
            std::fill(matching_marks.begin() + mark_range.begin, matching_marks.begin() + mark_range.end, false);

        if (has_final_mark)
            matching_marks[marks_count - 1] = false;
    }
    else
    {
        /// by default, all marks potentially are potential matches, i.e. we can't skip them.
        MatchingMarks matching_marks(marks_count, true);

        for (const auto & mark_range : mark_ranges)
            std::fill(matching_marks.begin() + mark_range.begin, matching_marks.begin() + mark_range.end, false);

        if (has_final_mark)
            matching_marks[marks_count - 1] = false;

        buffered[key] = std::move(matching_marks);
    }

    LOG_TRACE(
        logger,
        "Inserted entry for table_id: {}, part_name: {}, condition_hash: {}, marks_count: {}, has_final_mark: {}, ranges: {}",
        table_id,
        part_name,
        condition_hash,
        marks_count,
        has_final_mark,
        toString(mark_ranges));
}

void QueryConditionCacheWriter::finalize()
{
    std::lock_guard<std::mutex> lock(mutex);

    for (auto & [key, matching_marks] : buffered)
    {
        if (matching_marks.empty())
            continue;

        size_t count = std::count(matching_marks.begin(), matching_marks.end(), false);
        double ratio = static_cast<double>(count) / matching_marks.size();
        if (ratio > query_condition_cache_zero_ratio_threshold)
            query_condition_cache->write(key, matching_marks);
    }
}
}
