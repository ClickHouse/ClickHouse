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

bool QueryConditionCache::Key::operator==(const Key & other) const
{
    return table_id == other.table_id
        && part_name == other.part_name
        && condition_hash == other.condition_hash;
}

size_t QueryConditionCache::KeyHasher::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.table_id);
    hash.update(key.part_name);
    hash.update(key.condition_hash);
    return hash.get64();
}

size_t QueryConditionCache::EntryWeight::operator()(const Entry & entry) const
{
    /// Estimate the memory size of `std::vector<bool>` (it uses bit-packing internally)
    size_t memory = (entry.matching_marks.capacity() + 7) / 8; /// round up to bytes.
    return memory + sizeof(decltype(entry.matching_marks));
}

QueryConditionCache::QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : cache(cache_policy, max_size_in_bytes, 0, size_ratio)
{
}

QueryConditionCache::EntryPtr QueryConditionCache::read(const UUID & table_id, const String & part_name, UInt64 condition_hash)
{
    Key key = {table_id, part_name, condition_hash, ""};

    if (auto entry = cache.get(key))
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheHits);

        LOG_TRACE(
            logger,
            "Read entry for table_uuid: {}, part: {}, condition_hash: {}",
            table_id,
            part_name,
            condition_hash);

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

void QueryConditionCache::write(
    const Key & key, const MatchingMarks & matching_marks)
{
    cache.set(key, std::make_shared<Entry>(matching_marks));
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

QueryConditionCacheWriter::QueryConditionCacheWriter(
    QueryConditionCachePtr query_condition_cache_,
    UInt64 condition_hash_,
    const String & condition_)
    : query_condition_cache(query_condition_cache_)
    , condition_hash(condition_hash_)
    , condition(condition_)
{}

QueryConditionCacheWriter::~QueryConditionCacheWriter()
{
    std::lock_guard<std::mutex> lock(mutex);

    for (const auto & [key, matching_marks] : buffer)
    {
        if (matching_marks.empty())
            continue;

        query_condition_cache->write(key, matching_marks);
    }
}

void QueryConditionCacheWriter::write(const UUID & table_id, const String & part_name, const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark)
{
    std::lock_guard<std::mutex> lock(mutex);

    QueryConditionCache::Key key{table_id, part_name, condition_hash, condition};

    auto it = buffer.find(key);
    if (it != buffer.end())
    {
        QueryConditionCache::MatchingMarks & matching_marks = it->second;

        chassert(marks_count == matching_marks.size());

        /// The input mark ranges are the areas which the scan can skip later on.
        for (const auto & mark_range : mark_ranges)
            std::fill(matching_marks.begin() + mark_range.begin, matching_marks.begin() + mark_range.end, false);

        if (has_final_mark)
            matching_marks[marks_count - 1] = false;
    }
    else
    {
        /// All marks are initially potential matches, i.e. we can't skip them
        QueryConditionCache::MatchingMarks matching_marks(marks_count, true);

        /// Flip areas in the ranges which the scan can skip later on.
        for (const auto & mark_range : mark_ranges)
            std::fill(matching_marks.begin() + mark_range.begin, matching_marks.begin() + mark_range.end, false);

        if (has_final_mark)
            matching_marks[marks_count - 1] = false;

        buffer[key] = matching_marks;
    }

    LOG_TRACE(
        logger,
        "{} entry for table_id: {}, part_name: {}, condition_hash: {}, marks_count: {}, has_final_mark: {}",
        it != buffer.end() ? "Updated" : "Inserted",
        table_id,
        part_name,
        condition_hash,
        marks_count,
        has_final_mark);
}

}
