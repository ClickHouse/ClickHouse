#include <Interpreters/Cache/QueryConditionCache.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>

namespace ProfileEvents
{
    extern const Event QueryConditionCacheHits;
    extern const Event QueryConditionCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric QueryConditionCacheBytes;
    extern const Metric QueryConditionCacheEntries;
}

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
    size_t memory = (entry.capacity() + 7) / 8; /// round up to bytes.
    return memory + sizeof(decltype(entry));
}

QueryConditionCache::QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : cache(cache_policy, CurrentMetrics::QueryConditionCacheBytes, CurrentMetrics::QueryConditionCacheEntries, max_size_in_bytes, 0, size_ratio)
{
}

void QueryConditionCache::write(const Key & key, const Entry & entry)
{
    cache.set(key, std::make_shared<Entry>(entry));
}

QueryConditionCache::EntryPtr QueryConditionCache::read(const UUID & table_id, const String & part_name, UInt64 condition_hash)
{
    Key key = {table_id, part_name, condition_hash, ""};

    if (auto entry = cache.get(key))
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheHits);

        LOG_TEST(
            logger,
            "Read entry for table_uuid: {}, part: {}, condition_hash: {}",
            table_id,
            part_name,
            condition_hash);

        return {entry};
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheMisses);

        LOG_TEST(
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

size_t QueryConditionCache::maxSizeInBytes() const
{
    return cache.maxSizeInBytes();
}

QueryConditionCacheWriter::QueryConditionCacheWriter(
    QueryConditionCache & query_condition_cache_,
    size_t condition_hash_,
    const String & condition_,
    double selectivity_threshold_)
    : query_condition_cache(query_condition_cache_)
    , condition_hash(condition_hash_)
    , condition(condition_)
    , selectivity_threshold(selectivity_threshold_)
    /// Implementation note: It would be nicer to pass in the table_id as well ...
{}

QueryConditionCacheWriter::~QueryConditionCacheWriter()
{
    finalize();
}

void QueryConditionCacheWriter::addRanges(const UUID & table_id, const String & part_name, const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark)
{
    QueryConditionCache::Key key = {table_id, part_name, condition_hash, condition};

    std::lock_guard<std::mutex> lock(mutex);

    bool is_insert;

    /// ClickHouse scans ranges within the same part in parallel.
    /// The first scan thread which calls addRanges creates and inserts an entry.
    /// The other scan threads update existing entry and update it.
    if (auto it = new_entries.find(key); it == new_entries.end())
    {
        /// By default, all marks potentially are potential matches, i.e. we can't skip them.
        /// Treat all marks for the new entry of the part as potential matches, i.e. don't skip them during read.
        /// This is important for error handling: Imagine an exception is thrown during query execution and the stack is unwound. At that
        /// point, a new entry may not have received updates for all scanned ranges within the part. As a result, future scans queries could
        /// skip too many ranges, causing wrong results. This situation is prevented by initializing all marks of each entry as non-matching.
        /// Even if there is an exception, future scans will not skip them.
        QueryConditionCache::Entry entry(marks_count, true);

        for (const auto & mark_range : mark_ranges)
            std::fill(entry.begin() + mark_range.begin, entry.begin() + mark_range.end, false);

        if (has_final_mark)
            entry[marks_count - 1] = false;

        new_entries[key] = std::move(entry);

        is_insert = true;
    }
    else
    {
        QueryConditionCache::Entry & matching_marks = it->second;

        chassert(marks_count == matching_marks.size());

        /// The input mark ranges are the areas which the scan can skip later on.
        for (const auto & mark_range : mark_ranges)
            std::fill(matching_marks.begin() + mark_range.begin, matching_marks.begin() + mark_range.end, false);

        if (has_final_mark)
            matching_marks[marks_count - 1] = false;

        is_insert = false;
    }

    LOG_TEST(
        logger,
        "{} entry for table_id: {}, part_name: {}, condition_hash: {}, condition: {}, marks_count: {}, has_final_mark: {}",
        is_insert ? "Inserted" : "Updated",
        table_id,
        part_name,
        condition_hash,
        condition,
        marks_count,
        has_final_mark);
}

void QueryConditionCacheWriter::finalize()
{
    std::lock_guard<std::mutex> lock(mutex);

    for (const auto & [key, entry] : new_entries)
    {
        if (entry.empty())
            continue;

        size_t matching_marks = std::count(entry.begin(), entry.end(), true);
        double selectivity = static_cast<double>(matching_marks) / entry.size();
        if (selectivity <= selectivity_threshold)
            query_condition_cache.write(key, entry);
    }
}

}
