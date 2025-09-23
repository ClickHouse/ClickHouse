#include <Interpreters/Cache/QueryConditionCache.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>

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

/// The addRanges method tries to avoid exclusive locking like the plague. Shared locking is fine. The reason is that Clickhouse scan ranges
/// in parallel: addRanges method is frequently called and we need to avoid lock contention.
void QueryConditionCacheWriter::addRanges(const UUID & table_id, const String & part_name, const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark)
{
    QueryConditionCache::Key key = {table_id, part_name, condition_hash, condition};

    /// First, check if a cache entry is already registered for the key
    CacheEntryPtr cache_entry;
    {
        std::shared_lock shared_lock(mutex);
        auto it = new_entries.find(key);
        if (it != new_entries.end())
            cache_entry = it->second;
    }

    /// Check if we are lucky and don't need to update the matching marks of the entry (if found).
    bool need_update_marks = false;
    if (cache_entry)
    {
        /// Check under the entry's read lock. If no update is needed, exit immediately.
        std::shared_lock shared_lock(cache_entry->mutex);
        if (!needUpdateMarks(cache_entry->entry, mark_ranges, marks_count, has_final_mark))
            return;

        need_update_marks = true;
    }

    /// Marks potentially need to be updated. We don't know for sure because another thread could have updated them in the meantime.
    /// Acquire a write lock and attempt the update.
    bool is_insert = false;
    if (!need_update_marks)
    {
        /// Check from the query condition cache whether marks need to be updated,
        /// which helps avoid a large number of unnecessary updates and reduces lock contention.
        /// It also addresses the issue where marks in the cache entries are incomplete
        /// due to the LIMIT operator terminating the ranges scan early.
        auto entry_ptr = query_condition_cache.read(table_id, part_name, condition_hash);
        if (entry_ptr)
        {
            chassert(marks_count == entry_ptr->size());

            {
                /// Copy entry in query condition cache.
                std::lock_guard lock(mutex);
                auto [it, inserted] = new_entries.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(key),
                    std::forward_as_tuple(std::make_shared<CacheEntry>(*entry_ptr))
                );
                cache_entry = it->second;
            }

            if (!needUpdateMarks(*entry_ptr, mark_ranges, marks_count, has_final_mark))
                return;

            is_insert = true;
        }
    }

    /// Create new entry.
    if (!cache_entry)
    {
        std::lock_guard lock(mutex);
        /// If another thread created the entry in the meantime, emplace will do nothing.
        auto [it, inserted] = new_entries.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple(std::make_shared<CacheEntry>(marks_count))
        );
        cache_entry = it->second; /// either the one we inserted here or the one which prevented the insertion

        is_insert = true;
    }

    {
        std::lock_guard lock(cache_entry->mutex);
        /// The input mark ranges are the areas which the scan can skip later on.
        for (const auto & mark_range : mark_ranges)
            std::fill(cache_entry->entry.begin() + mark_range.begin, cache_entry->entry.begin() + mark_range.end, false);

        if (has_final_mark)
            cache_entry->entry[marks_count - 1] = false;

        cache_entry->is_dirty = true;
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

QueryConditionCacheWriter::CacheEntry::CacheEntry(const QueryConditionCache::Entry & entry_)
    : entry(entry_)
    , is_dirty(false)
{
}

/// Treat all marks for the new entry of the part as potential matches, i.e. don't skip them during read.
/// This is important for error handling: Imagine an exception is thrown during query execution and the stack is unwound. At that
/// point, a new entry may not have received updates for all scanned ranges within the part. As a result, future scans queries could
/// skip too many ranges, causing wrong results. This situation is prevented by initializing all marks of each entry as non-matching.
/// Even if there is an exception, future scans will not skip them.
QueryConditionCacheWriter::CacheEntry::CacheEntry(size_t marks_count)
    : entry(marks_count, true)
    , is_dirty(true)
{
}

void QueryConditionCacheWriter::finalize()
{
    std::lock_guard lock(mutex);
    for (const auto & [key, cache_entry] : new_entries)
    {
        auto & entry = cache_entry->entry;
        if (entry.empty() || !cache_entry->is_dirty)
            continue;

        size_t matching_marks = std::count(entry.begin(), entry.end(), true);
        double selectivity = static_cast<double>(matching_marks) / entry.size();
        if (selectivity <= selectivity_threshold)
            query_condition_cache.write(key, entry);
    }
}

bool QueryConditionCacheWriter::needUpdateMarks(const QueryConditionCache::Entry & entry, const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark) const
{
    bool need_not_update_marks = true;
    for (const auto & mark_range : mark_ranges)
    {
        /// If the bits are already in the desired state (false), we don't need to update them.
        need_not_update_marks = std::all_of(entry.begin() + mark_range.begin,
                                            entry.begin() + mark_range.end,
                                            [](auto b) { return b == false; });
        if (!need_not_update_marks)
            break;
    }

    /// Do we either have no final mark or final mark is already in the desired state?
    bool need_not_update_final_mark = !has_final_mark || entry[marks_count - 1] == false;

    if (need_not_update_marks && need_not_update_final_mark)
        return false;

    return true;
}

}
