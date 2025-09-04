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
    size_t memory = (entry.matching_marks.capacity() + 7) / 8; /// round up to bytes.
    return memory + sizeof(decltype(entry.matching_marks));
}

QueryConditionCache::QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : cache(cache_policy, CurrentMetrics::QueryConditionCacheBytes, CurrentMetrics::QueryConditionCacheEntries, max_size_in_bytes, 0, size_ratio)
{
}

void QueryConditionCache::write(
    const UUID & table_id, const String & part_name, UInt64 condition_hash, const String & condition,
    const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark)
{
    Key key = {table_id, part_name, condition_hash, condition};

    auto load_func = [&](){ return std::make_shared<Entry>(marks_count); };
    auto [entry, inserted] = cache.getOrSet(key, load_func);

    /// Try to avoid acquiring the RW lock below (*) by early-ing out. Matters for systems with lots of cores.
    {
        std::shared_lock shared_lock(entry->mutex); /// cheap

        bool need_not_update_marks = true;
        for (const auto & mark_range : mark_ranges)
        {
            /// If the bits are already in the desired state (false), we don't need to update them.
            need_not_update_marks = std::all_of(entry->matching_marks.begin() + mark_range.begin,
                                                entry->matching_marks.begin() + mark_range.end,
                                                [](auto b) { return b == false; });
            if (!need_not_update_marks)
                break;
        }

        /// Do we either have no final mark or final mark is already in the desired state?
        bool need_not_update_final_mark = !has_final_mark || entry->matching_marks[marks_count - 1] == false;

        if (need_not_update_marks && need_not_update_final_mark)
            return;
    }

    {
        std::lock_guard lock(entry->mutex); /// (*)

        chassert(marks_count == entry->matching_marks.size());

        /// The input mark ranges are the areas which the scan can skip later on.
        for (const auto & mark_range : mark_ranges)
            std::fill(entry->matching_marks.begin() + mark_range.begin, entry->matching_marks.begin() + mark_range.end, false);

        if (has_final_mark)
            entry->matching_marks[marks_count - 1] = false;
    }

    LOG_TEST(
        logger,
        "{} entry for table_id: {}, part_name: {}, condition_hash: {}, condition: {}, marks_count: {}, has_final_mark: {}",
        inserted ? "Inserted" : "Updated",
        table_id,
        part_name,
        condition_hash,
        condition,
        marks_count,
        has_final_mark);
}

std::optional<QueryConditionCache::MatchingMarks> QueryConditionCache::read(const UUID & table_id, const String & part_name, UInt64 condition_hash)
{
    Key key = {table_id, part_name, condition_hash, ""};

    if (auto entry = cache.get(key))
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheHits);

        std::shared_lock lock(entry->mutex);

        LOG_TEST(
            logger,
            "Read entry for table_uuid: {}, part: {}, condition_hash: {}",
            table_id,
            part_name,
            condition_hash);

        return {entry->matching_marks};
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

QueryConditionCache::Entry::Entry(size_t mark_count)
    : matching_marks(mark_count, true) /// by default, all marks potentially are potential matches, i.e. we can't skip them
{
}

}
