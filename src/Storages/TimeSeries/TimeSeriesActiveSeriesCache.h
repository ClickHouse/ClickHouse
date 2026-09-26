#pragma once

#include <Columns/IColumn.h>
#include <base/types.h>
#include <Common/HashTable/HashMap.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>


namespace DB
{

/// In-memory working-set cache of active time series identifiers.
/// Skips redundant tag writes into the "tags" table for known active series.
class TimeSeriesActiveSeriesCache
{
public:
    explicit TimeSeriesActiveSeriesCache(size_t max_entries, UInt32 ttl_seconds);

    /// Checks which series in `id_column` must be written against committed cache entries.
    /// Fills `out_filter` with 1 for series to write and 0 for series to skip.
    void checkBulk(const ColumnPtr & id_column, UInt32 current_time, IColumn::Filter & out_filter, size_t & out_written_count) const;

    /// Commits series IDs to the shared cache once the tags pipeline has finished.
    void commit(const std::vector<UInt128> & ids, UInt32 commit_time = 0) const;

    /// Updates cache configuration settings (called e.g. on ALTER TABLE SETTINGS).
    void updateSettings(size_t max_entries, UInt32 ttl_seconds) const;

    /// Helper to extract a 128-bit series identifier from any column type.
    static UInt128 extractId(const IColumn & id_column, size_t row);

private:
    static constexpr size_t NUM_SHARDS = 64;

    struct Shard
    {
        mutable std::mutex mutex;
        HashMap<UInt128, UInt32, HashCRC32<UInt128>> map;
        size_t max_shard_entries = 0;
        bool unlimited = false;
    };

    mutable std::vector<Shard> shards;
    mutable std::atomic<UInt32> ttl_seconds;

    static size_t getShardIndex(const UInt128 & id)
    {
        UInt64 h = id.items[0] ^ id.items[1];
        return static_cast<size_t>(h & (NUM_SHARDS - 1));
    }
};

using TimeSeriesActiveSeriesCachePtr = std::shared_ptr<const TimeSeriesActiveSeriesCache>;

}
