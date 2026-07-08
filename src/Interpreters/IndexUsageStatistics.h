#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Interpreters/StorageID.h>

#include <mutex>
#include <unordered_map>

namespace DB
{

/// Cumulative in-memory usage counters for data skipping indexes and projections,
/// an analogue of PostgreSQL's `pg_stat_user_indexes`. Filled by `ReadFromMergeTree`
/// during query execution and exposed via `system.data_skipping_indices` and
/// `system.projections`. Counters are kept per server and reset on restart.
class IndexUsageStatistics
{
public:
    enum class IndexKind : UInt8
    {
        Skip,
        Projection,
    };

    struct Key
    {
        UUID table_uuid = UUIDHelpers::Nil;
        /// Filled only when the table has no UUID (e.g. Ordinary database engine).
        String database;
        String table;
        IndexKind kind = IndexKind::Skip;
        String name;

        bool operator==(const Key & other) const = default;
    };

    struct Counters
    {
        UInt64 times_used = 0;
        UInt64 granules_evaluated = 0;
        UInt64 granules_dropped = 0;
        time_t last_used_time = 0;
    };

    static Key makeKey(const StorageID & storage_id, IndexKind kind, const String & name);

    void record(const Key & key, UInt64 granules_evaluated, UInt64 granules_dropped, time_t now);

    /// With `use_skip_indexes_on_data_read` the granules are dropped while reading, after the
    /// index was already counted as evaluated: add the dropped granules without a new evaluation.
    void addGranulesDropped(const Key & key, UInt64 granules_dropped);

    /// Returns zero counters for indexes that were never used.
    Counters get(const Key & key) const;

private:
    struct KeyHash
    {
        size_t operator()(const Key & key) const;
    };

    mutable std::mutex mutex;
    std::unordered_map<Key, Counters, KeyHash> counters TSA_GUARDED_BY(mutex);
};

using IndexUsageStatisticsPtr = std::shared_ptr<IndexUsageStatistics>;

}
