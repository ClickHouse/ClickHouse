#pragma once

#include <atomic>
#include <memory>

#include <base/types.h>

namespace DB
{

/// Tracks the number of unavailable shards that were skipped during distributed query execution.
/// Shared across all RemoteQueryExecutor instances for a single query.
/// If the number or ratio of skipped shards exceeds the configured thresholds, throws an exception.
struct UnavailableShardTracker
{
    size_t total_shards;
    size_t max_unavailable_num;
    Float64 max_unavailable_ratio;

    /// Skipped units in the high half; in the low half how many of those returned no data (a unit that
    /// streamed rows first is excluded). One word, so one skip advances both. Counts stay under 2^32.
    std::atomic<UInt64> skip_counts{0};

    /// One unit per logical shard, plus one per additional RemoteQueryExecutor a shard fans out to
    /// (custom-key parallel replicas). Distinct from `total_shards`, which the two limits above use.
    std::atomic<size_t> total_units;
    /// Until set, the all-units check is not evaluated: units are still being registered.
    std::atomic<bool> sealed{false};

    UnavailableShardTracker(size_t total_shards_, size_t max_num_, Float64 max_ratio_)
        : total_shards(total_shards_)
        , max_unavailable_num(max_num_)
        , max_unavailable_ratio(max_ratio_)
        , total_units(total_shards_)
    {
    }

    struct SkipCounts
    {
        size_t skipped;
        size_t no_data;
    };

    static SkipCounts unpack(UInt64 packed) { return {packed >> 32, packed & 0xFFFFFFFFULL}; }

    /// Called when a shard is determined to be unavailable and would be skipped.
    /// `produced_data` tells whether that unit had already returned rows.
    /// Throws if the configured thresholds are exceeded, or if every unit was skipped without data.
    void onShardSkipped(bool produced_data = false);

    /// Adds execution units beyond the one-per-logical-shard baseline.
    void registerExtraUnits(size_t extra);

    /// Declares the topology complete and re-checks the all-units condition. Idempotent.
    void seal();

private:
    void throwIfAllUnitsSkipped(size_t observed_no_data) const;
};

using UnavailableShardTrackerPtr = std::shared_ptr<UnavailableShardTracker>;

}
