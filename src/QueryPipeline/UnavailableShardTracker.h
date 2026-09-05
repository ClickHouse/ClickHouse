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

    /// Both skip counts live in one word so a single unit's skip advances them indivisibly: the
    /// number of skipped units in the high half, and in the low half how many of those never
    /// produced data. A unit that streamed rows before being skipped is excluded from the low half,
    /// since it did contribute to the result. Shard counts never approach 2^32.
    std::atomic<UInt64> skip_counts{0};

    /// Execution units are the things that can independently report a skip: one per logical shard,
    /// plus one extra for every additional RemoteQueryExecutor a shard fans out to (parallel replicas
    /// with a custom key). Kept apart from `total_shards`, which remains the logical shard count the
    /// two limits above are documented against.
    std::atomic<size_t> total_units;
    /// While false, the all-units check is not evaluated: units are registered as the pipeline is
    /// built, so a skip can be reported before the remaining units exist.
    std::atomic<bool> sealed{false};

    UnavailableShardTracker(size_t total_shards_, size_t max_num_, Float64 max_ratio_)
        : total_shards(total_shards_)
        , max_unavailable_num(max_num_)
        , max_unavailable_ratio(max_ratio_)
        , total_units(total_shards_)
    {
    }

    /// One skip's view of both counts.
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

    /// Declares the topology complete and re-checks the all-units condition, which may have become
    /// true while units were still being registered. Idempotent.
    void seal();

private:
    /// Takes the skipped-unit count so the caller decides from the value it observed.
    void throwIfAllUnitsSkipped(size_t observed_no_data) const;
};

using UnavailableShardTrackerPtr = std::shared_ptr<UnavailableShardTracker>;

}
