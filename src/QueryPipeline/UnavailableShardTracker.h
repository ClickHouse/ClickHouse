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
    std::atomic<size_t> unavailable_count{0};
    size_t total_shards;
    size_t max_unavailable_num;
    Float64 max_unavailable_ratio;

    UnavailableShardTracker(size_t total_shards_, size_t max_num_, Float64 max_ratio_)
        : total_shards(total_shards_)
        , max_unavailable_num(max_num_)
        , max_unavailable_ratio(max_ratio_)
    {
    }

    /// Called when a shard is determined to be unavailable and would be skipped.
    /// Throws if the configured thresholds are exceeded.
    void onShardSkipped();
};

using UnavailableShardTrackerPtr = std::shared_ptr<UnavailableShardTracker>;

}
