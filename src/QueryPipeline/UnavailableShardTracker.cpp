#include <QueryPipeline/UnavailableShardTracker.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int TOO_MANY_UNAVAILABLE_SHARDS;
}

void UnavailableShardTracker::registerExtraUnits(size_t extra)
{
    total_units += extra;
}

void UnavailableShardTracker::seal()
{
    sealed = true;
    throwIfAllUnitsSkipped(unpack(skip_counts.load()).no_data);
}

void UnavailableShardTracker::throwIfAllUnitsSkipped(size_t observed_no_data) const
{
    if (!sealed)
        return;

    const size_t units = total_units;
    if (units > 0 && observed_no_data >= units)
        throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "No available shards to query");
}

void UnavailableShardTracker::onShardSkipped(bool produced_data)
{
    /// One transition advances both counts, so the pair below is a state the tracker really held and
    /// no sibling's skip can be counted inside it. Every throw here decides from that one pair, which
    /// is what keeps the reported error independent of how the units interleave.
    const UInt64 delta = (1ULL << 32) | (produced_data ? 0ULL : 1ULL);
    const auto [count, no_data] = unpack(skip_counts.fetch_add(delta) + delta);

    if (max_unavailable_num > 0 && count > max_unavailable_num)
        throw Exception(
            ErrorCodes::TOO_MANY_UNAVAILABLE_SHARDS,
            "Too many unavailable shards: {} out of {} total shards are unavailable, "
            "max_skip_unavailable_shards_num is set to {}",
            count, total_shards, max_unavailable_num);

    if (max_unavailable_ratio > 0 && total_shards > 0
        && static_cast<Float64>(count) / static_cast<Float64>(total_shards) > max_unavailable_ratio)
        throw Exception(
            ErrorCodes::TOO_MANY_UNAVAILABLE_SHARDS,
            "Too many unavailable shards: {} out of {} total shards are unavailable ({:.1f}%), "
            "max_skip_unavailable_shards_ratio is set to {}",
            count, total_shards, 100.0 * static_cast<double>(count) / static_cast<double>(total_shards), max_unavailable_ratio);

    throwIfAllUnitsSkipped(no_data);
}

}
