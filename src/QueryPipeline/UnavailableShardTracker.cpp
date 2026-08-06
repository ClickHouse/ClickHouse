#include <QueryPipeline/UnavailableShardTracker.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_UNAVAILABLE_SHARDS;
}

void UnavailableShardTracker::onShardSkipped()
{
    size_t count = ++unavailable_count;

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
}

}
