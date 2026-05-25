#pragma once

#include <Processors/Chunk.h>
#include <Core/UUID.h>
#include <base/types.h>

namespace DB
{

/// Part identity for `use_partial_aggregate_cache` execution-time path (hits, misses, and `put`).
/// Attached in `MergeTreeSource`: fixed part at plan time (`partial_aggregate_identity_from_plan`, in-order reads) or
/// `MergeTreeSelectProcessor::buildPartialAggregateInfoFromCurrentTask` for pooled reads.
/// `skip_execution_time_cache_lookup`: plan-time `PartialAggregateCache::get` already missed for this part
/// (`ReadFromMergeTree` sets `MergeTreeReaderSettings::skip_partial_aggregate_execution_cache_lookup` on miss readers).
class PartialAggregateInfo : public ChunkInfoCloneable<PartialAggregateInfo>
{
public:
    PartialAggregateInfo() = default;
    PartialAggregateInfo(const PartialAggregateInfo & other) = default;

    PartialAggregateInfo(
        UUID table_uuid_,
        const String & part_name_,
        UInt64 part_mutation_version_)
        : table_uuid(table_uuid_)
        , part_name(part_name_)
        , part_mutation_version(part_mutation_version_)
    {
    }

    UUID table_uuid = UUIDHelpers::Nil;

    String part_name;
    UInt64 part_mutation_version = 0;

    /// When `ReadFromMergeTree` already probed `PartialAggregateCache` at plan time and missed, skip a redundant `get` in `AggregatingTransform`.
    bool skip_execution_time_cache_lookup = false;
};

using PartialAggregateInfoPtr = std::shared_ptr<PartialAggregateInfo>;

}

