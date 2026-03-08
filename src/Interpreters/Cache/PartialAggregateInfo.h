#pragma once

#include <Processors/Chunk.h>
#include <Core/UUID.h>
#include <base/types.h>

namespace DB
{

/// Chunk info that carries MergeTree part information for partial aggregate caching.
/// This is attached to chunks during MergeTree reading when use_partial_aggregate_cache is enabled.
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

    /// Table identification
    UUID table_uuid = UUIDHelpers::Nil;

    /// Part identification (used as cache key components)
    String part_name;
    UInt64 part_mutation_version = 0;
};

using PartialAggregateInfoPtr = std::shared_ptr<PartialAggregateInfo>;

}

