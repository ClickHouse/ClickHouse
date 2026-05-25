#pragma once

#include <Processors/Chunk.h>
#include <Core/UUID.h>
#include <base/types.h>

namespace DB
{

/// Metadata for partial aggregate cache lookups: table UUID, part name, and mutation version.
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

    /// Reserved; unused while query integration is not enabled.
    bool skip_execution_time_cache_lookup = false;
};

using PartialAggregateInfoPtr = std::shared_ptr<PartialAggregateInfo>;

}

