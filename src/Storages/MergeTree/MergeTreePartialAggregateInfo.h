#pragma once

#include <Interpreters/Cache/PartialAggregateInfo.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>

#include <fmt/format.h>

namespace DB
{

/// Build `PartialAggregateInfo` from a MergeTree data part (projection names use `parent:projection` form).
/// The metadata identity and `skip_execution_time_cache_lookup` come from the `MergeTreeReaderSettings` of the read
/// (see `ReadFromMergeTree::initPartialAggregateCacheReaderSettings` and `ReadFromMergeTree::initializePipeline`).
inline PartialAggregateInfoPtr partialAggregateInfoFromMergeTreePart(const IMergeTreeDataPart & data_part, const MergeTreeReaderSettings & reader_settings)
{
    const String part_name = data_part.isProjectionPart()
        ? fmt::format("{}:{}", data_part.getParentPartName(), data_part.name)
        : data_part.name;
    auto info = std::make_shared<PartialAggregateInfo>(
        data_part.storage.getStorageID().uuid,
        part_name,
        data_part.info.mutation,
        reader_settings.partial_aggregate_metadata_identity);
    info->skip_execution_time_cache_lookup = reader_settings.skip_partial_aggregate_execution_cache_lookup;
    return info;
}

}
