#pragma once

#include <Interpreters/Cache/PartialAggregateInfo.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <fmt/format.h>

namespace DB
{

/// Helper for constructing `PartialAggregateInfo` from a MergeTree data part (projection names use `parent:projection` form).
inline PartialAggregateInfoPtr partialAggregateInfoFromMergeTreePart(const IMergeTreeDataPart & data_part)
{
    const String part_name = data_part.isProjectionPart()
        ? fmt::format("{}:{}", data_part.getParentPartName(), data_part.name)
        : data_part.name;
    return std::make_shared<PartialAggregateInfo>(
        data_part.storage.getStorageID().uuid,
        part_name,
        data_part.info.mutation);
}

}
