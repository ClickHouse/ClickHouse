#pragma once

#include <base/types.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Core/UUID.h>


namespace DB
{

class MergeTreeData;

/// Auxiliary struct holding metainformation for the future merged or mutated part.
struct FutureMergedMutatedPart
{
    String name;
    UUID uuid = UUIDHelpers::Nil;
    String path;
    MergeTreeDataPartFormat part_format;
    MergeTreePartInfo part_info;
    MergeTreeData::DataPartsVector parts;
    MergeTreeData::DataPartsVector patch_parts;
    std::vector<std::string> blocking_parts_to_remove;
    MergeType merge_type = MergeType::Regular;
    bool final = false;
    /// Set at selection time for a mutation admitted as hardlink-only (see
    /// MutationHelpers::isHardlinkOnlyMutation). Such a mutation holds a small reservation instead
    /// of one covering the whole source part, so it must not take a path that copies or rewrites
    /// data. MutateTask re-validates this on the write side and throws if it no longer holds.
    bool hardlink_only = false;

    const MergeTreePartition & getPartition() const { return parts.front()->partition; }
    bool isResultPatch() const { return !parts.empty() && parts.front()->info.isPatch();}

    void assign(MergeTreeData::DataPartsVector parts_, MergeTreeData::DataPartsVector patch_parts_, ProjectionDescriptionRawPtr projection);
    void assign(MergeTreeData::DataPartsVector parts_, MergeTreeData::DataPartsVector patch_parts_, MergeTreeDataPartFormat future_part_format);

    void updatePath(const MergeTreeData & storage, const IReservation * reservation);
};

using FutureMergedMutatedPartPtr = std::shared_ptr<FutureMergedMutatedPart>;

}
