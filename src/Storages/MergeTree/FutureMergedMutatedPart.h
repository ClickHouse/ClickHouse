#pragma once

#include <base/types.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>


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

    const MergeTreePartition & getPartition() const { return parts.front()->partition; }
    bool isResultPatch() const { return !parts.empty() && parts.front()->info.isPatch();}

    FutureMergedMutatedPart() = default;

    FutureMergedMutatedPart(MergeTreeData::DataPartsVector parts_, MergeTreeData::DataPartsVector patch_parts_)
    {
        assign(std::move(parts_), std::move(patch_parts_));
    }

    void assign(MergeTreeData::DataPartsVector parts_, MergeTreeData::DataPartsVector patch_parts_);
    void assign(MergeTreeData::DataPartsVector parts_, MergeTreeData::DataPartsVector patch_parts_, MergeTreeDataPartFormat future_part_format);

    void updatePath(const MergeTreeData & storage, const IReservation * reservation);
};

using FutureMergedMutatedPartPtr = std::shared_ptr<FutureMergedMutatedPart>;

}
