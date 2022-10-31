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
    MergeTreeDataPartType type;
    MergeTreePartInfo part_info;
    MergeTreeData::DataPartsVector parts;
    MergeType merge_type = MergeType::Regular;

    const MergeTreePartition & getPartition() const { return parts.front()->partition; }

    FutureMergedMutatedPart() = default;

    explicit FutureMergedMutatedPart(MergeTreeData::DataPartsVector parts_)
    {
        assign(std::move(parts_));
    }

    FutureMergedMutatedPart(MergeTreeData::DataPartsVector parts_, MergeTreeDataPartType future_part_type)
    {
        assign(std::move(parts_), future_part_type);
    }

    void assign(MergeTreeData::DataPartsVector parts_);
    void assign(MergeTreeData::DataPartsVector parts_, MergeTreeDataPartType future_part_type);

    void updatePath(const MergeTreeData & storage, const IReservation * reservation);
};

using FutureMergedMutatedPartPtr = std::shared_ptr<FutureMergedMutatedPart>;

}
