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

    const MergeTreePartition & getPartition() const { return parts.front()->partition; }
    bool isResultPatch() const { return !parts.empty() && parts.front()->info.isPatch();}

    void assign(MergeTreeData::DataPartsVector parts_, MergeTreeData::DataPartsVector patch_parts_, ProjectionDescriptionRawPtr projection);
    void assign(MergeTreeData::DataPartsVector parts_, MergeTreeData::DataPartsVector patch_parts_, MergeTreeDataPartFormat future_part_format);

    /// Raise the mutation version of the result part above what `assign` derived from the sources.
    /// Used to record mutations that the operation materializes by itself, so that they are not
    /// applied a second time to the result part.
    void raiseMutationVersion(Int64 mutation_version);

    void updatePath(const MergeTreeData & storage, const IReservation * reservation);

private:
    /// Derives `name` from `part_info` and the source parts. Must be called after every change of
    /// `part_info`, because the name encodes it.
    void updateName();
};

using FutureMergedMutatedPartPtr = std::shared_ptr<FutureMergedMutatedPart>;

}
