#pragma once

#include <Common/DiskSpaceMonitor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <functional>
#include <vector>

namespace DB
{


struct MargeTreeMoveEntry
{
    MergeTreeData::DataPartPtr part;
    DiskSpace::ReservationPtr reserved_space;

    MargeTreeMoveEntry(const MergeTreeData::DataPartPtr & part_, DiskSpace::ReservationPtr reservation_)
        : part(part_),
          reserved_space(std::move(reservation_))
    {
    }
};

using MergeTreeMovingParts = std::vector<MargeTreeMoveEntry>;

class MergeTreePartsMover
{
    using AllowedMovingPredicate = std::function<bool (const MergeTreeData::DataPartPtr &, String * reason)>;
public:
    MergeTreePartsMover(MergeTreeData & data_)
        : data(data_)
        , log(&Poco::Logger::get("MergeTreePartsMover"))
    {
    }

    bool selectPartsToMove(
        MergeTreeMovingParts & parts_to_move,
        const AllowedMovingPredicate & can_move);

    MergeTreeData::DataPartsVector cloneParts(const MergeTreeMovingParts & parts);

    bool swapClonedParts(const MergeTreeData::DataPartsVector & cloned_parts, String * out_reason);

private:

    MergeTreeData & data;
    Logger * log;
};

}
