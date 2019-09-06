#pragma once

#include <functional>
#include <vector>
#include <optional>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Common/ActionBlocker.h>
#include <Common/DiskSpaceMonitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct MergeTreeMoveEntry
{
    std::shared_ptr<const MergeTreeDataPart> part;
    DiskSpace::ReservationPtr reserved_space;

    MergeTreeMoveEntry(const std::shared_ptr<const MergeTreeDataPart> & part_, DiskSpace::ReservationPtr reservation_)
        : part(part_), reserved_space(std::move(reservation_))
    {
    }
};

using MergeTreeMovingParts = std::vector<MergeTreeMoveEntry>;


class MergeTreePartsMover


{
private:
    using AllowedMovingPredicate = std::function<bool(const std::shared_ptr<const MergeTreeDataPart> &, String * reason)>;

public:
    MergeTreePartsMover(MergeTreeData * data_)
        : data(data_)
        , log(&Poco::Logger::get("MergeTreePartsMover"))
    {
    }

    bool selectPartsForMove(
        MergeTreeMovingParts & parts_to_move,
        const AllowedMovingPredicate & can_move,
        const std::lock_guard<std::mutex> & moving_parts_lock);

    std::shared_ptr<const MergeTreeDataPart> clonePart(const MergeTreeMoveEntry & moving_part) const;

    void swapClonedPart(const std::shared_ptr<const MergeTreeDataPart> & cloned_parts) const;

public:
    ActionBlocker moves_blocker;

private:

    MergeTreeData * data;
    Logger * log;
};


}
