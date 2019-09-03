#pragma once

#include <functional>
#include <vector>
#include <optional>
#include <Storages/MergeTree/MergeTreeData.h>
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
    MergeTreeData::DataPartPtr part;
    DiskSpace::ReservationPtr reserved_space;

    MergeTreeMoveEntry(const MergeTreeData::DataPartPtr & part_, DiskSpace::ReservationPtr reservation_)
        : part(part_),
          reserved_space(std::move(reservation_))
    {
    }
};

using MergeTreeMovingParts = std::vector<MergeTreeMoveEntry>;

struct MovingPartsTagger
{
    MergeTreeMovingParts parts_to_move;
    std::unique_lock<std::mutex> background_lock;
    MergeTreeData::DataParts & all_moving_parts;


    MovingPartsTagger(MergeTreeMovingParts && moving_parts_,
        std::unique_lock<std::mutex> && background_lock_,
        MergeTreeData::DataParts & all_moving_data_parts_)
        : parts_to_move(std::move(moving_parts_))
        , background_lock(std::move(background_lock_))
        , all_moving_parts(all_moving_data_parts_)
    {
        if (!background_lock)
            throw Exception("Cannot tag moving parts without background lock.", ErrorCodes::LOGICAL_ERROR);

        for (const auto & moving_part : parts_to_move)
            if (!all_moving_parts.emplace(moving_part.part).second)
                throw Exception("Cannot move part '" + moving_part.part->name + "'. It's already moving.", ErrorCodes::LOGICAL_ERROR);

        background_lock.unlock();
    }

    ~MovingPartsTagger()
    {
        background_lock.lock();
        for (const auto & moving_part : parts_to_move)
        {
            /// Something went completely wrong
            if (!all_moving_parts.count(moving_part.part))
                std::terminate();
            all_moving_parts.erase(moving_part.part);
        }

    }
};

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

    MergeTreeData::DataPartPtr clonePart(const MergeTreeMoveEntry & moving_part) const;

    void swapClonedPart(const MergeTreeData::DataPartPtr & cloned_parts) const;

public:
    ActionBlocker moves_blocker;

private:

    MergeTreeData & data;
    Logger * log;
};


}
