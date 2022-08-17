#pragma once

#include <functional>
#include <optional>
#include <vector>
#include <Disks/StoragePolicy.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/ActionBlocker.h>

namespace DB
{


/// Active part from storage and destination reservation where
/// it have to be moved.
struct MergeTreeMoveEntry
{
    std::shared_ptr<const IMergeTreeDataPart> part;
    ReservationPtr reserved_space;

    MergeTreeMoveEntry(const std::shared_ptr<const IMergeTreeDataPart> & part_, ReservationPtr reservation_)
        : part(part_), reserved_space(std::move(reservation_))
    {
    }
};

using MergeTreeMovingParts = std::vector<MergeTreeMoveEntry>;

/** Can select parts for background moves, clone them to appropriate disks into
 * /detached directory and replace them into active parts set
 */
class MergeTreePartsMover
{
private:
    /// Callback tells that part is not participating in background process
    using AllowedMovingPredicate = std::function<bool(const std::shared_ptr<const IMergeTreeDataPart> &, String * reason)>;

public:
    explicit MergeTreePartsMover(MergeTreeData * data_)
        : data(data_)
        , log(&Poco::Logger::get("MergeTreePartsMover"))
    {
    }

    /// Select parts for background moves according to storage_policy configuration.
    /// Returns true if at least one part was selected for move.
    bool selectPartsForMove(
        MergeTreeMovingParts & parts_to_move,
        const AllowedMovingPredicate & can_move,
        const std::lock_guard<std::mutex> & moving_parts_lock);

    /// Copies part to selected reservation in detached folder. Throws exception if part already exists.
    MergeTreeDataPartPtr clonePart(const MergeTreeMoveEntry & moving_part) const;

    /// Replaces cloned part from detached directory into active data parts set.
    /// Replacing part changes state to DeleteOnDestroy and will be removed from disk after destructor of
    ///IMergeTreeDataPart called. If replacing part doesn't exists or not active (committed) than
    /// cloned part will be removed and log message will be reported. It may happen in case of concurrent
    /// merge or mutation.
    void swapClonedPart(const MergeTreeDataPartPtr & cloned_parts) const;

    /// Can stop background moves and moves from queries
    ActionBlocker moves_blocker;

private:

    MergeTreeData * data;
    Poco::Logger * log;
};


}
