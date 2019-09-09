#include <Storages/MergeTree/MergeTreePartsMover.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <set>
#include <boost/algorithm/string/join.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int NO_SUCH_DATA_PART;
}

namespace
{

/// Contains minimal number of heaviest parts, which sum size on disk is greater than required.
/// If there are not enough summary size, than contains all.
class LargestPartsWithRequiredSize
{
    struct PartsSizeOnDiskComparator
    {
        bool operator()(const MergeTreeData::DataPartPtr & f, const MergeTreeData::DataPartPtr & s) const
        {
            return f->bytes_on_disk < s->bytes_on_disk;
        }
    };

    std::set<MergeTreeData::DataPartPtr, PartsSizeOnDiskComparator> elems;
    UInt64 required_size_sum;
    UInt64 current_size_sum = 0;

public:
    LargestPartsWithRequiredSize(UInt64 required_sum_size_) : required_size_sum(required_sum_size_) {}

    void add(MergeTreeData::DataPartPtr part)
    {
        if (current_size_sum < required_size_sum)
        {
            elems.emplace(part);
            current_size_sum += part->bytes_on_disk;
            return;
        }

        /// Adding smaller element
        if (!elems.empty() && (*elems.begin())->bytes_on_disk >= part->bytes_on_disk)
            return;

        elems.emplace(part);
        current_size_sum += part->bytes_on_disk;

        while (!elems.empty() && (current_size_sum - (*elems.begin())->bytes_on_disk >= required_size_sum))
        {
            current_size_sum -= (*elems.begin())->bytes_on_disk;
            elems.erase(elems.begin());
        }
    }

    /// Returns elems ordered by size
    MergeTreeData::DataPartsVector getAccumulatedParts()
    {
        MergeTreeData::DataPartsVector res;
        for (const auto & elem : elems)
            res.push_back(elem);
        return res;
    }
};

}

bool MergeTreePartsMover::selectPartsForMove(
    MergeTreeMovingParts & parts_to_move,
    const AllowedMovingPredicate & can_move,
    const std::lock_guard<std::mutex> & /* moving_parts_lock */)
{
    MergeTreeData::DataPartsVector data_parts = data->getDataPartsVector();

    if (data_parts.empty())
        return false;

    std::unordered_map<DiskSpace::DiskPtr, LargestPartsWithRequiredSize> need_to_move;
    const auto & policy = data->getStoragePolicy();
    const auto & volumes = policy->getVolumes();

    /// Do not check if policy has one volume
    if (volumes.size() == 1)
        return false;

    /// Do not check last volume
    for (size_t i = 0; i != volumes.size() - 1; ++i)
    {
        for (const auto & disk : volumes[i]->disks)
        {
            auto space_information = disk->getSpaceInformation();

            UInt64 required_available_space = space_information.getTotalSpace() * policy->getMoveFactor();

            if (required_available_space > space_information.getAvailableSpace())
                need_to_move.emplace(disk, required_available_space - space_information.getAvailableSpace());
        }
    }

    for (const auto & part : data_parts)
    {
        String reason;
        if (!can_move(part, &reason))
            continue;

        auto to_insert = need_to_move.find(part->disk);
        if (to_insert != need_to_move.end())
            to_insert->second.add(part);
    }

    for (auto && move : need_to_move)
    {
        auto min_volume_priority = policy->getVolumePriorityByDisk(move.first) + 1;
        for (auto && part : move.second.getAccumulatedParts())
        {
            auto reservation = policy->reserve(part->bytes_on_disk, min_volume_priority);
            if (!reservation)
            {
                /// Next parts to move from this disk has greater size and same min volume priority
                /// There are no space for them
                /// But it can be possible to move data from other disks
                break;
            }
            parts_to_move.emplace_back(part, std::move(reservation));
        }
    }

    return !parts_to_move.empty();
}

MergeTreeData::DataPartPtr MergeTreePartsMover::clonePart(const MergeTreeMoveEntry & moving_part) const
{
    if (moves_blocker.isCancelled())
        throw Exception("Cancelled moving parts.", ErrorCodes::ABORTED);

    LOG_TRACE(log, "Cloning part " << moving_part.part->name);
    moving_part.part->makeCloneOnDiskDetached(moving_part.reserved_space);

    MergeTreeData::MutableDataPartPtr cloned_part =
        std::make_shared<MergeTreeData::DataPart>(*data, moving_part.reserved_space->getDisk(), moving_part.part->name);
    cloned_part->relative_path = "detached/" + moving_part.part->name;
    LOG_TRACE(log, "Part " << moving_part.part->name << " was cloned to " << cloned_part->getFullPath());

    cloned_part->loadColumnsChecksumsIndexes(true, true);
    return cloned_part;

}


void MergeTreePartsMover::swapClonedPart(const MergeTreeData::DataPartPtr & cloned_part) const
{
    if (moves_blocker.isCancelled())
        throw Exception("Cancelled moving parts.", ErrorCodes::ABORTED);

    auto active_part = data->getActiveContainingPart(cloned_part->name);

    if (!active_part || active_part->name != cloned_part->name)
        throw Exception("Failed to swap " + cloned_part->name + ". Active part doesn't exist."
            + " It can be removed by merge or deleted by hand. Will remove copy on path '"
            + cloned_part->getFullPath() + "'.", ErrorCodes::NO_SUCH_DATA_PART);

    cloned_part->renameTo(active_part->name);

    data->swapActivePart(cloned_part);

    LOG_TRACE(log, "Part " << cloned_part->name << " was moved to " << cloned_part->getFullPath());
}

}
