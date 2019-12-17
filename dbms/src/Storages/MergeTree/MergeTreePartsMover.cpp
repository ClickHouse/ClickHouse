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
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Contains minimal number of heaviest parts, which sum size on disk is greater than required.
/// If there are not enough summary size, than contains all parts.
class LargestPartsWithRequiredSize
{
    struct PartsSizeOnDiskComparator
    {
        bool operator()(const MergeTreeData::DataPartPtr & f, const MergeTreeData::DataPartPtr & s) const
        {
            /// If parts have equal sizes, than order them by names (names are unique)
            return std::tie(f->bytes_on_disk, f->name) < std::tie(s->bytes_on_disk, s->name);
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

        removeRedundantElements();
    }

    /// Weaken requirements on size
    void decreaseRequiredSizeAndRemoveRedundantParts(UInt64 size_decrease)
    {
        required_size_sum -= std::min(size_decrease, required_size_sum);
        removeRedundantElements();
    }

    /// Returns parts ordered by size
    MergeTreeData::DataPartsVector getAccumulatedParts()
    {
        MergeTreeData::DataPartsVector res;
        for (const auto & elem : elems)
            res.push_back(elem);
        return res;
    }

private:
    void removeRedundantElements()
    {
        while (!elems.empty() && (current_size_sum - (*elems.begin())->bytes_on_disk >= required_size_sum))
        {
            current_size_sum -= (*elems.begin())->bytes_on_disk;
            elems.erase(elems.begin());
        }
    }
};

}

bool MergeTreePartsMover::selectPartsForMove(
    MergeTreeMovingParts & parts_to_move,
    const AllowedMovingPredicate & can_move,
    const std::lock_guard<std::mutex> & /* moving_parts_lock */)
{
    unsigned parts_to_move_by_policy_rules = 0;
    unsigned parts_to_move_by_ttl_rules = 0;
    double parts_to_move_total_size_bytes = 0.0;

    MergeTreeData::DataPartsVector data_parts = data->getDataPartsVector();

    if (data_parts.empty())
        return false;

    std::unordered_map<DiskPtr, LargestPartsWithRequiredSize> need_to_move;
    const auto & policy = data->getStoragePolicy();
    const auto & volumes = policy->getVolumes();

    if (volumes.size() > 0)
    {
        /// Do not check last volume
        for (size_t i = 0; i != volumes.size() - 1; ++i)
        {
            for (const auto & disk : volumes[i]->disks)
            {
                UInt64 required_maximum_available_space = disk->getTotalSpace() * policy->getMoveFactor();
                UInt64 unreserved_space = disk->getUnreservedSpace();

                if (unreserved_space < required_maximum_available_space)
                    need_to_move.emplace(disk, required_maximum_available_space - unreserved_space);
            }
        }
    }

    time_t time_of_move = time(nullptr);

    for (const auto & part : data_parts)
    {
        String reason;
        /// Don't report message to log, because logging is excessive.
        if (!can_move(part, &reason))
            continue;

        const MergeTreeData::TTLEntry * ttl_entry_ptr = part->storage.selectTTLEntryForTTLInfos(part->ttl_infos, time_of_move);
        auto to_insert = need_to_move.find(part->disk);
        ReservationPtr reservation;
        if (ttl_entry_ptr)
        {
            auto destination = ttl_entry_ptr->getDestination(policy);
            if (destination && !ttl_entry_ptr->isPartInDestination(policy, *part))
                reservation = part->storage.tryReserveSpace(part->bytes_on_disk, ttl_entry_ptr->getDestination(policy));
        }

        if (reservation) /// Found reservation by TTL rule.
        {
            parts_to_move.emplace_back(part, std::move(reservation));
            /// If table TTL rule satisfies on this part, won't apply policy rules on it.
            /// In order to not over-move, we need to "release" required space on this disk,
            /// possibly to zero.
            if (to_insert != need_to_move.end())
            {
                to_insert->second.decreaseRequiredSizeAndRemoveRedundantParts(part->bytes_on_disk);
            }
            ++parts_to_move_by_ttl_rules;
            parts_to_move_total_size_bytes += part->bytes_on_disk;
        }
        else
        {
            if (to_insert != need_to_move.end())
                to_insert->second.add(part);
        }
    }

    for (auto && move : need_to_move)
    {
        auto min_volume_index = policy->getVolumeIndexByDisk(move.first) + 1;
        for (auto && part : move.second.getAccumulatedParts())
        {
            auto reservation = policy->reserve(part->bytes_on_disk, min_volume_index);
            if (!reservation)
            {
                /// Next parts to move from this disk has greater size and same min volume index.
                /// There are no space for them.
                /// But it can be possible to move data from other disks.
                break;
            }
            parts_to_move.emplace_back(part, std::move(reservation));
            ++parts_to_move_by_policy_rules;
            parts_to_move_total_size_bytes += part->bytes_on_disk;
        }
    }

    if (!parts_to_move.empty())
    {
        LOG_TRACE(log, "Selected " << parts_to_move_by_policy_rules << " parts to move according to storage policy rules and "
            << parts_to_move_by_ttl_rules << " parts according to TTL rules, "
            << formatReadableSizeWithBinarySuffix(parts_to_move_total_size_bytes) << " total");
        return true;
    }
    else
        return false;
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

    /// It's ok, because we don't block moving parts for merges or mutations
    if (!active_part || active_part->name != cloned_part->name)
    {
        LOG_INFO(log, "Failed to swap " << cloned_part->name << ". Active part doesn't exist."
            << " Possible it was merged or mutated. Will remove copy on path '" << cloned_part->getFullPath() << "'.");
        return;
    }

    cloned_part->renameTo(active_part->name);
    /// TODO what happen if server goes down here?
    data->swapActivePart(cloned_part);

    LOG_TRACE(log, "Part " << cloned_part->name << " was moved to " << cloned_part->getFullPath());
}

}
