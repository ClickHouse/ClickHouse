#include <Storages/MergeTree/MergeTreePartsMover.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <set>
#include <boost/algorithm/string/join.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
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
            UInt64 first_part_size = f->getBytesOnDisk();
            UInt64 second_part_size = s->getBytesOnDisk();
            return std::tie(first_part_size, f->name) < std::tie(second_part_size, s->name);
        }
    };

    std::set<MergeTreeData::DataPartPtr, PartsSizeOnDiskComparator> elems;
    UInt64 required_size_sum;
    UInt64 current_size_sum = 0;

public:
    explicit LargestPartsWithRequiredSize(UInt64 required_sum_size_) : required_size_sum(required_sum_size_) {}

    void add(MergeTreeData::DataPartPtr part)
    {
        if (current_size_sum < required_size_sum)
        {
            elems.emplace(part);
            current_size_sum += part->getBytesOnDisk();
            return;
        }

        /// Adding smaller element
        if (!elems.empty() && (*elems.begin())->getBytesOnDisk() >= part->getBytesOnDisk())
            return;

        elems.emplace(part);
        current_size_sum += part->getBytesOnDisk();

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
        while (!elems.empty() && (current_size_sum - (*elems.begin())->getBytesOnDisk() >= required_size_sum))
        {
            current_size_sum -= (*elems.begin())->getBytesOnDisk();
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
    const auto policy = data->getStoragePolicy();
    const auto & volumes = policy->getVolumes();

    if (!volumes.empty())
    {
        /// Do not check last volume
        for (size_t i = 0; i != volumes.size() - 1; ++i)
        {
            for (const auto & disk : volumes[i]->getDisks())
            {
                UInt64 required_maximum_available_space = disk->getTotalSpace() * policy->getMoveFactor();
                UInt64 unreserved_space = disk->getUnreservedSpace();

                if (unreserved_space < required_maximum_available_space)
                    need_to_move.emplace(disk, required_maximum_available_space - unreserved_space);
            }
        }
    }

    time_t time_of_move = time(nullptr);

    auto metadata_snapshot = data->getInMemoryMetadataPtr();

    for (const auto & part : data_parts)
    {
        String reason;
        /// Don't report message to log, because logging is excessive.
        if (!can_move(part, &reason))
            continue;

        auto ttl_entry = selectTTLDescriptionForTTLInfos(metadata_snapshot->getMoveTTLs(), part->ttl_infos.moves_ttl, time_of_move, true);

        auto to_insert = need_to_move.find(part->volume->getDisk());
        ReservationPtr reservation;
        if (ttl_entry)
        {
            auto destination = data->getDestinationForMoveTTL(*ttl_entry);
            if (destination && !data->isPartInTTLDestination(*ttl_entry, *part))
                reservation = data->tryReserveSpace(part->getBytesOnDisk(), data->getDestinationForMoveTTL(*ttl_entry));
        }

        if (reservation) /// Found reservation by TTL rule.
        {
            parts_to_move.emplace_back(part, std::move(reservation));
            /// If table TTL rule satisfies on this part, won't apply policy rules on it.
            /// In order to not over-move, we need to "release" required space on this disk,
            /// possibly to zero.
            if (to_insert != need_to_move.end())
            {
                to_insert->second.decreaseRequiredSizeAndRemoveRedundantParts(part->getBytesOnDisk());
            }
            ++parts_to_move_by_ttl_rules;
            parts_to_move_total_size_bytes += part->getBytesOnDisk();
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
            auto reservation = policy->reserve(part->getBytesOnDisk(), min_volume_index);
            if (!reservation)
            {
                /// Next parts to move from this disk has greater size and same min volume index.
                /// There are no space for them.
                /// But it can be possible to move data from other disks.
                break;
            }
            parts_to_move.emplace_back(part, std::move(reservation));
            ++parts_to_move_by_policy_rules;
            parts_to_move_total_size_bytes += part->getBytesOnDisk();
        }
    }

    if (!parts_to_move.empty())
    {
        LOG_DEBUG(log, "Selected {} parts to move according to storage policy rules and {} parts according to TTL rules, {} total", parts_to_move_by_policy_rules, parts_to_move_by_ttl_rules, ReadableSize(parts_to_move_total_size_bytes));
        return true;
    }
    else
        return false;
}

MergeTreeData::DataPartPtr MergeTreePartsMover::clonePart(const MergeTreeMoveEntry & moving_part) const
{
    if (moves_blocker.isCancelled())
        throw Exception("Cancelled moving parts.", ErrorCodes::ABORTED);

    auto settings = data->getSettings();

    auto part = moving_part.part;
    LOG_TRACE(log, "Cloning part {}", part->name);

    auto disk = moving_part.reserved_space->getDisk();
    const String directory_to_move = "moving";
    if (settings->allow_s3_zero_copy_replication)
    {
        /// Try to fetch part from S3 without copy and fallback to default copy
        /// if it's not possible
        moving_part.part->assertOnDisk();
        String path_to_clone = fs::path(data->getRelativeDataPath()) / directory_to_move / "";
        String relative_path = part->relative_path;
        if (disk->exists(path_to_clone + relative_path))
        {
            LOG_WARNING(log, "Path " + fullPath(disk, path_to_clone + relative_path) + " already exists. Will remove it and clone again.");
            disk->removeRecursive(fs::path(path_to_clone) / relative_path / "");
        }
        disk->createDirectories(path_to_clone);
        bool is_fetched = data->tryToFetchIfShared(*part, disk, fs::path(path_to_clone) / part->name);
        if (!is_fetched)
            part->volume->getDisk()->copy(fs::path(data->getRelativeDataPath()) / relative_path / "", disk, path_to_clone);
        part->volume->getDisk()->removeFileIfExists(fs::path(path_to_clone) / IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME);
    }
    else
    {
        part->makeCloneOnDisk(disk, directory_to_move);
    }

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part->name, moving_part.reserved_space->getDisk(), 0);
    MergeTreeData::MutableDataPartPtr cloned_part =
        data->createPart(part->name, single_disk_volume, fs::path(directory_to_move) / part->name);
    LOG_TRACE(log, "Part {} was cloned to {}", part->name, cloned_part->getFullPath());

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
        LOG_INFO(log, "Failed to swap {}. Active part doesn't exist. Possible it was merged or mutated. Will remove copy on path '{}'.", cloned_part->name, cloned_part->getFullPath());
        return;
    }

    /// Don't remove new directory but throw an error because it may contain part which is currently in use.
    cloned_part->renameTo(active_part->name, false);

    /// TODO what happen if server goes down here?
    data->swapActivePart(cloned_part);

    LOG_TRACE(log, "Part {} was moved to {}", cloned_part->name, cloned_part->getFullPath());
}

}
