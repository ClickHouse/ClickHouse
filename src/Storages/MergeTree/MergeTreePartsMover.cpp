#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartsMover.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>

#include <set>
#include <boost/algorithm/string/join.hpp>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_remote_fs_zero_copy_replication;
}

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int DIRECTORY_ALREADY_EXISTS;
}

namespace FailPoints
{
    extern const char stop_moving_part_before_swap_with_active[];
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

    MergeTreeData::DataPartsVector data_parts = data->getDataPartsVectorForInternalUsage();

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
                auto total_space = disk->getTotalSpace();
                auto unreserved_space = disk->getUnreservedSpace();
                if (total_space && unreserved_space)
                {
                    UInt64 required_maximum_available_space = static_cast<UInt64>(*total_space * policy->getMoveFactor());

                    if (*unreserved_space < required_maximum_available_space && !disk->isBroken())
                        need_to_move.emplace(disk, required_maximum_available_space - *unreserved_space);
                }
            }
        }
    }

    time_t time_of_move = time(nullptr);

    auto metadata_snapshot = data->getInMemoryMetadataPtr();

    if (need_to_move.empty() && !metadata_snapshot->hasAnyMoveTTL())
        return false;

    for (const auto & part : data_parts)
    {
        String reason;
        /// Don't report message to log, because logging is excessive.
        if (!can_move(part, &reason))
            continue;

        auto ttl_entry = selectTTLDescriptionForTTLInfos(metadata_snapshot->getMoveTTLs(), part->ttl_infos.moves_ttl, time_of_move, true);

        auto to_insert = need_to_move.end();
        auto part_disk_name = part->getDataPartStorage().getDiskName();

        for (auto it = need_to_move.begin(); it != need_to_move.end(); ++it)
        {
            if (it->first->getName() == part_disk_name)
            {
                to_insert = it;
                break;
            }
        }

        ReservationPtr reservation;
        if (ttl_entry)
        {
            auto destination = data->getDestinationForMoveTTL(*ttl_entry);
            if (destination && !data->isPartInTTLDestination(*ttl_entry, *part))
                reservation = MergeTreeData::tryReserveSpace(part->getBytesOnDisk(), data->getDestinationForMoveTTL(*ttl_entry));
        }

        if (reservation) /// Found reservation by TTL rule.
        {
            parts_to_move.emplace_back(part, std::move(reservation));
            /// If table TTL rule satisfies on this part, won't apply policy rules on it.
            /// In order to not over-move, we need to "release" required space on this disk,
            /// possibly to zero.
            if (to_insert != need_to_move.end())
                to_insert->second.decreaseRequiredSizeAndRemoveRedundantParts(part->getBytesOnDisk());

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
        auto min_volume_index = policy->getVolumeIndexByDiskName(move.first->getName()) + 1;
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
    return false;
}

MergeTreePartsMover::TemporaryClonedPart MergeTreePartsMover::clonePart(const MergeTreeMoveEntry & moving_part, const ReadSettings & read_settings, const WriteSettings & write_settings) const
{
    auto cancellation_hook = [&moves_blocker_ = moves_blocker]()
    {
        if (moves_blocker_.isCancelled())
            throw Exception(ErrorCodes::ABORTED, "Cancelled moving parts.");
    };
    cancellation_hook();

    auto settings = data->getSettings();
    auto part = moving_part.part;
    auto disk = moving_part.reserved_space->getDisk();
    LOG_DEBUG(log, "Cloning part {} from '{}' to '{}'", part->name, part->getDataPartStorage().getDiskName(), disk->getName());
    TemporaryClonedPart cloned_part;
    cloned_part.temporary_directory_lock = data->getTemporaryPartDirectoryHolder(part->name);

    MutableDataPartStoragePtr cloned_part_storage;
    bool preserve_blobs = false;
    if (disk->supportZeroCopyReplication() && (*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
    {
        /// Try zero-copy replication and fallback to default copy if it's not possible
        moving_part.part->assertOnDisk();
        String path_to_clone = fs::path(data->getRelativeDataPath()) / MergeTreeData::MOVING_DIR_NAME / "";
        String relative_path = part->getDataPartStorage().getPartDirectory();
        if (disk->existsFile(path_to_clone + relative_path))
        {
            // If setting is on, we should've already cleaned moving/ dir on startup
            if (data->allowRemoveStaleMovingParts())
                throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS,
                    "Cannot clone part {} from '{}' to '{}': path '{}' already exists",
                    part->name, part->getDataPartStorage().getDiskName(), disk->getName(),
                    fullPath(disk, path_to_clone + relative_path));

            LOG_DEBUG(log, "Path {} already exists. Will remove it and clone again",
                fullPath(disk, path_to_clone + relative_path));
            disk->removeRecursive(fs::path(path_to_clone) / relative_path / "");
        }

        disk->createDirectories(path_to_clone);

        auto zero_copy_part = data->tryToFetchIfShared(*part, disk, fs::path(path_to_clone) / part->name);

        if (zero_copy_part)
        {
            /// FIXME for some reason we cannot just use this part, we have to re-create it through MergeTreeDataPartBuilder
            preserve_blobs = true;
            zero_copy_part->is_temp = false;    /// Do not remove it in dtor
            cloned_part_storage = zero_copy_part->getDataPartStoragePtr();
        }
        else
        {
            LOG_INFO(log, "Part {} was not fetched, we are the first who move it to another disk, so we will copy it", part->name);
            cloned_part_storage = part->getDataPartStorage().clonePart(
                path_to_clone, part->getDataPartStorage().getPartDirectory(), disk, read_settings, write_settings, log, cancellation_hook);
        }
    }
    else
    {
        cloned_part_storage = part->makeCloneOnDisk(disk, MergeTreeData::MOVING_DIR_NAME, read_settings, write_settings, cancellation_hook);
    }

    MergeTreeDataPartBuilder builder(*data, part->name, cloned_part_storage);
    cloned_part.part = std::move(builder).withPartFormatFromDisk().build();
    LOG_TRACE(log, "Part {} was cloned to {}", part->name, cloned_part.part->getDataPartStorage().getFullPath());

    cloned_part.part->is_temp = false;
    if (data->allowRemoveStaleMovingParts())
    {
        cloned_part.part->is_temp = true;
        /// Setting it in case connection to zookeeper is lost while moving
        /// Otherwise part might be stuck in the moving directory due to the KEEPER_EXCEPTION in part's destructor
        if (preserve_blobs)
            cloned_part.part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::PRESERVE_BLOBS;
        else
            cloned_part.part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS;
    }
    cloned_part.part->loadColumnsChecksumsIndexes(true, true);
    cloned_part.part->loadVersionMetadata();
    cloned_part.part->modification_time = cloned_part.part->getDataPartStorage().getLastModified().epochTime();
    return cloned_part;
}


void MergeTreePartsMover::swapClonedPart(TemporaryClonedPart & cloned_part) const
{
    /// Used to get some stuck parts in the moving directory by stopping moves while pause is active
    FailPointInjection::pauseFailPoint(FailPoints::stop_moving_part_before_swap_with_active);
    if (moves_blocker.isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Cancelled moving parts.");

    /// `getActiveContainingPart` and `swapActivePart` are called under the same lock
    /// to prevent part becoming inactive between calls
    auto part_lock = data->lockParts();
    auto active_part = data->getActiveContainingPart(cloned_part.part->name, part_lock);

    /// It's ok, because we don't block moving parts for merges or mutations
    if (!active_part || active_part->name != cloned_part.part->name)
    {
        LOG_INFO(log,
            "Failed to swap {}. Active part doesn't exist (containing part {}). "
            "Possible it was merged or mutated. Part on path '{}' {}",
            cloned_part.part->name,
            active_part ? active_part->name : "doesn't exist",
            cloned_part.part->getDataPartStorage().getFullPath(),
            data->allowRemoveStaleMovingParts() ? "will be removed" : "will remain intact (set <allow_remove_stale_moving_parts> in config.xml, exercise caution when using)");
        return;
    }

    cloned_part.part->is_temp = false;

    /// Don't remove new directory but throw an error because it may contain part which is currently in use.
    cloned_part.part->renameTo(active_part->name, false);

    /// TODO what happen if server goes down here?
    data->swapActivePart(cloned_part.part, part_lock);

    LOG_TRACE(log, "Part {} was moved to {}", cloned_part.part->name, cloned_part.part->getDataPartStorage().getFullPath());

    cloned_part.temporary_directory_lock = {};
}

}
