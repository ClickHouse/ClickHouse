#include <Storages/MergeTree/MergeMutateSelectedEntry.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_ENOUGH_SPACE;
}

/// While exists, marks parts as 'currently_merging_mutating_parts' and reserves free space on filesystem.
CurrentlyMergingPartsTagger::CurrentlyMergingPartsTagger(
    FutureMergedMutatedPartPtr future_part_,
    size_t total_size,
    MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot,
    bool is_mutation)
    : future_part(future_part_), storage(storage_)
{
    /// Assume mutex is already locked, because this method is called from mergeTask.

    /// if we mutate part, than we should reserve space on the same disk, because mutations possible can create hardlinks
    if (is_mutation)
    {
        reserved_space = storage.tryReserveSpace(total_size, future_part->parts[0]->getDataPartStorage());
    }
    else
    {
        IMergeTreeDataPart::TTLInfos ttl_infos;
        size_t max_volume_index = 0;
        for (auto & part_ptr : future_part->parts)
        {
            ttl_infos.update(part_ptr->ttl_infos);
            auto disk_name = part_ptr->getDataPartStorage().getDiskName();
            size_t volume_index = storage.getStoragePolicy()->getVolumeIndexByDiskName(disk_name);
            max_volume_index = std::max(max_volume_index, volume_index);
        }

        reserved_space = storage.balancedReservation(
            metadata_snapshot,
            total_size,
            max_volume_index,
            future_part->name,
            future_part->part_info,
            future_part->parts,
            &tagger,
            &ttl_infos);

        if (!reserved_space)
            reserved_space
                = storage.tryReserveSpacePreferringTTLRules(metadata_snapshot, total_size, ttl_infos, time(nullptr), max_volume_index);
    }

    if (!reserved_space)
    {
        if (is_mutation)
            throw Exception("Not enough space for mutating part '" + future_part->parts[0]->name + "'", ErrorCodes::NOT_ENOUGH_SPACE);
        else
            throw Exception("Not enough space for merging parts", ErrorCodes::NOT_ENOUGH_SPACE);
    }

    future_part->updatePath(storage, reserved_space.get());

    for (const auto & part : future_part->parts)
    {
        if (storage.currently_merging_mutating_parts.contains(part))
            throw Exception("Tagging already tagged part " + part->name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
    }
    storage.currently_merging_mutating_parts.insert(future_part->parts.begin(), future_part->parts.end());
}

CurrentlyMergingPartsTagger::~CurrentlyMergingPartsTagger()
{
    std::lock_guard lock(storage.currently_processing_in_background_mutex);
    for (const auto & part : future_part->parts)
    {
        if (!storage.currently_merging_mutating_parts.contains(part))
            std::terminate();
        storage.currently_merging_mutating_parts.erase(part);
    }
    storage.currently_processing_in_background_condition.notify_all();
}

}
