#include <Storages/MergeTree/Compaction/MergePredicates/MergeTreeMergePredicate.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <base/defines.h>

namespace DB
{

static std::vector<MergeTreePartInfo> getPatchPartInfos(const StorageMergeTree & storage)
{
    auto patches_vector = storage.getPatchPartsVectorForInternalUsage();

    std::vector<MergeTreePartInfo> patch_infos;
    patch_infos.reserve(patches_vector.size());

    for (const auto & patch : patches_vector)
        patch_infos.push_back(patch->info);

    return patch_infos;
}

MergeTreeMergePredicate::MergeTreeMergePredicate(const StorageMergeTree & storage_, std::unique_lock<std::mutex> & merge_mutate_lock_)
    : storage(storage_)
    , merge_mutate_lock(merge_mutate_lock_)
    , committing_blocks(storage.getCommittingBlocks())
    , min_update_block(getMinUpdateBlockNumber(committing_blocks))
{
    auto patches_vector = getPatchPartInfos(storage);
    patches_by_partition = getPatchPartsByPartition(patches_vector, min_update_block.value_or(std::numeric_limits<Int64>::max()));
}

std::expected<void, PreformattedMessage> MergeTreeMergePredicate::canMergeParts(const PartProperties & left, const PartProperties & right) const
{
    if (left.info.getPartitionId() != right.info.getPartitionId())
        return std::unexpected(PreformattedMessage::create("Parts {} and {} belong to different partitions", left.name, right.name));

    if (left.info.isPatch() != right.info.isPatch())
        return std::unexpected(PreformattedMessage::create("One of parts ({}, {}) is patch part and another is regular part", left.name, right.name));

    if (left.projection_names != right.projection_names)
    {
        return std::unexpected(PreformattedMessage::create(
            "Parts have different projection sets: {} in '{}' and {} in '{}'",
            left.projection_names, left.name, right.projection_names, right.name));
    }

    {
        uint64_t left_mutation_version = storage.getCurrentMutationVersion(left.info.getDataVersion(), merge_mutate_lock);
        uint64_t right_mutation_version = storage.getCurrentMutationVersion(right.info.getDataVersion(), merge_mutate_lock);

        if (left_mutation_version != right_mutation_version)
            return std::unexpected(PreformattedMessage::create("Parts {} and {} have different mutation version", left.name, right.name));
    }

    {
        uint32_t max_possible_level = storage.getMaxLevelInBetween(left, right);

        if (max_possible_level > std::max(left.info.level, right.info.level))
            return std::unexpected(PreformattedMessage::create(
                    "There is an outdated part in a gap between two active parts ({}, {}) with merge level {} higher than these active parts have",
                    left.name, right.name, max_possible_level));
    }

    return {};
}

std::expected<void, PreformattedMessage> MergeTreeMergePredicate::canUsePartInMerges(const MergeTreeDataPartPtr & part) const
{
    chassert(merge_mutate_lock.owns_lock()); /// guards currently_merging_mutating_parts

    if (storage.currently_merging_mutating_parts.contains(part->info))
        return std::unexpected(PreformattedMessage::create("Part {} currently in a merging or mutating process", part->name));

    if (min_update_block && part->info.getDataVersion() >= *min_update_block)
    {
        return std::unexpected(PreformattedMessage::create(
            "Part {} has data version {}, but patch part with lower version {} is still being processed",
            part->name, part->info.getDataVersion(), *min_update_block));
    }

    return {};
}

PartsRange MergeTreeMergePredicate::getPatchesToApplyOnMerge(const PartsRange & range) const
{
    if (range.empty())
        return {};

    const auto & first_part = range.front().info;
    if (first_part.isPatch())
        return {};

    const auto & partition_id = first_part.getPartitionId();
    auto it = patches_by_partition.find(partition_id);

    if (it == patches_by_partition.end() || it->second.empty())
        return {};

    Int64 next_version = storage.getNextMutationVersion(first_part.getDataVersion(), merge_mutate_lock);
    return DB::getPatchesToApplyOnMerge(it->second, range, next_version);
}

}
