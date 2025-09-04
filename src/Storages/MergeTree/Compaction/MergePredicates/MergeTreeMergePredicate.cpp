#include <Storages/MergeTree/Compaction/MergePredicates/MergeTreeMergePredicate.h>

#include <base/defines.h>

namespace DB
{

MergeTreeMergePredicate::MergeTreeMergePredicate(const StorageMergeTree & storage_, std::unique_lock<std::mutex> & merge_mutate_lock_)
    : storage(storage_)
    , merge_mutate_lock(merge_mutate_lock_)
{
}

std::expected<void, PreformattedMessage> MergeTreeMergePredicate::canMergeParts(const PartProperties & left, const PartProperties & right) const
{
    if (left.info.partition_id != right.info.partition_id)
        return std::unexpected(PreformattedMessage::create("Parts {} and {} belong to different partitions", left.name, right.name));

    if (left.projection_names != right.projection_names)
        return std::unexpected(PreformattedMessage::create(
                "Parts have different projection sets: {{}} in {} and {{}} in {}",
                fmt::join(left.projection_names, ", "), left.name, fmt::join(right.projection_names, ", "), right.name));

    {
        uint64_t left_mutation_version = storage.getCurrentMutationVersion(left.info, merge_mutate_lock);
        uint64_t right_mutation_ver = storage.getCurrentMutationVersion(right.info, merge_mutate_lock);

        if (left_mutation_version != right_mutation_ver)
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

    return {};
}

}
