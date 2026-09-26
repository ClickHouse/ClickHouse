#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Storages/MergeTree/Compaction/MergePredicates/MergeTreeMergePredicate.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <base/defines.h>

#include <algorithm>

namespace DB
{

static std::vector<MergeTreePartInfo> getPatchPartInfos(const StorageMergeTree & storage, const MergeTreeTransactionPtr & tx)
{
    auto patches_vector = storage.getPatchPartsVectorForInternalUsage();

    std::vector<MergeTreePartInfo> patch_infos;
    patch_infos.reserve(patches_vector.size());

    for (const auto & patch : patches_vector)
    {
        /// A patch part of a transaction that has not committed yet is already active. Applying it
        /// on a merge would put an update that can still roll back into the merged part, so require
        /// the same visibility that 'MergeTreePartsCollector' requires of the parts being merged.
        if (tx && !patch->version->isVisible(tx->getSnapshot(), Tx::EmptyTID))
            continue;

        patch_infos.push_back(patch->info);
    }

    return patch_infos;
}

/// The same set of parts that 'MergeTreePartsCollector::collectInitial' works with. Inside a transaction
/// it is a superset of the visible parts: it keeps the outdated parts that a rollback can bring back,
/// so that a merge is not assigned over a gap between them.
static MergeTreeDataPartsVector getPartsVisibleForMerge(const StorageMergeTree & storage, const MergeTreeTransactionPtr & tx)
{
    MergeTreeData::DataPartsKinds affordable_kinds{MergeTreeData::DataPartKind::Regular, MergeTreeData::DataPartKind::Patch};

    if (!tx)
        return storage.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Active}, affordable_kinds);

    MergeTreeDataPartsVector active_parts;
    MergeTreeDataPartsVector outdated_parts;

    {
        auto lock = storage.readLockParts();
        active_parts = storage.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Active}, affordable_kinds, lock);
        outdated_parts = storage.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Outdated}, affordable_kinds, lock);
    }

    ActiveDataPartSet active_parts_set{storage.format_version};
    for (const auto & part : active_parts)
        active_parts_set.add(part->name);

    for (const auto & part : outdated_parts)
    {
        const auto current_version_info = part->version->getInfo();
        if (current_version_info.creation_csn == Tx::RolledBackCSN || current_version_info.removal_csn != Tx::UnknownCSN)
            continue;

        active_parts_set.add(part->name);
    }

    const auto remove_predicate
        = [&](const MergeTreeDataPartPtr & part) { return active_parts_set.getContainingPart(part->info) != part->name; };
    std::erase_if(active_parts, remove_predicate);
    std::erase_if(outdated_parts, remove_predicate);

    MergeTreeDataPartsVector parts;
    std::merge(
        active_parts.begin(),
        active_parts.end(),
        outdated_parts.begin(),
        outdated_parts.end(),
        std::back_inserter(parts),
        MergeTreeData::LessDataPart());
    return parts;
}

MergeTreeMergePredicate::MergeTreeMergePredicate(
    const StorageMergeTree & storage_, const MergeTreeTransactionPtr & tx_, std::unique_lock<std::mutex> & merge_mutate_lock_)
    : storage(storage_)
    , merge_mutate_lock(merge_mutate_lock_)
    , committing_blocks(storage.getCommittingBlocks())
    , min_update_block(getMinUpdateBlockNumber(committing_blocks))
{
    /// The wider set is used only to find the data versions that a merge of patch parts must not span.
    /// A version that only a rollbackable outdated part has still has to be seen here, otherwise the
    /// merge becomes wrong as soon as that part is active again.
    auto parts_visible_for_merge = getPartsVisibleForMerge(storage, tx_);

    bool has_patches = std::ranges::any_of(parts_visible_for_merge, [](const auto & part) { return part->info.isPatch(); });

    if (has_patches)
        data_versions_by_partition = getDataVersionsByPartition(parts_visible_for_merge);

    /// The patch parts that a merge applies must be visible to that merge itself, and nothing here
    /// checks their visibility later, so they are taken from the active parts that the transaction sees.
    patches_by_partition = getPatchPartsByPartition(getPatchPartInfos(storage, tx_), min_update_block.value_or(std::numeric_limits<Int64>::max()));
}

std::expected<void, PreformattedMessage>
MergeTreeMergePredicate::canMergeParts(const PartProperties & left, const PartProperties & right) const
{
    if (left.info.getPartitionId() != right.info.getPartitionId())
        return std::unexpected(PreformattedMessage::create("Parts {} and {} belong to different partitions", left.name, right.name));

    if (left.info.isPatch() != right.info.isPatch())
        return std::unexpected(
            PreformattedMessage::create("One of parts ({}, {}) is patch part and another is regular part", left.name, right.name));

    if (left.is_in_volume_where_merges_avoid || right.is_in_volume_where_merges_avoid)
        return std::unexpected(
            PreformattedMessage::create("One of parts ({}, {}) lies on volume where merges should be avoided", left.name, right.name));

    if (left.projection_names != right.projection_names)
    {
        return std::unexpected(
            PreformattedMessage::create(
                "Parts have different projection sets: {} in '{}' and {} in '{}'",
                left.projection_names,
                left.name,
                right.projection_names,
                right.name));
    }

    {
        uint64_t left_mutation_version = storage.getCurrentMutationVersion(left.info.getDataVersion(), merge_mutate_lock);
        uint64_t right_mutation_version = storage.getCurrentMutationVersion(right.info.getDataVersion(), merge_mutate_lock);

        if (left_mutation_version != right_mutation_version)
            return std::unexpected(PreformattedMessage::create("Parts {} and {} have different mutation version", left.name, right.name));
    }

    if (left.info.isPatch())
    {
        /// The check above only sees the mutations that are still known. A mutation that is already
        /// finished leaves no entry in 'current_mutations_by_version', while the data version it gave
        /// to the parts stays. Merging patch parts across such a version produces a patch that neither
        /// wholly applies nor wholly does not apply to those parts, which is a logical error.
        auto original_partition_id = left.info.getOriginalPartitionId();

        auto spanned_version = findDataVersionInRange(
            data_versions_by_partition, original_partition_id, left.info.getDataVersion(), right.info.getDataVersion());

        if (spanned_version.has_value())
            return std::unexpected(
                PreformattedMessage::create(
                    "Merge of patch parts {} and {} would span data version {} of a part in partition {}",
                    left.name,
                    right.name,
                    *spanned_version,
                    original_partition_id));
    }

    {
        auto [max_possible_level, max_possible_mutation] = storage.getMaxLevelMutationInBetween(left, right);

        if (max_possible_level > std::max(left.info.level, right.info.level))
            return std::unexpected(
                PreformattedMessage::create(
                    "There is an outdated part in a gap between two active parts ({}, {}) with merge level {} higher than these active "
                    "parts have",
                    left.name,
                    right.name,
                    max_possible_level));

        if (max_possible_mutation > std::max(left.info.mutation, right.info.mutation))
            return std::unexpected(
                PreformattedMessage::create(
                    "There is an outdated part in a gap between two active parts ({}, {}) with mutation version {} higher than these "
                    "active parts have",
                    left.name,
                    right.name,
                    max_possible_mutation));
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
        return std::unexpected(
            PreformattedMessage::create(
                "Part {} has data version {}, but patch part with lower version {} is still being processed",
                part->name,
                part->info.getDataVersion(),
                *min_update_block));
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
