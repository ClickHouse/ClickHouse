#include <Storages/MergeTree/Compaction/MergePredicates/DistributedMergePredicate.h>
#include <Storages/MergeTree/Compaction/MergePredicates/ReplicatedMergeTreeMergePredicate.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/StorageReplicatedMergeTree.h>

#include <mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 replicated_max_mutations_in_one_entry;
}

using MergeCore = DistributedMergePredicate<ActiveDataPartSet, ReplicatedMergeTreeQueue>;

ReplicatedMergeTreeBaseMergePredicate::ReplicatedMergeTreeBaseMergePredicate(const ReplicatedMergeTreeQueue & queue_, std::optional<PartitionIdsHint> partition_ids_hint_)
    : MergeCore(std::move(partition_ids_hint_))
    , queue(queue_)
{
}

std::expected<void, PreformattedMessage> ReplicatedMergeTreeBaseMergePredicate::canMergeParts(const PartProperties & left, const PartProperties & right) const
{
    /// FIXME: remove lock here
    std::lock_guard lock(queue.state_mutex);
    return MergeCore::canMergeParts(left, right);
}

std::expected<void, PreformattedMessage> ReplicatedMergeTreeBaseMergePredicate::canUsePartInMerges(const MergeTreeDataPartPtr & part) const
{
    if (pinned_part_uuids_ptr && pinned_part_uuids_ptr->part_uuids.contains(part->uuid))
        return std::unexpected(PreformattedMessage::create("Part {} has uuid {} which is currently pinned", part->name, part->uuid));

    if (inprogress_quorum_part_ptr && *inprogress_quorum_part_ptr == part->name)
        return std::unexpected(PreformattedMessage::create("Quorum insert for part {} is currently in progress", part->name));

    /// FIXME: remove lock here
    std::lock_guard lock(queue.state_mutex);
    return MergeCore::canUsePartInMerges(part->name, part->info);
}

PartsRange ReplicatedMergeTreeBaseMergePredicate::getPatchesToApplyOnMerge(const PartsRange & range) const
{
    std::lock_guard lock(queue.state_mutex);
    return MergeCore::getPatchesToApplyOnMerge(range);
}

ReplicatedMergeTreeLocalMergePredicate::ReplicatedMergeTreeLocalMergePredicate(ReplicatedMergeTreeQueue & queue_)
    : ReplicatedMergeTreeBaseMergePredicate(queue_, std::nullopt)
{
    /// Use only information that can be quickly accessed locally without querying ZooKeeper
    virtual_parts_ptr = &queue_.virtual_parts;
    mutations_state_ptr = &queue_;

    {
        std::lock_guard lock(queue_.state_mutex);
        patches_by_partition = getPatchPartsByPartition(virtual_parts_ptr->getPatchPartInfos(), {});
    }
}

ReplicatedMergeTreeZooKeeperMergePredicate::ReplicatedMergeTreeZooKeeperMergePredicate(
    ReplicatedMergeTreeQueue & queue_, zkutil::ZooKeeperPtr & zookeeper, std::optional<PartitionIdsHint> partition_ids_hint_)
    : ReplicatedMergeTreeBaseMergePredicate(queue_, std::move(partition_ids_hint_))
{
    /// Order of actions is important, DO NOT CHANGE.
    /// For explanation check DistributedMergePredicate.

    {
        std::lock_guard lock(queue.state_mutex);
        prev_virtual_parts = std::make_shared<ActiveDataPartSet>(queue.virtual_parts);
    }

    /// Load current quorum status.
    auto quorum_status_future = zookeeper->asyncTryGet(fs::path(queue.zookeeper_path) / "quorum" / "status");
    committing_blocks = std::make_shared<CommittingBlocks>(getCommittingBlocks(zookeeper, queue.zookeeper_path, partition_ids_hint, /*with_data=*/ false));

    std::tie(merges_version, std::ignore) = queue_.pullLogsToQueue(zookeeper, {}, ReplicatedMergeTreeQueue::MERGE_PREDICATE);

    {
        /// We avoid returning here a version to be used in a lightweight transaction.
        ///
        /// When pinned parts set is changed a log entry is added to the queue in the same transaction.
        /// The log entry serves as a synchronization point, and it also increments `merges_version`.
        ///
        /// If pinned parts are fetched after logs are pulled then we can safely say that it contains all locks up to `merges_version`.
        String s = zookeeper->get(queue.zookeeper_path + "/pinned_part_uuids");
        pinned_part_uuids = std::make_shared<PinnedPartUUIDs>();
        pinned_part_uuids->fromString(s);
    }

    Coordination::GetResponse quorum_status_response = quorum_status_future.get();
    if (quorum_status_response.error == Coordination::Error::ZOK)
    {
        ReplicatedMergeTreeQuorumEntry quorum_status;
        quorum_status.fromString(quorum_status_response.data);
        inprogress_quorum_part = std::make_shared<String>(quorum_status.part_name);
    }

    /// Pass all information about parts into MergeCore
    prev_virtual_parts_ptr = prev_virtual_parts.get();
    virtual_parts_ptr = &queue.virtual_parts;
    committing_blocks_ptr = committing_blocks.get();
    mutations_state_ptr = &queue;

    {
        std::lock_guard lock(queue.state_mutex);
        patches_by_partition = getPatchPartsByPartition(virtual_parts_ptr->getPatchPartInfos(), *committing_blocks_ptr);
    }

    /// Initialize ReplicatedMergeTree Merge preconditions
    pinned_part_uuids_ptr = pinned_part_uuids.get();
    inprogress_quorum_part_ptr = inprogress_quorum_part.get();
}

bool ReplicatedMergeTreeZooKeeperMergePredicate::partParticipatesInReplaceRange(const MergeTreeData::DataPartPtr & part, PreformattedMessage & out_reason) const
{
    /// FIXME: remove lock here
    std::lock_guard lock(queue.state_mutex);

    for (const auto & entry : queue.queue)
    {
        if (entry->type != ReplicatedMergeTreeLogEntry::REPLACE_RANGE)
            continue;

        for (const auto & part_name : entry->replace_range_entry->new_part_names)
        {
            if (part->info.isDisjoint(MergeTreePartInfo::fromPartName(part_name, queue.format_version)))
                continue;

            out_reason = PreformattedMessage::create("Part {} participates in REPLACE_RANGE {} ({})", part_name, entry->new_part_name, entry->znode_name);
            return true;
        }
    }
    return false;
}

std::optional<std::pair<Int64, int>> ReplicatedMergeTreeZooKeeperMergePredicate::getExpectedMutationVersion(const MergeTreeData::DataPartPtr & part) const
{
    /// Assigning mutations is easier than assigning merges because mutations appear in the same order as
    /// the order of their version numbers (see StorageReplicatedMergeTree::mutate).
    /// This means that if we have loaded the mutation with version number X then all mutations with
    /// the version numbers less than X are also loaded and if there is no merge or mutation assigned to
    /// the part (checked by querying queue.virtual_parts), we can confidently assign a mutation to
    /// version X for this part.

    /// We cannot mutate part if it's being inserted with quorum and it's not
    /// already reached.
    if (inprogress_quorum_part && part->name == *inprogress_quorum_part)
    {
        queue.addPartsPostponeReasons(part->name, PostponeReasons::QUORUM_NOT_REACHED);
        return {};
    }

    std::lock_guard lock(queue.state_mutex);

    if (queue.virtual_parts.getContainingPart(part->info) != part->name)
        return {};

    auto in_partition = queue.mutations_by_partition.find(part->info.getPartitionId());
    if (in_partition == queue.mutations_by_partition.end())
        return {};

    UInt64 mutations_limit = (*queue.storage.getSettings())[MergeTreeSetting::replicated_max_mutations_in_one_entry];
    UInt64 mutations_count = 0;

    Int64 current_version = queue.getCurrentMutationVersion(part->info.getPartitionId(), part->info.getDataVersion());
    Int64 max_version = in_partition->second.begin()->first;

    std::set<Int64> versions_of_patches;
    if (auto it = patches_by_partition.find(part->info.getPartitionId()); it != patches_by_partition.end())
    {
        for (const auto & patch_part : it->second)
            versions_of_patches.insert(patch_part.getDataVersion());
    }

    int alter_version = -1;
    bool barrier_found = false;

    for (auto it = in_partition->second.begin(); it != in_partition->second.end(); ++it)
    {
        const auto & [mutation_version, mutation_status] = *it;

        /// Some commands cannot stick together with other commands
        if (mutation_status->entry->commands.containBarrierCommand())
        {
            /// We already collected some mutation, we don't want to stick it with barrier
            if (max_version != mutation_version && max_version > current_version)
                break;

            /// This mutations is fresh, but it's barrier, let's execute only it
            if (mutation_version > current_version)
                barrier_found = true;
        }

        if (it != in_partition->second.begin())
        {
            auto prev_version = std::prev(it)->first;

            if (prev_version > current_version)
            {
                auto prev_pos = versions_of_patches.lower_bound(prev_version);
                auto cur_pos = versions_of_patches.upper_bound(mutation_version);

                /// If there are patch parts between two mutation versions
                /// we cannot execute mutations together because the order
                /// of applying mutations and patches may be violated.
                if (prev_pos != cur_pos)
                    break;
            }
        }

        max_version = mutation_version;
        if (current_version < max_version)
            ++mutations_count;

        if (mutation_status->entry->isAlterMutation())
        {
            /// We want to assign mutations for part which version is bigger
            /// than part current version. But it doesn't make sense to assign
            /// more fresh versions of alter-mutations if previous alter still
            /// not done because alters execute one by one in strict order.
            if (mutation_version > current_version || !mutation_status->is_done)
            {
                alter_version = mutation_status->entry->alter_version;
                break;
            }
        }

        if (mutations_limit && mutations_count == mutations_limit)
        {
            LOG_WARNING(queue.log, "Will apply only {} of {} mutations and mutate part {} to version {} (the last version is {})",
                        mutations_count, in_partition->second.size(), part->name, max_version, in_partition->second.rbegin()->first);
            break;
        }

        if (barrier_found == true)
            break;
    }

    if (current_version >= max_version)
        return {};

    LOG_TRACE(queue.log, "Will apply {} mutations and mutate part {} to version {} (the last version is {})",
              mutations_count, part->name, max_version, in_partition->second.rbegin()->first);

    return std::make_pair(max_version, alter_version);
}

bool ReplicatedMergeTreeZooKeeperMergePredicate::isMutationFinished(
    const std::string & znode_name, const std::map<String, int64_t> & block_numbers, std::unordered_set<String> & checked_partitions_cache) const
{
    /// Check committing block numbers, maybe some affected inserts
    /// still not written to disk and committed to ZK.
    for (const auto & kv : block_numbers)
    {
        const String & partition_id = kv.first;
        Int64 block_num = kv.second;

        /// Maybe we already know that there are no relevant uncommitted blocks
        if (checked_partitions_cache.contains(partition_id))
            continue;

        if (partition_ids_hint && !partition_ids_hint->contains(partition_id))
        {
            /// The partition was in the hint initially but was removed by `getCommittingBlocks`
            /// because it no longer exists in ZooKeeper (e.g. it was forgotten via `FORGET PARTITION`).
            /// If there's no block_numbers directory for this partition, there can be no committing blocks.
            /// But if the partition still has parts, it's a bug — the block_numbers node should exist.
            if (prev_virtual_parts && prev_virtual_parts->hasPartitionId(partition_id))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Committing blocks were not loaded for non-empty partition {}, it's a bug", partition_id);

            checked_partitions_cache.insert(partition_id);
            continue;
        }

        auto partition_it = committing_blocks->find(partition_id);
        if (partition_it != committing_blocks->end())
        {
            size_t blocks_count = std::distance(
                partition_it->second.begin(), partition_it->second.lower_bound(block_num));
            if (blocks_count)
            {
                LOG_TRACE(queue.log, "Mutation {} is not done yet because in partition ID {} there are still {} uncommitted blocks.", znode_name, partition_id, blocks_count);
                return false;
            }
        }

        /// There are no committing blocks less than block_num in that partition and there's no way they can appear
        /// TODO Why not to get committing blocks when pulling a mutation? We could get rid of finalization task or simplify it
        checked_partitions_cache.insert(partition_id);
    }

    std::lock_guard lock(queue.state_mutex);
    /// When we creating predicate we have updated the queue. Some committing inserts can now be committed so
    /// we check parts_to_do one more time. Also this code is async so mutation actually could be deleted from memory.
    if (auto it = queue.mutations_by_znode.find(znode_name); it != queue.mutations_by_znode.end())
    {
        if (it->second.parts_to_do.size() == 0)
            return true;

        LOG_TRACE(queue.log, "Mutation {} is not done because some parts [{}] were just committed", znode_name, fmt::join(it->second.parts_to_do.getParts(), ", "));
        return false;
    }

    LOG_TRACE(queue.log, "Mutation {} is done because it doesn't exist anymore", znode_name);
    return true;
}

bool ReplicatedMergeTreeZooKeeperMergePredicate::isGoingToBeDropped(const MergeTreePartInfo & new_drop_range_info, MergeTreePartInfo * out_drop_range_info) const
{
    return queue.isGoingToBeDropped(new_drop_range_info, out_drop_range_info);
}

String ReplicatedMergeTreeZooKeeperMergePredicate::getCoveringVirtualPart(const String & part_name) const
{
    std::lock_guard lock(queue.state_mutex);
    return queue.virtual_parts.getContainingPart(MergeTreePartInfo::fromPartName(part_name, queue.format_version));
}

}
