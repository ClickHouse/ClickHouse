#pragma once

#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <base/defines.h>

#include <fmt/ranges.h>

namespace DB
{

using PartitionIdsHint = std::unordered_set<String>;
using CommittingBlocks = std::unordered_map<String, std::set<Int64>>;

CommittingBlocks getCommittingBlocks(zkutil::ZooKeeperPtr & zookeeper, const std::string & zookeeper_path, std::optional<PartitionIdsHint> & partition_ids_hint);

template<typename VirtualPartsT, typename MutationsStateT>
class DistributedMergePredicate : public IMergePredicate
{
    std::expected<void, PreformattedMessage> checkCanMergePartsPreconditions(const std::string & name, const MergeTreePartInfo & info) const
    {
        if (prev_virtual_parts_ptr && prev_virtual_parts_ptr->getContainingPart(info).empty())
            return std::unexpected(PreformattedMessage::create("Part {} does not contain in snapshot of previous virtual parts", name));

        if (partition_ids_hint && !partition_ids_hint->contains(info.partition_id))
            return std::unexpected(PreformattedMessage::create("Uncommitted blocks were not loaded for partition {}", info.partition_id));

        return {};
    }

public:
    explicit DistributedMergePredicate(std::optional<PartitionIdsHint> partition_ids_hint_)
        : partition_ids_hint(std::move(partition_ids_hint_))
    {
    }

    std::expected<void, PreformattedMessage> canMergeParts(const PartProperties & left, const PartProperties & right) const override
    {
        /// A sketch of a proof of why this method actually works:
        ///
        /// The trickiest part is to ensure that no new parts will ever appear in the range of blocks between left and right.
        /// Inserted parts get their block numbers by acquiring an ephemeral lock (see EphemeralLockInZooKeeper.h).
        /// These block numbers are monotonically increasing in a partition.
        ///
        /// Because there is a window between the moment the inserted part gets its block number and
        /// the moment it is committed (appears in the replication log), we can't get the name of all parts up to the given
        /// block number just by looking at the replication log - some parts with smaller block numbers may be currently committing
        /// and will appear in the log later than the parts with bigger block numbers.
        ///
        /// We also can't take a consistent snapshot of parts that are already committed plus parts that are about to commit
        /// due to limitations of ZooKeeper transactions.
        ///
        /// So we do the following:
        /// * copy virtual_parts from queue to prev_virtual_parts
        ///   (a set of parts which corresponds to executing the replication log up to a certain point)
        /// * load committing_blocks (inserts and mutations that have already acquired a block number but haven't appeared in the log yet)
        /// * do pullLogsToQueue() again to load fresh queue.virtual_parts and mutations.
        ///
        /// Now we have an invariant: if some part is in prev_virtual_parts then:
        /// * all parts with smaller block numbers are either in committing_blocks or in queue.virtual_parts
        ///   (those that managed to commit before we loaded committing_blocks).
        /// * all mutations with smaller block numbers are either in committing_blocks or in queue.mutations_by_partition
        ///
        /// So to check that no new parts will ever appear in the range of blocks between left and right we first check that
        /// left and right are already present in prev_virtual_parts (we can't give a definite answer for parts that were committed later)
        /// and then check that there are no blocks between them in committing_blocks and no parts in queue.virtual_parts.
        ///
        /// Similarly, to check that there will be no mutation with a block number between two parts from prev_virtual_parts
        /// (only then we can merge them without mutating the left part), we first check committing_blocks
        /// and then check that these two parts have the same mutation version according to queue.mutations_by_partition.

        chassert(left.name != right.name);
        chassert(checkCanMergePartsPreconditions(left.name, left.info) && checkCanMergePartsPreconditions(right.name, right.info));

        if (left.info.partition_id != right.info.partition_id)
            return std::unexpected(PreformattedMessage::create("Parts {} and {} belong to different partitions", left.name, right.name));

        int64_t left_max_block = left.info.max_block;
        int64_t right_min_block = right.info.min_block;
        chassert(left_max_block < right_min_block);

        if (committing_blocks_ptr && left_max_block + 1 < right_min_block)
        {
            auto committing_blocks_ptrin_partition = committing_blocks_ptr->find(left.info.partition_id);
            if (committing_blocks_ptrin_partition != committing_blocks_ptr->end())
            {
                const std::set<Int64> & block_numbers = committing_blocks_ptrin_partition->second;

                auto block_it = block_numbers.upper_bound(left_max_block);
                if (block_it != block_numbers.end() && *block_it < right_min_block)
                    return std::unexpected(PreformattedMessage::create("Block number {} is still being inserted between parts {} and {}", *block_it, left.name, right.name));
            }
        }

        if (virtual_parts_ptr && left_max_block + 1 < right_min_block)
        {
            /// Fake part which will appear as merge result
            MergeTreePartInfo gap_part_info(
                left.info.partition_id, left_max_block + 1, right_min_block - 1,
                MergeTreePartInfo::MAX_LEVEL, MergeTreePartInfo::MAX_BLOCK_NUMBER);

            /// We don't select parts if any smaller part covered by our merge must exist after
            /// processing replication log up to log_pointer.
            Strings covered = virtual_parts_ptr->getPartsCoveredBy(gap_part_info);
            if (!covered.empty())
                return std::unexpected(PreformattedMessage::create(
                            "There are {} parts (from {} to {}) that are still not present or being processed by other background process on this replica between {} and {}",
                            covered.size(), covered.front(), covered.back(), left.name, right.name));
        }

        if (mutations_state_ptr)
        {
            Int64 left_mutation_version = mutations_state_ptr->getCurrentMutationVersion(
                left.info.partition_id, left.info.getDataVersion());

            Int64 right_mutation_version = mutations_state_ptr->getCurrentMutationVersion(
                left.info.partition_id, right.info.getDataVersion());

            if (left_mutation_version != right_mutation_version)
                return std::unexpected(PreformattedMessage::create(
                            "Current mutation versions of parts {} and {} differ: {} and {} respectively",
                            left.name, right.name, left_mutation_version, right_mutation_version));
        }

        if (left.projection_names != right.projection_names)
            return std::unexpected(PreformattedMessage::create(
                    "Parts have different projection sets: {{}} in '{}' and {{}} in '{}'",
                    fmt::join(left.projection_names, ", "), left.name, fmt::join(right.projection_names, ", "), right.name));

        return {};
    }

    std::expected<void, PreformattedMessage> canUsePartInMerges(const std::string & name, const MergeTreePartInfo & info) const
    {
        if (auto result = checkCanMergePartsPreconditions(name, info); !result)
            return result;

        /// We look for containing parts in queue.virtual_parts (and not in prev_virtual_parts) because queue.virtual_parts is newer
        /// and it is guaranteed that it will contain all merges assigned before this object is constructed.
        if (String containing_part = virtual_parts_ptr->getContainingPart(info); containing_part != name)
            return std::unexpected(PreformattedMessage::create("Part {} has already been assigned a merge into {}", name, containing_part));

        return {};
    }

protected:
    /// A list of partitions that can be used in the merge predicate
    std::optional<PartitionIdsHint> partition_ids_hint;

    /// A snapshot of active parts that would appear if the replica executes all log entries in its queue.
    const VirtualPartsT * prev_virtual_parts_ptr = nullptr;
    const VirtualPartsT * virtual_parts_ptr = nullptr;

    /// partition ID -> block numbers of the inserts and mutations that are about to commit
    /// (loaded at some later time than prev_virtual_parts).
    const CommittingBlocks * committing_blocks_ptr = nullptr;

    /// An object that provides current mutation version for a part
    const MutationsStateT * mutations_state_ptr = nullptr;
};

}
