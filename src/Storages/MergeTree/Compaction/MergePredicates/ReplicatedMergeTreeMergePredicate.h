#pragma once

#include <Storages/MergeTree/Compaction/MergePredicates/DistributedMergePredicate.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>

namespace DB
{

class ReplicatedMergeTreeBaseMergePredicate : public DistributedMergePredicate<ActiveDataPartSet, ReplicatedMergeTreeQueue>
{
public:
    explicit ReplicatedMergeTreeBaseMergePredicate(const ReplicatedMergeTreeQueue & queue_, std::optional<PartitionIdsHint> partition_ids_hint_);

    std::expected<void, PreformattedMessage> canMergeParts(const PartProperties & left, const PartProperties & right) const override;
    std::expected<void, PreformattedMessage> canUsePartInMerges(const MergeTreeDataPartPtr & part) const;
    PartsRange getPatchesToApplyOnMerge(const PartsRange & range) const override;

protected:
    const ReplicatedMergeTreeQueue & queue;

    /// List of UUIDs for parts that have their identity "pinned".
    const PinnedPartUUIDs * pinned_part_uuids_ptr = nullptr;

    /// Quorum state taken at some later time than prev_virtual_parts.
    const String * inprogress_quorum_part_ptr = nullptr;
};

/// Lightweight version of ReplicatedMergeTreeZooKeeperMergePredicate that do not make any ZooKeeper requests,
/// but may return false-positive results. Checks only a subset of required conditions.
class ReplicatedMergeTreeLocalMergePredicate final : public ReplicatedMergeTreeBaseMergePredicate
{
public:
    explicit ReplicatedMergeTreeLocalMergePredicate(ReplicatedMergeTreeQueue & queue_);
    ~ReplicatedMergeTreeLocalMergePredicate() override = default;
};

/// Complete version of merge predicate for ReplicatedMergeTree. Fetches data from ZooKeeper.
class ReplicatedMergeTreeZooKeeperMergePredicate final : public ReplicatedMergeTreeBaseMergePredicate
{
public:
    ReplicatedMergeTreeZooKeeperMergePredicate(ReplicatedMergeTreeQueue & queue_, zkutil::ZooKeeperPtr & zookeeper, std::optional<PartitionIdsHint> partition_ids_hint_);
    ~ReplicatedMergeTreeZooKeeperMergePredicate() override = default;

    /// Returns true if part is needed for some REPLACE_RANGE entry.
    /// We should not drop part in this case, because replication queue may stuck without that part.
    bool partParticipatesInReplaceRange(const MergeTreeData::DataPartPtr & part, PreformattedMessage & out_reason) const;

    /// Return nonempty optional of desired mutation version and alter version.
    /// If we have no alter (modify/drop) mutations in mutations queue, than we return biggest possible
    /// mutation version (and -1 as alter version). In other case, we return biggest mutation version with
    /// smallest alter version. This required, because we have to execute alter mutations sequentially and
    /// don't glue them together. Alter is rare operation, so it shouldn't affect performance.
    std::optional<std::pair<Int64, int>> getExpectedMutationVersion(const MergeTreeData::DataPartPtr & part) const;

    bool isMutationFinished(const std::string & znode_name, const std::map<String, int64_t> & block_numbers,
                            std::unordered_set<String> & checked_partitions_cache) const;

    /// The version of "log" node that is used to check that no new merges have appeared.
    int32_t getVersion() const { return merges_version; }

    /// Returns true if there's a drop range covering new_drop_range_info
    bool isGoingToBeDropped(const MergeTreePartInfo & new_drop_range_info, MergeTreePartInfo * out_drop_range_info = nullptr) const;

    /// Returns virtual part covering part_name (if any) or empty string
    String getCoveringVirtualPart(const String & part_name) const;

private:
    std::shared_ptr<ActiveDataPartSet> prev_virtual_parts;
    std::shared_ptr<CommittingBlocks> committing_blocks;
    std::shared_ptr<PinnedPartUUIDs> pinned_part_uuids;
    std::shared_ptr<String> inprogress_quorum_part;

    int32_t merges_version = -1;
};

using ReplicatedMergeTreeMergePredicatePtr = std::shared_ptr<const ReplicatedMergeTreeBaseMergePredicate>;

}
