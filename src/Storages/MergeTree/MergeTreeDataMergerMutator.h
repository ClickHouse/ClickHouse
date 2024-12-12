#pragma once

#include <functional>

#include <Common/ActionBlocker.h>

#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/MutateTask.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>

namespace DB
{

enum class SelectPartsDecision : uint8_t
{
    SELECTED = 0,
    CANNOT_SELECT = 1,
    NOTHING_TO_MERGE = 2,
};

/** Can select parts for background processes and do them.
 * Currently helps with merges, mutations and moves
 */
class MergeTreeDataMergerMutator
{
public:
    using AllowedMergingPredicate = std::function<bool (const MergeTreeData::DataPartPtr &,
                                                        const MergeTreeData::DataPartPtr &,
                                                        PreformattedMessage &)>;

    explicit MergeTreeDataMergerMutator(MergeTreeData & data_);

    /// Useful to quickly get a list of partitions that contain parts that we may want to merge
    /// The result is limited by top_number_of_partitions_to_consider_for_merge
    PartitionIdsHint getPartitionsThatMayBeMerged(
        size_t max_total_size_to_merge,
        const AllowedMergingPredicate & can_merge_callback,
        bool merge_with_ttl_allowed,
        const MergeTreeTransactionPtr & txn) const;

    /** Selects which parts to merge. Uses a lot of heuristics.
      *
      * can_merge - a function that determines if it is possible to merge a pair of adjacent parts.
      *  This function must coordinate merge with inserts and other merges, ensuring that
      *  - Parts between which another part can still appear can not be merged. Refer to METR-7001.
      *  - A part that already merges with something in one place, you can not start to merge into something else in another place.
      */
    SelectPartsDecision selectPartsToMerge(
        FutureMergedMutatedPartPtr future_part,
        bool aggressive,
        size_t max_total_size_to_merge,
        const AllowedMergingPredicate & can_merge,
        bool merge_with_ttl_allowed,
        const MergeTreeTransactionPtr & txn,
        PreformattedMessage & out_disable_reason,
        const PartitionIdsHint * partitions_hint = nullptr);

    /** Select all the parts in the specified partition for merge, if possible.
      * final - choose to merge even a single part - that is, allow to merge one part "with itself",
      * but if setting optimize_skip_merged_partitions is true than single part with level > 0
      * and without expired TTL won't be merged with itself.
      */
    SelectPartsDecision selectAllPartsToMergeWithinPartition(
        FutureMergedMutatedPartPtr future_part,
        const AllowedMergingPredicate & can_merge,
        const String & partition_id,
        bool final,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeTransactionPtr & txn,
        PreformattedMessage & out_disable_reason,
        bool optimize_skip_merged_partitions = false);

    /** Creates a task to merge parts.
      * If `reservation != nullptr`, now and then reduces the size of the reserved space
      *  is approximately proportional to the amount of data already written.
      *
      * time_of_merge - the time when the merge was assigned.
      * Important when using ReplicatedGraphiteMergeTree to provide the same merge on replicas.
      */
    MergeTaskPtr mergePartsToTemporaryPart(
        FutureMergedMutatedPartPtr future_part,
        const StorageMetadataPtr & metadata_snapshot,
        MergeListEntry * merge_entry,
        std::unique_ptr<MergeListElement> projection_merge_list_element,
        TableLockHolder & table_lock_holder,
        time_t time_of_merge,
        ContextPtr context,
        ReservationSharedPtr space_reservation,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        const MergeTreeData::MergingParams & merging_params,
        const MergeTreeTransactionPtr & txn,
        bool need_prefix = true,
        IMergeTreeDataPart * parent_part = nullptr,
        const String & suffix = "");

    /// Mutate a single data part with the specified commands. Will create and return a temporary part.
    MutateTaskPtr mutatePartToTemporaryPart(
        FutureMergedMutatedPartPtr future_part,
        StorageMetadataPtr metadata_snapshot,
        MutationCommandsConstPtr commands,
        MergeListEntry * merge_entry,
        time_t time_of_mutation,
        ContextPtr context,
        const MergeTreeTransactionPtr & txn,
        ReservationSharedPtr space_reservation,
        TableLockHolder & table_lock_holder,
        bool need_prefix = true);

    MergeTreeData::DataPartPtr renameMergedTemporaryPart(
        MergeTreeData::MutableDataPartPtr & new_data_part,
        const MergeTreeData::DataPartsVector & parts,
        const MergeTreeTransactionPtr & txn,
        MergeTreeData::Transaction & out_transaction);

    /** Is used to cancel all merges and mutations. On cancel() call all currently running actions will throw exception soon.
      * All new attempts to start a merge or mutation will throw an exception until all 'LockHolder' objects will be destroyed.
      */
    PartitionActionBlocker merges_blocker;
    ActionBlocker ttl_merges_blocker;

private:
    MergeTreeData & data;

    LoggerPtr log;

    /// When the last time you wrote to the log that the disk space was running out (not to write about this too often).
    time_t disk_space_warning_time = 0;

    /// Stores the next TTL delete merge due time for each partition (used only by TTLDeleteMergeSelector)
    PartitionIdToTTLs next_delete_ttl_merge_times_by_partition;

    /// Stores the next TTL recompress merge due time for each partition (used only by TTLRecompressionMergeSelector)
    PartitionIdToTTLs next_recompress_ttl_merge_times_by_partition;
    /// Performing TTL merges independently for each partition guarantees that
    /// there is only a limited number of TTL merges and no partition stores data, that is too stale
};


}
