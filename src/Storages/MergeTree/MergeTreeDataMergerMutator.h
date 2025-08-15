#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>
#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>

#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/MutateTask.h>

#include <expected>

namespace DB
{

struct SelectMergeFailure
{
    enum class Reason : uint8_t
    {
        CANNOT_SELECT = 0,
        NOTHING_TO_MERGE = 1,
    };

    Reason reason = Reason::CANNOT_SELECT;
    PreformattedMessage explanation = {};
};

/** Can select parts for background processes and do them.
 * Currently helps with merges, mutations and moves
 */
class MergeTreeDataMergerMutator
{
    void updateTTLMergeTimes(const MergeSelectorChoices & choices, const MergeTreeSettingsPtr & settings, time_t current_time);

public:
    explicit MergeTreeDataMergerMutator(MergeTreeData & data_);

    /** Useful to quickly get a list of partitions that contain parts that we may want to merge.
      * Used to reduce ZooKeeper requests during merge predicate construction for RMT.
      */
    // UInt64 getMaxSourcePartsSizeForMerge() const;

    // /** For explicitly passed size of pool and number of used tasks.
    //   * This method could be used to calculate threshold depending on number of tasks in replication queue.
    //   */
    // UInt64 getMaxSourcePartsSizeForMerge(size_t max_count, size_t scheduled_tasks_count) const;

    // /** Get maximum total size of parts to do mutation, at current moment of time.
    //   * It depends only on amount of free space in disk.
    //   */
    // UInt64 getMaxSourcePartSizeForMutation() const;

    // struct PartitionInfo
    // {
    //     time_t min_age{std::numeric_limits<time_t>::max()};
    //     size_t num_parts = 0;
    // };
    // using PartitionsInfo = std::unordered_map<std::string, PartitionInfo>;

    // using PartitionIdsHint = PartitionIds;

    // /// The first step of selecting parts to merge: returns a list of all active/visible parts
    // MergeTreeData::DataPartsVector getDataPartsToSelectMergeFrom(const MergeTreeTransactionPtr & txn) const;

    // /// Same as above, but filters partitions according to partitions_hint
    // MergeTreeData::DataPartsVector getDataPartsToSelectMergeFrom(
    //     const MergeTreeTransactionPtr & txn,
    //     const PartitionIdsHint * partitions_hint) const;

    // struct MergeSelectingInfo
    // {
    //     time_t current_time;
    //     PartitionsInfo partitions_info;
    //     IMergeSelector::PartsRanges parts_ranges;
    //     size_t parts_selected_precondition = 0;
    // };

    // /// The second step of selecting parts to merge: splits parts list into a set of ranges according to can_merge_callback.
    // /// All parts within a range can be merged without violating some invariants.
    // MergeSelectingInfo getPossibleMergeRanges(
    //     const MergeTreeData::DataPartsVector & data_parts,
    //     const AllowedMergingPredicate & can_merge_callback,
    //     const MergeTreeTransactionPtr & txn,
    //     PreformattedMessage & out_disable_reason) const;

    // /// The third step of selecting parts to merge: takes ranges that we can merge, and selects parts that we want to merge
    // SelectPartsDecision selectPartsToMergeFromRanges(
    //     FutureMergedMutatedPartPtr future_part,
    //     bool aggressive,
    //     size_t max_total_size_to_merge,
    //     bool merge_with_ttl_allowed,
    //     const StorageMetadataPtr & metadata_snapshot,
    //     const IMergeSelector::PartsRanges & parts_ranges,
    //     const time_t & current_time,
    //     PreformattedMessage & out_disable_reason,
    //     bool dry_run = false);

    // /// Actually the most fresh partition with biggest modification_time
    // String getBestPartitionToOptimizeEntire(const PartitionsInfo & partitions_info) const;

    // /// Useful to quickly get a list of partitions that contain parts that we may want to merge
    // /// The result is limited by top_number_of_partitions_to_consider_for_merge
    PartitionIdsHint getPartitionsThatMayBeMerged(
        const PartsCollectorPtr & parts_collector,
        const MergePredicatePtr & merge_predicate,
        const MergeSelectorApplier & selector) const;

    /** Selects which parts to merge. Uses a lot of heuristics.
      *
      * can_merge - a function that determines if it is possible to merge a pair of adjacent parts.
      *  This function must coordinate merge with inserts and other merges, ensuring that
      *  - Parts between which another part can still appear can not be merged. Refer to METR-7001.
      *  - A part that already merges with something in one place, you can not start to merge into something else in another place.
      */
    std::expected<MergeSelectorChoices, SelectMergeFailure> selectPartsToMerge(
        const PartsCollectorPtr & parts_collector,
        const MergePredicatePtr & merge_predicate,
        const MergeSelectorApplier & selector,
        const std::optional<PartitionIdsHint> & partitions_hint);

    /** Select all the parts in the specified partition for merge, if possible.
      * final - choose to merge even a single part - that is, allow to merge one part "with itself",
      * but if setting optimize_skip_merged_partitions is true than single part with level > 0
      * and without expired TTL won't be merged with itself.
      */
    std::expected<MergeSelectorChoices, SelectMergeFailure> selectAllPartsToMergeWithinPartition(
        const StorageMetadataPtr & metadata_snapshot,
        const PartsCollectorPtr & parts_collector,
        const MergePredicatePtr & merge_predicate,
        const String & partition_id,
        bool final,
        bool optimize_skip_merged_partitions);

    /** Creates a task to merge parts.
      * If `reservation != nullptr`, now and then reduces the size of the reserved space
      *  is approximately proportional to the amount of data already written.
      *
      * time_of_merge - the time when the merge was assigned.
      * Important when using ReplicatedGraphiteMergeTree to provide the same merge on replicas.
      */
    MergeTaskPtr mergePartsToTemporaryPart(
        FutureMergedMutatedPartPtr future_part,
        StorageMetadataPtr metadata_snapshot,
        MergeListEntry * merge_entry,
        std::unique_ptr<MergeListElement> projection_merge_list_element,
        TableLockHolder & table_lock_holder,
        time_t time_of_merge,
        ContextPtr context,
        ReservationSharedPtr space_reservation,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        MergeTreeData::MergingParams merging_params,
        MergeTreeTransactionPtr txn,
        bool need_prefix = true,
        IMergeTreeDataPart * parent_part = nullptr,
        const String & suffix = "");

    /** Mutate a single data part with the specified commands. Will create and return a temporary part.
      */
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

    /// Stores the next TTL delete merge due time for each partition (used only by TTLDeleteMergeSelector)
    PartitionIdToTTLs next_delete_ttl_merge_times_by_partition;

    /// Stores the next TTL recompress merge due time for each partition (used only by TTLRecompressionMergeSelector)
    PartitionIdToTTLs next_recompress_ttl_merge_times_by_partition;
    /// Performing TTL merges independently for each partition guarantees that
    /// there is only a limited number of TTL merges and no partition stores data, that is too stale
};

}
