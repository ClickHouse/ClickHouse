#pragma once

#include <atomic>
#include <mutex>
#include <functional>

#include <Common/ActionBlocker.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/MutateTask.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>


namespace DB
{

class MergeProgressCallback;

enum class SelectPartsDecision : uint8_t
{
    SELECTED = 0,
    CANNOT_SELECT = 1,
    NOTHING_TO_MERGE = 2,
};

enum class ExecuteTTLType : uint8_t
{
    NONE = 0,
    NORMAL = 1,
    RECALCULATE= 2,
};

/** Can select parts for background processes and do them.
 * Currently helps with merges, mutations and moves
 */
class MergeTreeDataMergerMutator
{
public:
    using AllowedMergingPredicate = std::function<bool (const MergeTreeData::DataPartPtr &,
                                                        const MergeTreeData::DataPartPtr &,
                                                        const MergeTreeTransaction *,
                                                        PreformattedMessage &)>;

    explicit MergeTreeDataMergerMutator(MergeTreeData & data_);

    /** Get maximum total size of parts to do merge, at current moment of time.
      * It depends on number of free threads in background_pool and amount of free space in disk.
      */
    UInt64 getMaxSourcePartsSizeForMerge() const;

    /** For explicitly passed size of pool and number of used tasks.
      * This method could be used to calculate threshold depending on number of tasks in replication queue.
      */
    UInt64 getMaxSourcePartsSizeForMerge(size_t max_count, size_t scheduled_tasks_count) const;

    /** Get maximum total size of parts to do mutation, at current moment of time.
      * It depends only on amount of free space in disk.
      */
    UInt64 getMaxSourcePartSizeForMutation() const;

    struct PartitionInfo
    {
        time_t min_age{std::numeric_limits<time_t>::max()};
    };
    using PartitionsInfo = std::unordered_map<std::string, PartitionInfo>;

    using PartitionIdsHint = std::unordered_set<String>;

    /// The first step of selecting parts to merge: returns a list of all active/visible parts
    MergeTreeData::DataPartsVector getDataPartsToSelectMergeFrom(const MergeTreeTransactionPtr & txn) const;

    /// Same as above, but filters partitions according to partitions_hint
    MergeTreeData::DataPartsVector getDataPartsToSelectMergeFrom(
        const MergeTreeTransactionPtr & txn,
        const PartitionIdsHint * partitions_hint) const;

    struct MergeSelectingInfo
    {
        time_t current_time;
        PartitionsInfo partitions_info;
        IMergeSelector::PartsRanges parts_ranges;
        size_t parts_selected_precondition = 0;
    };

    /// The second step of selecting parts to merge: splits parts list into a set of ranges according to can_merge_callback.
    /// All parts within a range can be merged without violating some invariants.
    MergeSelectingInfo getPossibleMergeRanges(
        const MergeTreeData::DataPartsVector & data_parts,
        const AllowedMergingPredicate & can_merge_callback,
        const MergeTreeTransactionPtr & txn,
        PreformattedMessage & out_disable_reason) const;

    /// The third step of selecting parts to merge: takes ranges that we can merge, and selects parts that we want to merge
    SelectPartsDecision selectPartsToMergeFromRanges(
        FutureMergedMutatedPartPtr future_part,
        bool aggressive,
        size_t max_total_size_to_merge,
        bool merge_with_ttl_allowed,
        const StorageMetadataPtr & metadata_snapshot,
        const IMergeSelector::PartsRanges & parts_ranges,
        const time_t & current_time,
        PreformattedMessage & out_disable_reason,
        bool dry_run = false);

    String getBestPartitionToOptimizeEntire(const PartitionsInfo & partitions_info) const;

    /// Useful to quickly get a list of partitions that contain parts that we may want to merge
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


    /// The approximate amount of disk space needed for merge or mutation. With a surplus.
    static size_t estimateNeededDiskSpace(const MergeTreeData::DataPartsVector & source_parts, const bool & account_for_deleted = false);

private:
    /** Select all parts belonging to the same partition.
      */
    MergeTreeData::DataPartsVector selectAllPartsFromPartition(const String & partition_id);

    friend class MutateTask;
    friend class MergeTask;

public :
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
    ITTLMergeSelector::PartitionIdToTTLs next_delete_ttl_merge_times_by_partition;

    /// Stores the next TTL recompress merge due time for each partition (used only by TTLRecompressionMergeSelector)
    ITTLMergeSelector::PartitionIdToTTLs next_recompress_ttl_merge_times_by_partition;
    /// Performing TTL merges independently for each partition guarantees that
    /// there is only a limited number of TTL merges and no partition stores data, that is too stale
};


}
