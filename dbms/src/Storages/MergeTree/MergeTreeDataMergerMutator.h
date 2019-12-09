#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MutationCommands.h>
#include <atomic>
#include <functional>
#include <Common/ActionBlocker.h>


namespace DB
{

class MergeListEntry;
class MergeProgressCallback;

/// Auxiliary struct holding metainformation for the future merged or mutated part.
struct FutureMergedMutatedPart
{
    String name;
    String path;
    MergeTreePartInfo part_info;
    MergeTreeData::DataPartsVector parts;

    const MergeTreePartition & getPartition() const { return parts.front()->partition; }

    FutureMergedMutatedPart() = default;
    explicit FutureMergedMutatedPart(MergeTreeData::DataPartsVector parts_)
    {
        assign(std::move(parts_));
    }

    void assign(MergeTreeData::DataPartsVector parts_);
    void updatePath(const MergeTreeData & storage, const ReservationPtr & reservation);
};


/** Can select parts for background processes and do them.
 * Currently helps with merges, mutations and moves
 */
class MergeTreeDataMergerMutator
{
public:
    using AllowedMergingPredicate = std::function<bool (const MergeTreeData::DataPartPtr &, const MergeTreeData::DataPartPtr &, String * reason)>;

public:
    MergeTreeDataMergerMutator(MergeTreeData & data_, size_t background_pool_size);

    /** Get maximum total size of parts to do merge, at current moment of time.
      * It depends on number of free threads in background_pool and amount of free space in disk.
      */
    UInt64 getMaxSourcePartsSizeForMerge();

    /** For explicitly passed size of pool and number of used tasks.
      * This method could be used to calculate threshold depending on number of tasks in replication queue.
      */
    UInt64 getMaxSourcePartsSizeForMerge(size_t pool_size, size_t pool_used);

    /** Get maximum total size of parts to do mutation, at current moment of time.
      * It depends only on amount of free space in disk.
      */
    UInt64 getMaxSourcePartSizeForMutation();

    /** Selects which parts to merge. Uses a lot of heuristics.
      *
      * can_merge - a function that determines if it is possible to merge a pair of adjacent parts.
      *  This function must coordinate merge with inserts and other merges, ensuring that
      *  - Parts between which another part can still appear can not be merged. Refer to METR-7001.
      *  - A part that already merges with something in one place, you can not start to merge into something else in another place.
      */
    bool selectPartsToMerge(
        FutureMergedMutatedPart & future_part,
        bool aggressive,
        size_t max_total_size_to_merge,
        const AllowedMergingPredicate & can_merge,
        String * out_disable_reason = nullptr);


    /** Select all the parts in the specified partition for merge, if possible.
      * final - choose to merge even a single part - that is, allow to merge one part "with itself".
      */
    bool selectAllPartsToMergeWithinPartition(
        FutureMergedMutatedPart & future_part,
        UInt64 & available_disk_space,
        const AllowedMergingPredicate & can_merge,
        const String & partition_id,
        bool final,
        String * out_disable_reason = nullptr);

    /** Merge the parts.
      * If `reservation != nullptr`, now and then reduces the size of the reserved space
      *  is approximately proportional to the amount of data already written.
      *
      * Creates and returns a temporary part.
      * To end the merge, call the function renameMergedTemporaryPart.
      *
      * time_of_merge - the time when the merge was assigned.
      * Important when using ReplicatedGraphiteMergeTree to provide the same merge on replicas.
      */
    MergeTreeData::MutableDataPartPtr mergePartsToTemporaryPart(
        const FutureMergedMutatedPart & future_part,
        MergeListEntry & merge_entry, TableStructureReadLockHolder & table_lock_holder, time_t time_of_merge,
        const ReservationPtr & disk_reservation, bool deduplication, bool force_ttl);

    /// Mutate a single data part with the specified commands. Will create and return a temporary part.
    MergeTreeData::MutableDataPartPtr mutatePartToTemporaryPart(
        const FutureMergedMutatedPart & future_part,
        const std::vector<MutationCommand> & commands,
        MergeListEntry & merge_entry, const Context & context,
        const ReservationPtr & disk_reservation,
        TableStructureReadLockHolder & table_lock_holder);

    MergeTreeData::DataPartPtr renameMergedTemporaryPart(
        MergeTreeData::MutableDataPartPtr & new_data_part,
        const MergeTreeData::DataPartsVector & parts,
        MergeTreeData::Transaction * out_transaction = nullptr);


    /// The approximate amount of disk space needed for merge or mutation. With a surplus.
    static size_t estimateNeededDiskSpace(const MergeTreeData::DataPartsVector & source_parts);

private:
    /** Select all parts belonging to the same partition.
      */
    MergeTreeData::DataPartsVector selectAllPartsFromPartition(const String & partition_id);

public:
    /** Is used to cancel all merges and mutations. On cancel() call all currently running actions will throw exception soon.
      * All new attempts to start a merge or mutation will throw an exception until all 'LockHolder' objects will be destroyed.
      */
    ActionBlocker merges_blocker;
    ActionBlocker ttl_merges_blocker;

    enum class MergeAlgorithm
    {
        Horizontal, /// per-row merge of all columns
        Vertical    /// per-row merge of PK and secondary indices columns, per-column gather for non-PK columns
    };

private:

    MergeAlgorithm chooseMergeAlgorithm(
        const MergeTreeData::DataPartsVector & parts,
        size_t rows_upper_bound, const NamesAndTypesList & gathering_columns, bool deduplicate, bool need_remove_expired_values) const;

private:
    MergeTreeData & data;
    const size_t background_pool_size;

    Logger * log;

    /// When the last time you wrote to the log that the disk space was running out (not to write about this too often).
    time_t disk_space_warning_time = 0;

    /// Last time when TTLMergeSelector has been used
    time_t last_merge_with_ttl = 0;
};


}
