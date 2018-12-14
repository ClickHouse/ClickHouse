#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MutationCommands.h>
#include <atomic>
#include <functional>
#include <Common/ActionBlocker.h>


namespace DB
{

class MergeListEntry;
class MergeProgressCallback;


/** Can select the parts to merge and merge them.
  */
class MergeTreeDataMergerMutator
{
public:
    using AllowedMergingPredicate = std::function<bool (const MergeTreeData::DataPartPtr &, const MergeTreeData::DataPartPtr &, String * reason)>;

    struct FuturePart
    {
        String name;
        MergeTreePartInfo part_info;
        MergeTreeData::DataPartsVector parts;

        const MergeTreePartition & getPartition() const { return parts.front()->partition; }

        FuturePart() = default;
        explicit FuturePart(MergeTreeData::DataPartsVector parts_)
        {
            assign(std::move(parts_));
        }

        void assign(MergeTreeData::DataPartsVector parts_);
    };

public:
    MergeTreeDataMergerMutator(MergeTreeData & data_, const BackgroundProcessingPool & pool_);

    /** Get maximum total size of parts to do merge, at current moment of time.
      * It depends on number of free threads in background_pool and amount of free space in disk.
      */
    size_t getMaxSourcePartsSize();

    /** For explicitly passed size of pool and number of used tasks.
      * This method could be used to calculate threshold depending on number of tasks in replication queue.
      */
    size_t getMaxSourcePartsSize(size_t pool_size, size_t pool_used);

    /** Selects which parts to merge. Uses a lot of heuristics.
      *
      * can_merge - a function that determines if it is possible to merge a pair of adjacent parts.
      *  This function must coordinate merge with inserts and other merges, ensuring that
      *  - Parts between which another part can still appear can not be merged. Refer to METR-7001.
      *  - A part that already merges with something in one place, you can not start to merge into something else in another place.
      */
    bool selectPartsToMerge(
        FuturePart & future_part,
        bool aggressive,
        size_t max_total_size_to_merge,
        const AllowedMergingPredicate & can_merge,
        String * out_disable_reason = nullptr);

    /** Select all the parts in the specified partition for merge, if possible.
      * final - choose to merge even a single part - that is, allow to merge one part "with itself".
      */
    bool selectAllPartsToMergeWithinPartition(
        FuturePart & future_part,
        size_t & available_disk_space,
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
        const FuturePart & future_part,
        MergeListEntry & merge_entry, time_t time_of_merge,
        DiskSpaceMonitor::Reservation * disk_reservation, bool deduplication);

    /// Mutate a single data part with the specified commands. Will create and return a temporary part.
    MergeTreeData::MutableDataPartPtr mutatePartToTemporaryPart(
        const FuturePart & future_part,
        const std::vector<MutationCommand> & commands,
        const Context & context);

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
    ActionBlocker actions_blocker;

    enum class MergeAlgorithm
    {
        Horizontal, /// per-row merge of all columns
        Vertical    /// per-row merge of PK columns, per-column gather for non-PK columns
    };

private:

    MergeAlgorithm chooseMergeAlgorithm(
            const MergeTreeData & data, const MergeTreeData::DataPartsVector & parts,
            size_t rows_upper_bound, const NamesAndTypesList & gathering_columns, bool deduplicate) const;

private:
    MergeTreeData & data;
    const BackgroundProcessingPool & pool;

    Logger * log;

    /// When the last time you wrote to the log that the disk space was running out (not to write about this too often).
    time_t disk_space_warning_time = 0;
};


}
