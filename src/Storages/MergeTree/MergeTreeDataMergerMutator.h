#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MutationCommands.h>
#include <atomic>
#include <functional>
#include <Common/ActionBlocker.h>
#include <Storages/MergeTree/TTLMergeSelector.h>


namespace DB
{

class MergeListEntry;
class MergeProgressCallback;

/// Auxiliary struct holding metainformation for the future merged or mutated part.
struct FutureMergedMutatedPart
{
    String name;
    String path;
    MergeTreeDataPartType type;
    MergeTreePartInfo part_info;
    MergeTreeData::DataPartsVector parts;

    const MergeTreePartition & getPartition() const { return parts.front()->partition; }

    FutureMergedMutatedPart() = default;

    explicit FutureMergedMutatedPart(MergeTreeData::DataPartsVector parts_)
    {
        assign(std::move(parts_));
    }

    FutureMergedMutatedPart(MergeTreeData::DataPartsVector parts_, MergeTreeDataPartType future_part_type)
    {
        assign(std::move(parts_), future_part_type);
    }

    void assign(MergeTreeData::DataPartsVector parts_);
    void assign(MergeTreeData::DataPartsVector parts_, MergeTreeDataPartType future_part_type);

    void updatePath(const MergeTreeData & storage, const ReservationPtr & reservation);
};


/** Can select parts for background processes and do them.
 * Currently helps with merges, mutations and moves
 */
class MergeTreeDataMergerMutator
{
public:
    using AllowedMergingPredicate = std::function<bool (const MergeTreeData::DataPartPtr &, const MergeTreeData::DataPartPtr &, String *)>;

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
        const StorageMetadataPtr & metadata_snapshot,
        MergeListEntry & merge_entry,
        TableLockHolder & table_lock_holder,
        time_t time_of_merge,
        const ReservationPtr & space_reservation,
        bool deduplicate,
        bool force_ttl);

    /// Mutate a single data part with the specified commands. Will create and return a temporary part.
    MergeTreeData::MutableDataPartPtr mutatePartToTemporaryPart(
        const FutureMergedMutatedPart & future_part,
        const StorageMetadataPtr & metadata_snapshot,
        const MutationCommands & commands,
        MergeListEntry & merge_entry,
        time_t time_of_mutation,
        const Context & context,
        const ReservationPtr & space_reservation,
        TableLockHolder & table_lock_holder);

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

    /** Split mutation commands into two parts:
      * First part should be executed by mutations interpreter.
      * Other is just simple drop/renames, so they can be executed without interpreter.
      */
    static void splitMutationCommands(
        MergeTreeData::DataPartPtr part,
        const MutationCommands & commands,
        MutationCommands & for_interpreter,
        MutationCommands & for_file_renames);

    /// Apply commands to source_part i.e. remove and rename some columns in
    /// source_part and return set of files, that have to be removed or renamed
    /// from filesystem and in-memory checksums. Ordered result is important,
    /// because we can apply renames that affects each other: x -> z, y -> x.
    static NameToNameVector collectFilesForRenames(MergeTreeData::DataPartPtr source_part, const MutationCommands & commands_for_removes, const String & mrk_extension);

    /// Files, that we don't need to remove and don't need to hardlink, for example columns.txt and checksums.txt.
    /// Because we will generate new versions of them after we perform mutation.
    static NameSet collectFilesToSkip(const Block & updated_header, const std::set<MergeTreeIndexPtr> & indices_to_recalc, const String & mrk_extension);

    /// Get the columns list of the resulting part in the same order as storage_columns.
    static NamesAndTypesList getColumnsForNewDataPart(
        MergeTreeData::DataPartPtr source_part,
        const Block & updated_header,
        NamesAndTypesList storage_columns,
        const MutationCommands & commands_for_removes);

    /// Get skip indices, that should exists in the resulting data part.
    static MergeTreeIndices getIndicesForNewDataPart(
        const IndicesDescription & all_indices,
        const MutationCommands & commands_for_removes);

    static bool shouldExecuteTTL(const StorageMetadataPtr & metadata_snapshot, const Names & columns, const MutationCommands & commands);

    /// Return set of indices which should be recalculated during mutation also
    /// wraps input stream into additional expression stream
    static std::set<MergeTreeIndexPtr> getIndicesToRecalculate(
        BlockInputStreamPtr & input_stream,
        const NamesAndTypesList & updated_columns,
        const StorageMetadataPtr & metadata_snapshot,
        const Context & context);

    /// Override all columns of new part using mutating_stream
    void mutateAllPartColumns(
        MergeTreeData::MutableDataPartPtr new_data_part,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeIndices & skip_indices,
        BlockInputStreamPtr mutating_stream,
        time_t time_of_mutation,
        const CompressionCodecPtr & codec,
        MergeListEntry & merge_entry,
        bool need_remove_expired_values) const;

    /// Mutate some columns of source part with mutation_stream
    void mutateSomePartColumns(
        const MergeTreeDataPartPtr & source_part,
        const StorageMetadataPtr & metadata_snapshot,
        const std::set<MergeTreeIndexPtr> & indices_to_recalc,
        const Block & mutation_header,
        MergeTreeData::MutableDataPartPtr new_data_part,
        BlockInputStreamPtr mutating_stream,
        time_t time_of_mutation,
        const CompressionCodecPtr & codec,
        MergeListEntry & merge_entry,
        bool need_remove_expired_values) const;

    /// Initialize and write to disk new part fields like checksums, columns,
    /// etc.
    static void finalizeMutatedPart(
        const MergeTreeDataPartPtr & source_part,
        MergeTreeData::MutableDataPartPtr new_data_part,
        bool need_remove_expired_values);

public :
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

    bool checkOperationIsNotCanceled(const MergeListEntry & merge_entry) const;


private:
    MergeTreeData & data;
    const size_t background_pool_size;

    Poco::Logger * log;

    /// When the last time you wrote to the log that the disk space was running out (not to write about this too often).
    time_t disk_space_warning_time = 0;

    /// Stores the next TTL merge due time for each partition (used only by TTLMergeSelector)
    TTLMergeSelector::PartitionIdToTTLs next_ttl_merge_times_by_partition;
    /// Performing TTL merges independently for each partition guarantees that
    /// there is only a limited number of TTL merges and no partition stores data, that is too stale
};


}
