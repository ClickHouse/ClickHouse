#pragma once

#include <Core/Names.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreePartsMover.h>
#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/MergeTreeDeduplicationLog.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/MergePlainMergeTreeTask.h>
#include <Storages/MergeTree/MutatePlainMergeTreeTask.h>

#include <Disks/StoragePolicy.h>
#include <Common/SimpleIncrement.h>


namespace DB
{

/** See the description of the data structure in MergeTreeData.
  */
class StorageMergeTree final : public MergeTreeData
{
public:
    /** Attach the table with the appropriate name, along the appropriate path (with / at the end),
      *  (correctness of names and paths are not checked)
      *  consisting of the specified columns.
      *
      * See MergeTreeData constructor for comments on parameters.
      */
    StorageMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag);

    void startup() override;
    void flush() override;
    void shutdown() override;

    ~StorageMergeTree() override;

    std::string getName() const override { return merging_params.getModeName() + "MergeTree"; }

    bool supportsParallelInsert() const override { return true; }

    bool supportsIndexForIn() const override { return true; }

    bool supportsTransactions() const override { return true; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    std::optional<UInt64> totalRows(const Settings &) const override;
    std::optional<UInt64> totalRowsByPartitionPredicate(const SelectQueryInfo &, ContextPtr) const override;
    std::optional<UInt64> totalBytes(const Settings &) const override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    /** Perform the next step in combining the parts.
      */
    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        ContextPtr context) override;

    void mutate(const MutationCommands & commands, ContextPtr context) override;

    /// Return introspection information about currently processing or recently processed mutations.
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;

    CancellationCode killMutation(const String & mutation_id) override;

    void drop() override;
    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    void alter(const AlterCommands & commands, ContextPtr context, AlterLockHolder & table_lock_holder) override;

    void checkTableCanBeDropped() const override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    void onActionLockRemove(StorageActionBlockType action_type) override;

    CheckResults checkData(const ASTPtr & query, ContextPtr context) override;

    RestoreTaskPtr restoreData(ContextMutablePtr context, const ASTs & partitions, const BackupPtr & backup, const String & data_path_in_backup, const StorageRestoreSettings & restore_settings, const std::shared_ptr<IRestoreCoordination> & restore_coordination) override;

    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    MergeTreeDeduplicationLog * getDeduplicationLog() { return deduplication_log.get(); }

private:

    /// Mutex and condvar for synchronous mutations wait
    std::mutex mutation_wait_mutex;
    std::condition_variable mutation_wait_event;

    MergeTreeDataSelectExecutor reader;
    MergeTreeDataWriter writer;
    MergeTreeDataMergerMutator merger_mutator;

    std::unique_ptr<MergeTreeDeduplicationLog> deduplication_log;

    /// For block numbers.
    SimpleIncrement increment;

    /// For clearOldParts
    AtomicStopwatch time_after_previous_cleanup_parts;
    /// For clearOldTemporaryDirectories.
    AtomicStopwatch time_after_previous_cleanup_temporary_directories;
    /// For clearOldBrokenDetachedParts
    AtomicStopwatch time_after_previous_cleanup_broken_detached_parts;

    /// Mutex for parts currently processing in background
    /// merging (also with TTL), mutating or moving.
    mutable std::mutex currently_processing_in_background_mutex;
    mutable std::condition_variable currently_processing_in_background_condition;

    /// Parts that currently participate in merge or mutation.
    /// This set have to be used with `currently_processing_in_background_mutex`.
    DataParts currently_merging_mutating_parts;

    std::map<UInt64, MergeTreeMutationEntry> current_mutations_by_version;

    /// We store information about mutations which are not applicable to the partition of each part.
    /// The value is a maximum version for a part which will be the same as his current version,
    /// that is, to which version it can be upgraded without any change.
    std::map<std::pair<Int64, Int64>, UInt64> updated_version_by_block_range;

    std::atomic<bool> shutdown_called {false};
    std::atomic<bool> flush_called {false};

    void loadMutations();

    /// Load and initialize deduplication logs. Even if deduplication setting
    /// equals zero creates object with deduplication window equals zero.
    void loadDeduplicationLog();

    /** Determines what parts should be merged and merges it.
      * If aggressive - when selects parts don't takes into account their ratio size and novelty (used for OPTIMIZE query).
      * Returns true if merge is finished successfully.
      */
    bool merge(
            bool aggressive,
            const String & partition_id,
            bool final, bool deduplicate,
            const Names & deduplicate_by_columns,
            const MergeTreeTransactionPtr & txn,
            String * out_disable_reason = nullptr,
            bool optimize_skip_merged_partitions = false);

    /// Make part state outdated and queue it to remove without timeout
    /// If force, then stop merges and block them until part state became outdated. Throw exception if part doesn't exists
    /// If not force, then take merges selector and check that part is not participating in background operations.
    MergeTreeDataPartPtr outdatePart(MergeTreeTransaction * txn, const String & part_name, bool force);
    ActionLock stopMergesAndWait();

    /// Allocate block number for new mutation, write mutation to disk
    /// and into in-memory structures. Wake up merge-mutation task.
    Int64 startMutation(const MutationCommands & commands, ContextPtr query_context);
    /// Wait until mutation with version will finish mutation for all parts
    void waitForMutation(Int64 version);
    void waitForMutation(const String & mutation_id) override;
    void setMutationCSN(const String & mutation_id, CSN csn) override;


    friend struct CurrentlyMergingPartsTagger;

    struct PartVersionWithName
    {
        Int64 version;
        String name;

        bool operator <(const PartVersionWithName & s) const
        {
            return version < s.version;
        }
    };

    std::shared_ptr<MergeMutateSelectedEntry> selectPartsToMerge(
        const StorageMetadataPtr & metadata_snapshot,
        bool aggressive,
        const String & partition_id,
        bool final,
        String * disable_reason,
        TableLockHolder & table_lock_holder,
        std::unique_lock<std::mutex> & lock,
        const MergeTreeTransactionPtr & txn,
        bool optimize_skip_merged_partitions = false,
        SelectPartsDecision * select_decision_out = nullptr);


    std::shared_ptr<MergeMutateSelectedEntry> selectPartsToMutate(const StorageMetadataPtr & metadata_snapshot, String * disable_reason, TableLockHolder & table_lock_holder, std::unique_lock<std::mutex> & currently_processing_in_background_mutex_lock, bool & were_some_mutations_for_some_parts_skipped);

    /// For current mutations queue, returns maximum version of mutation for a part,
    /// with respect of mutations which would not change it.
    /// Returns 0 if there is no such mutation in active status.
    UInt64 getCurrentMutationVersion(
        const DataPartPtr & part,
        std::unique_lock<std::mutex> & /* currently_processing_in_background_mutex_lock */) const;

    /// Returns maximum version of a part, with respect of mutations which would not change it.
    Int64 getUpdatedDataVersion(
        const DataPartPtr & part,
        std::unique_lock<std::mutex> & /* currently_processing_in_background_mutex_lock */) const;

    size_t clearOldMutations(bool truncate = false);

    std::vector<PartVersionWithName> getSortedPartVersionsWithNames(std::unique_lock<std::mutex> & /* currently_processing_in_background_mutex_lock */) const;

    // Partition helpers
    void dropPartNoWaitNoThrow(const String & part_name) override;
    void dropPart(const String & part_name, bool detach, ContextPtr context) override;
    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr context) override;
    void dropPartsImpl(DataPartsVector && parts_to_remove, bool detach);
    PartitionCommandsResultInfo attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, ContextPtr context) override;

    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context) override;
    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;
    /// Update mutation entries after part mutation execution. May reset old
    /// errors if mutation was successful. Otherwise update last_failed* fields
    /// in mutation entries.
    void updateMutationEntriesErrors(FutureMergedMutatedPartPtr result_part, bool is_successful, const String & exception_message);

    /// Return empty optional if mutation was killed. Otherwise return partially
    /// filled mutation status with information about error (latest_fail*) and
    /// is_done. mutation_ids filled with mutations with the same errors,
    /// because we can execute several mutations at once. Order is important for
    /// better readability of exception message. If mutation was killed doesn't
    /// return any ids.
    std::optional<MergeTreeMutationStatus> getIncompleteMutationsStatus(Int64 mutation_version, std::set<String> * mutation_ids = nullptr) const;

    void startBackgroundMovesIfNeeded() override;

    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;

    friend class MergeTreeSink;
    friend class MergeTreeData;
    friend class MergePlainMergeTreeTask;
    friend class MutatePlainMergeTreeTask;


protected:

    MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const override;
};

}
