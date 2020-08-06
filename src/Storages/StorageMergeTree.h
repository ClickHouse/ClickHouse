#pragma once

#include <ext/shared_ptr_helper.h>

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
#include <Disks/StoragePolicy.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Common/SimpleIncrement.h>
#include <Core/BackgroundSchedulePool.h>


namespace DB
{

/** See the description of the data structure in MergeTreeData.
  */
class StorageMergeTree final : public ext::shared_ptr_helper<StorageMergeTree>, public MergeTreeData
{
    friend struct ext::shared_ptr_helper<StorageMergeTree>;
public:
    void startup() override;
    void shutdown() override;
    ~StorageMergeTree() override;

    std::string getName() const override { return merging_params.getModeName() + "MergeTree"; }

    bool supportsParallelInsert() const override { return true; }

    bool supportsIndexForIn() const override { return true; }

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    std::optional<UInt64> totalRows() const override;
    std::optional<UInt64> totalBytes() const override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    /** Perform the next step in combining the parts.
      */
    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Context & context) override;

    Pipes alterPartition(
        const ASTPtr & query,
        const StorageMetadataPtr & /* metadata_snapshot */,
        const PartitionCommands & commands,
        const Context & context) override;

    void mutate(const MutationCommands & commands, const Context & context) override;

    /// Return introspection information about currently processing or recently processed mutations.
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;

    CancellationCode killMutation(const String & mutation_id) override;

    void drop() override;
    void truncate(const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &) override;

    void alter(const AlterCommands & commands, const Context & context, TableLockHolder & table_lock_holder) override;

    void checkTableCanBeDropped() const override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    CheckResults checkData(const ASTPtr & query, const Context & context) override;

private:

    /// Mutex and condvar for synchronous mutations wait
    std::mutex mutation_wait_mutex;
    std::condition_variable mutation_wait_event;

    MergeTreeDataSelectExecutor reader;
    MergeTreeDataWriter writer;
    MergeTreeDataMergerMutator merger_mutator;

    /// For block numbers.
    SimpleIncrement increment{0};

    /// For clearOldParts, clearOldTemporaryDirectories.
    AtomicStopwatch time_after_previous_cleanup;

    /// Mutex for parts currently processing in background
    /// merging (also with TTL), mutating or moving.
    mutable std::mutex currently_processing_in_background_mutex;
    mutable std::condition_variable currently_processing_in_background_condition;

    /// Parts that currently participate in merge or mutation.
    /// This set have to be used with `currently_processing_in_background_mutex`.
    DataParts currently_merging_mutating_parts;


    std::map<String, MergeTreeMutationEntry> current_mutations_by_id;
    std::multimap<Int64, MergeTreeMutationEntry &> current_mutations_by_version;

    std::atomic<bool> shutdown_called {false};

    /// Task handler for merges, mutations and moves.
    BackgroundProcessingPool::TaskHandle merging_mutating_task_handle;
    BackgroundProcessingPool::TaskHandle moving_task_handle;

    void loadMutations();

    /** Determines what parts should be merged and merges it.
      * If aggressive - when selects parts don't takes into account their ratio size and novelty (used for OPTIMIZE query).
      * Returns true if merge is finished successfully.
      */
    bool merge(bool aggressive, const String & partition_id, bool final, bool deduplicate, String * out_disable_reason = nullptr);

    BackgroundProcessingPoolTaskResult movePartsTask();

    /// Allocate block number for new mutation, write mutation to disk
    /// and into in-memory structures. Wake up merge-mutation task.
    Int64 startMutation(const MutationCommands & commands, String & mutation_file_name);
    /// Wait until mutation with version will finish mutation for all parts
    void waitForMutation(Int64 version, const String & file_name);

    /// Try and find a single part to mutate and mutate it. If some part was successfully mutated, return true.
    bool tryMutatePart();

    BackgroundProcessingPoolTaskResult mergeMutateTask();

    Int64 getCurrentMutationVersion(
        const DataPartPtr & part,
        std::unique_lock<std::mutex> & /* currently_processing_in_background_mutex_lock */) const;

    void clearOldMutations(bool truncate = false);

    // Partition helpers
    void dropPartition(const ASTPtr & partition, bool detach, const Context & context);
    PartitionCommandsResultInfo attachPartition(const ASTPtr & partition, bool part, const Context & context);

    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & context);
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, const Context & context);
    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;
    /// Update mutation entries after part mutation execution. May reset old
    /// errors if mutation was successful. Otherwise update last_failed* fields
    /// in mutation entries.
    void updateMutationEntriesErrors(FutureMergedMutatedPart result_part, bool is_successful, const String & exception_message);

    /// Return empty optional if mutation was killed. Otherwise return partially
    /// filled mutation status with information about error (latest_fail*) and
    /// is_done. mutation_ids filled with mutations with the same errors,
    /// because we can execute several mutations at once. Order is important for
    /// better readability of exception message. If mutation was killed doesn't
    /// return any ids.
    std::optional<MergeTreeMutationStatus> getIncompleteMutationsStatus(Int64 mutation_version, std::set<String> * mutation_ids = nullptr) const;

    void startBackgroundMovesIfNeeded() override;

    friend class MergeTreeBlockOutputStream;
    friend class MergeTreeData;
    friend struct CurrentlyMergingPartsTagger;

protected:

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
        Context & context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag);

    MutationCommands getFirtsAlterMutationCommandsForPart(const DataPartPtr & part) const override;
};

}
