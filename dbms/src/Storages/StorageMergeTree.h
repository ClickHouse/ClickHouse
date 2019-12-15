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
#include <Disks/DiskSpaceMonitor.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Common/SimpleIncrement.h>
#include <Core/BackgroundSchedulePool.h>


namespace DB
{

/** See the description of the data structure in MergeTreeData.
  */
class StorageMergeTree : public ext::shared_ptr_helper<StorageMergeTree>, public MergeTreeData
{
    friend struct ext::shared_ptr_helper<StorageMergeTree>;
public:
    void startup() override;
    void shutdown() override;
    ~StorageMergeTree() override;

    std::string getName() const override { return merging_params.getModeName() + "MergeTree"; }
    std::string getTableName() const override { return table_name; }
    std::string getDatabaseName() const override { return database_name; }

    bool supportsIndexForIn() const override { return true; }

    Pipes readWithProcessors(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool supportProcessorsPipeline() const override { return true; }

    std::optional<UInt64> totalRows() const override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    /** Perform the next step in combining the parts.
      */
    bool optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context) override;

    void alterPartition(const ASTPtr & query, const PartitionCommands & commands, const Context & context) override;

    void mutate(const MutationCommands & commands, const Context & context) override;

    /// Return introspection information about currently processing or recently processed mutations.
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;

    CancellationCode killMutation(const String & mutation_id) override;

    void drop(TableStructureWriteLockHolder &) override;
    void truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &) override;

    void alter(const AlterCommands & params, const Context & context, TableStructureWriteLockHolder & table_lock_holder) override;

    void checkTableCanBeDropped() const override;

    void checkPartitionCanBeDropped(const ASTPtr & partition) override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    CheckResults checkData(const ASTPtr & query, const Context & context) override;

private:

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

    /// Parts that currently participate in merge or mutation.
    /// This set have to be used with `currently_processing_in_background_mutex`.
    DataParts currently_merging_mutating_parts;


    std::map<String, MergeTreeMutationEntry> current_mutations_by_id;
    std::multimap<Int64, MergeTreeMutationEntry &> current_mutations_by_version;

    std::atomic<bool> shutdown_called {false};

    /// Task handler for merges, mutations and moves.
    BackgroundProcessingPool::TaskHandle merging_mutating_task_handle;
    BackgroundProcessingPool::TaskHandle moving_task_handle;

    std::vector<MergeTreeData::AlterDataPartTransactionPtr> prepareAlterTransactions(
        const ColumnsDescription & new_columns, const IndicesDescription & new_indices, const Context & context);

    void loadMutations();

    /** Determines what parts should be merged and merges it.
      * If aggressive - when selects parts don't takes into account their ratio size and novelty (used for OPTIMIZE query).
      * Returns true if merge is finished successfully.
      */
    bool merge(bool aggressive, const String & partition_id, bool final, bool deduplicate, String * out_disable_reason = nullptr);

    BackgroundProcessingPoolTaskResult movePartsTask();

    /// Try and find a single part to mutate and mutate it. If some part was successfully mutated, return true.
    bool tryMutatePart();

    BackgroundProcessingPoolTaskResult mergeMutateTask();

    Int64 getCurrentMutationVersion(
        const DataPartPtr & part,
        std::lock_guard<std::mutex> & /* currently_processing_in_background_mutex_lock */) const;

    void clearOldMutations(bool truncate = false);

    // Partition helpers
    void dropPartition(const ASTPtr & partition, bool detach, const Context & context);
    void clearColumnOrIndexInPartition(const ASTPtr & partition, const AlterCommand & alter_command, const Context & context);
    void attachPartition(const ASTPtr & partition, bool part, const Context & context);
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & context);
    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;


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
        const String & database_name_,
        const String & table_name_,
        const ColumnsDescription & columns_,
        const IndicesDescription & indices_,
        const ConstraintsDescription & constraints_,
        bool attach,
        Context & context_,
        const String & date_column_name,
        const ASTPtr & partition_by_ast_,
        const ASTPtr & order_by_ast_,
        const ASTPtr & primary_key_ast_,
        const ASTPtr & sample_by_ast_, /// nullptr, if sampling is not supported.
        const ASTPtr & ttl_table_ast_,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag);
};

}
