#pragma once

#include <ext/shared_ptr_helper.h>

#include <Core/Names.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Common/SimpleIncrement.h>


namespace DB
{

/** See the description of the data structure in MergeTreeData.
  */
class StorageMergeTree : public ext::shared_ptr_helper<StorageMergeTree>, public IStorage
{
public:
    void startup() override;
    void shutdown() override;
    ~StorageMergeTree() override;

    std::string getName() const override
    {
        return data.merging_params.getModeName() + "MergeTree";
    }

    std::string getTableName() const override { return table_name; }

    bool supportsSampling() const override { return data.supportsSampling(); }
    bool supportsPrewhere() const override { return data.supportsPrewhere(); }
    bool supportsFinal() const override { return data.supportsFinal(); }
    bool supportsIndexForIn() const override { return true; }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & /* query_context */) const override
    {
        return data.mayBenefitFromIndexForIn(left_in_operand);
    }

    const ColumnsDescription & getColumns() const override { return data.getColumns(); }
    void setColumns(ColumnsDescription columns_) override { return data.setColumns(std::move(columns_)); }

    virtual const IndicesDescription & getIndicesDescription() const override { return data.getIndicesDescription(); }
    virtual void setIndicesDescription(IndicesDescription indices_) override { data.setIndicesDescription(std::move(indices_)); }

    NameAndTypePair getColumn(const String & column_name) const override { return data.getColumn(column_name); }
    bool hasColumn(const String & column_name) const override { return data.hasColumn(column_name); }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    /** Perform the next step in combining the parts.
      */
    bool optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context) override;

    void alterPartition(const ASTPtr & query, const PartitionCommands & commands, const Context & context) override;

    void mutate(const MutationCommands & commands, const Context & context) override;
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const;
    CancellationCode killMutation(const String & mutation_id) override;

    void drop() override;
    void truncate(const ASTPtr &, const Context &) override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

    void alter(
        const AlterCommands & params, const String & database_name, const String & table_name,
        const Context & context, TableStructureWriteLockHolder & table_lock_holder) override;

    void checkTableCanBeDropped() const override;

    void checkPartitionCanBeDropped(const ASTPtr & partition) override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    MergeTreeData & getData() { return data; }
    const MergeTreeData & getData() const { return data; }

    String getDataPath() const override { return full_path; }

    ASTPtr getPartitionKeyAST() const override { return data.partition_by_ast; }
    ASTPtr getSortingKeyAST() const override { return data.getSortingKeyAST(); }
    ASTPtr getPrimaryKeyAST() const override { return data.getPrimaryKeyAST(); }
    ASTPtr getSamplingKeyAST() const override { return data.getSamplingExpression(); }

    Names getColumnsRequiredForPartitionKey() const override { return data.getColumnsRequiredForPartitionKey(); }
    Names getColumnsRequiredForSortingKey() const override { return data.getColumnsRequiredForSortingKey(); }
    Names getColumnsRequiredForPrimaryKey() const override { return data.getColumnsRequiredForPrimaryKey(); }
    Names getColumnsRequiredForSampling() const override { return data.getColumnsRequiredForSampling(); }
    Names getColumnsRequiredForFinal() const override { return data.getColumnsRequiredForSortingKey(); }

private:
    String path;
    String database_name;
    String table_name;
    String full_path;

    Context global_context;
    BackgroundProcessingPool & background_pool;

    MergeTreeData data;
    MergeTreeDataSelectExecutor reader;
    MergeTreeDataWriter writer;
    MergeTreeDataMergerMutator merger_mutator;

    /// For block numbers.
    SimpleIncrement increment{0};

    /// For clearOldParts, clearOldTemporaryDirectories.
    AtomicStopwatch time_after_previous_cleanup;

    mutable std::mutex currently_merging_mutex;
    MergeTreeData::DataParts currently_merging;
    std::map<String, MergeTreeMutationEntry> current_mutations_by_id;
    std::multimap<Int64, MergeTreeMutationEntry &> current_mutations_by_version;

    Logger * log;

    std::atomic<bool> shutdown_called {false};

    BackgroundProcessingPool::TaskHandle background_task_handle;

    void loadMutations();

    /** Determines what parts should be merged and merges it.
      * If aggressive - when selects parts don't takes into account their ratio size and novelty (used for OPTIMIZE query).
      * Returns true if merge is finished successfully.
      */
    bool merge(bool aggressive, const String & partition_id, bool final, bool deduplicate,
               String * out_disable_reason = nullptr);

    /// Try and find a single part to mutate and mutate it. If some part was successfully mutated, return true.
    bool tryMutatePart();

    BackgroundProcessingPoolTaskResult backgroundTask();

    Int64 getCurrentMutationVersion(
        const MergeTreeData::DataPartPtr & part,
        std::lock_guard<std::mutex> & /* currently_merging_mutex_lock */) const;

    void clearOldMutations();

    // Partition helpers
    void dropPartition(const ASTPtr & partition, bool detach, const Context & context);
    void clearColumnInPartition(const ASTPtr & partition, const Field & column_name, const Context & context);
    void attachPartition(const ASTPtr & partition, bool part, const Context & context);
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & context);

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
        const String & path_,
        const String & database_name_,
        const String & table_name_,
        const ColumnsDescription & columns_,
        const IndicesDescription & indices_,
        bool attach,
        Context & context_,
        const String & date_column_name,
        const ASTPtr & partition_by_ast_,
        const ASTPtr & order_by_ast_,
        const ASTPtr & primary_key_ast_,
        const ASTPtr & sample_by_ast_, /// nullptr, if sampling is not supported.
        const MergeTreeData::MergingParams & merging_params_,
        const MergeTreeSettings & settings_,
        bool has_force_restore_data_flag);
};

}
