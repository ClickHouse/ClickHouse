#pragma once

#include <ext/shared_ptr_helper.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeDataMerger.h>
#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Common/SimpleIncrement.h>


namespace DB
{

/** See the description of the data structure in MergeTreeData.
  */
class StorageMergeTree : public ext::shared_ptr_helper<StorageMergeTree>, public IStorage
{
friend class MergeTreeBlockOutputStream;

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
    bool supportsFinal() const override { return data.supportsFinal(); }
    bool supportsPrewhere() const override { return data.supportsPrewhere(); }

    const NamesAndTypesList & getColumnsListImpl() const override { return data.getColumnsListNonMaterialized(); }

    NameAndTypePair getColumn(const String & column_name) const override
    {
        return data.getColumn(column_name);
    }

    bool hasColumn(const String & column_name) const override
    {
        return data.hasColumn(column_name);
    }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    /** Perform the next step in combining the parts.
      */
    bool optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context) override;

    void dropPartition(const ASTPtr & query, const ASTPtr & partition, bool detach, const Context & context) override;
    void clearColumnInPartition(const ASTPtr & partition, const Field & column_name, const Context & context) override;
    void attachPartition(const ASTPtr & partition, bool part, const Context & context) override;
    void freezePartition(const ASTPtr & partition, const String & with_name, const Context & context) override;

    void drop() override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

    void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

    bool supportsIndexForIn() const override { return true; }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand) const override { return data.mayBenefitFromIndexForIn(left_in_operand); }

    bool checkTableCanBeDropped() const override;

    MergeTreeData & getData() { return data; }
    const MergeTreeData & getData() const { return data; }

    String getDataPath() const override { return full_path; }

private:
    String path;
    String database_name;
    String table_name;
    String full_path;

    Context & context;
    BackgroundProcessingPool & background_pool;

    MergeTreeData data;
    MergeTreeDataSelectExecutor reader;
    MergeTreeDataWriter writer;
    MergeTreeDataMerger merger;

    /// For block numbers.
    SimpleIncrement increment{0};

    /// For clearOldParts, clearOldTemporaryDirectories.
    AtomicStopwatch time_after_previous_cleanup;

    MergeTreeData::DataParts currently_merging;
    std::mutex currently_merging_mutex;

    Logger * log;

    std::atomic<bool> shutdown_called {false};

    BackgroundProcessingPool::TaskHandle merge_task_handle;

    friend struct CurrentlyMergingPartsTagger;

    /** Determines what parts should be merged and merges it.
      * If aggressive - when selects parts don't takes into account their ratio size and novelty (used for OPTIMIZE query).
      * Returns true if merge is finished successfully.
      */
    bool merge(size_t aio_threshold, bool aggressive, const String & partition_id, bool final, bool deduplicate,
               String * out_disable_reason = nullptr);

    bool mergeTask();

protected:
    /** Attach the table with the appropriate name, along the appropriate path (with  / at the end),
      *  (correctness of names and paths are not checked)
      *  consisting of the specified columns.
      *
      * primary_expr_ast      - expression for sorting;
      * date_column_name      - if not empty, the name of the column with the date used for partitioning by month;
          otherwise, partition_expr_ast is used as the partitioning expression;
      */
    StorageMergeTree(
        const String & path_,
        const String & database_name_,
        const String & table_name_,
        const NamesAndTypesList & columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        bool attach,
        Context & context_,
        const ASTPtr & primary_expr_ast_,
        const ASTPtr & secondary_sorting_expr_list_,
        const String & date_column_name,
        const ASTPtr & partition_expr_ast_,
        const ASTPtr & sampling_expression_, /// nullptr, if sampling is not supported.
        const MergeTreeData::MergingParams & merging_params_,
        const MergeTreeSettings & settings_,
        bool has_force_restore_data_flag);
};

}
