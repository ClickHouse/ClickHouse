#pragma once

#include <ext/shared_ptr_helper.hpp>

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
class StorageMergeTree : private ext::shared_ptr_helper<StorageMergeTree>, public IStorage
{
friend class ext::shared_ptr_helper<StorageMergeTree>;
friend class MergeTreeBlockOutputStream;

public:
    /** hook the table with the appropriate name, along the appropriate path (with  / at the end),
      *  (correctness of names and paths are not checked)
      *  consisting of the specified columns.
      *
      * primary_expr_ast      - expression for sorting;
      * date_column_name      - the name of the column with the date;
      * index_granularity     - fow how many rows one index value is written.
      */
    static StoragePtr create(
        const String & path_,
        const String & database_name_,
        const String & table_name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        bool attach,
        Context & context_,
        ASTPtr & primary_expr_ast_,
        const String & date_column_name_,
        const ASTPtr & sampling_expression_, /// nullptr, if sampling is not supported.
        size_t index_granularity_,
        const MergeTreeData::MergingParams & merging_params_,
        bool has_force_restore_data_flag,
        const MergeTreeSettings & settings_);

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
    bool supportsParallelReplicas() const override { return true; }

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
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

    BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override;

    /** Perform the next step in combining the parts.
      */
    bool optimize(const String & partition, bool final, bool deduplicate, const Settings & settings) override
    {
        return merge(settings.min_bytes_to_use_direct_io, true, partition, final, deduplicate);
    }

    void dropPartition(ASTPtr query, const Field & partition, bool detach, bool unreplicated, const Settings & settings) override;
    void dropColumnFromPartition(ASTPtr query, const Field & partition, const Field & column_name, const Settings & settings) override;
    void attachPartition(ASTPtr query, const Field & partition, bool unreplicated, bool part, const Settings & settings) override;
    void freezePartition(const Field & partition, const String & with_name, const Settings & settings) override;

    void drop() override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

    void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

    bool supportsIndexForIn() const override { return true; }

    bool checkTableCanBeDropped() const override;

    MergeTreeData & getData() { return data; }
    const MergeTreeData & getData() const { return data; }

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
    StopwatchWithLock time_after_previous_cleanup;

    MergeTreeData::DataParts currently_merging;
    std::mutex currently_merging_mutex;

    Logger * log;

    std::atomic<bool> shutdown_called {false};

    BackgroundProcessingPool::TaskHandle merge_task_handle;

    friend struct CurrentlyMergingPartsTagger;

    StorageMergeTree(
        const String & path_,
        const String & database_name_,
        const String & table_name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        bool attach,
        Context & context_,
        ASTPtr & primary_expr_ast_,
        const String & date_column_name_,
        const ASTPtr & sampling_expression_, /// nullptr, if sampling is not supported.
        size_t index_granularity_,
        const MergeTreeData::MergingParams & merging_params_,
        bool has_force_restore_data_flag,
        const MergeTreeSettings & settings_);

    /** Determines what parts should be merged and merges it.
      * If aggressive - when selects parts don't takes into account their ratio size and novelty (used for OPTIMIZE query).
      * Returns true if merge is finished successfully.
      */
    bool merge(size_t aio_threshold, bool aggressive, const String & partition, bool final, bool deduplicate);

    bool mergeTask();
};

}
