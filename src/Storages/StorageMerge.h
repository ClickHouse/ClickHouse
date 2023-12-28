#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/OptimizedRegularExpression.h>


namespace DB
{

struct QueryPlanResourceHolder;

/** A table that represents the union of an arbitrary number of other tables.
  * All tables must have the same structure.
  */
class StorageMerge final : public IStorage, WithContext
{
public:
    using DBToTableSetMap = std::map<String, std::set<String>>;

    StorageMerge(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const String & comment,
        const String & source_database_name_or_regexp_,
        bool database_is_regexp_,
        const DBToTableSetMap & source_databases_and_tables_,
        ContextPtr context_);

    StorageMerge(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const String & comment,
        const String & source_database_name_or_regexp_,
        bool database_is_regexp_,
        const String & source_table_regexp_,
        ContextPtr context_);

    std::string getName() const override { return "Merge"; }

    bool isRemote() const override;

    /// The check is delayed to the read method. It checks the support of the tables used.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsSubcolumns() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    std::optional<NameSet> supportedPrewhereColumns() const override;

    bool canMoveConditionsToPrewhere() const override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    /// you need to add and remove columns in the sub-tables manually
    /// the structure of sub-tables is not checked
    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & table_lock_holder) override;

    /// Evaluate database name or regexp for StorageMerge and TableFunction merge
    static std::tuple<bool /* is_regexp */, ASTPtr> evaluateDatabaseName(const ASTPtr & node, ContextPtr context);

    bool supportsTrivialCountOptimization() const override;

    std::optional<UInt64> totalRows(const Settings & settings) const override;
    std::optional<UInt64> totalBytes(const Settings & settings) const override;

private:
    std::optional<OptimizedRegularExpression> source_database_regexp;
    std::optional<OptimizedRegularExpression> source_table_regexp;
    std::optional<DBToTableSetMap> source_databases_and_tables;

    String source_database_name_or_regexp;
    bool database_is_regexp = false;

    /// (Database, Table, Lock, TableName)
    using StorageWithLockAndName = std::tuple<String, StoragePtr, TableLockHolder, String>;
    using StorageListWithLocks = std::list<StorageWithLockAndName>;
    using DatabaseTablesIterators = std::vector<DatabaseTablesIteratorPtr>;

    StorageMerge::StorageListWithLocks getSelectedTables(
        ContextPtr query_context,
        const ASTPtr & query = nullptr,
        bool filter_by_database_virtual_column = false,
        bool filter_by_table_virtual_column = false) const;

    template <typename F>
    StoragePtr getFirstTable(F && predicate) const;

    template <typename F>
    void forEachTable(F && func) const;

    DatabaseTablesIteratorPtr getDatabaseIterator(const String & database_name, ContextPtr context) const;

    DatabaseTablesIterators getDatabaseIterators(ContextPtr context) const;

    NamesAndTypesList getVirtuals() const override;
    ColumnSizeByName getColumnSizes() const override;

    ColumnsDescription getColumnsDescriptionFromSourceTables() const;

    bool tableSupportsPrewhere() const;

    template <typename F>
    std::optional<UInt64> totalRowsOrBytes(F && func) const;

    friend class ReadFromMerge;
};

class ReadFromMerge final : public SourceStepWithFilter
{
public:
    static constexpr auto name = "ReadFromMerge";
    String getName() const override { return name; }

    using StorageWithLockAndName = std::tuple<String, StoragePtr, TableLockHolder, String>;
    using StorageListWithLocks = std::list<StorageWithLockAndName>;
    using DatabaseTablesIterators = std::vector<DatabaseTablesIteratorPtr>;

    ReadFromMerge(
        Block common_header_,
        StorageListWithLocks selected_tables_,
        Names column_names_,
        bool has_database_virtual_column_,
        bool has_table_virtual_column_,
        size_t max_block_size,
        size_t num_streams,
        StoragePtr storage,
        StorageSnapshotPtr storage_snapshot,
        const SelectQueryInfo & query_info_,
        ContextMutablePtr context_,
        QueryProcessingStage::Enum processed_stage);

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    const StorageListWithLocks & getSelectedTables() const { return selected_tables; }

    /// Returns `false` if requested reading cannot be performed.
    bool requestReadingInOrder(InputOrderInfoPtr order_info_);

    void applyFilters() override;

private:
    const size_t required_max_block_size;
    const size_t requested_num_streams;
    const Block common_header;

    StorageListWithLocks selected_tables;
    Names column_names;
    bool has_database_virtual_column;
    bool has_table_virtual_column;
    StoragePtr storage_merge;
    StorageSnapshotPtr merge_storage_snapshot;

    /// Store read plan for each child table.
    /// It's needed to guarantee lifetime for child steps to be the same as for this step (mainly for EXPLAIN PIPELINE).
    std::vector<QueryPlan> child_plans;

    SelectQueryInfo query_info;
    ContextMutablePtr context;
    QueryProcessingStage::Enum common_processed_stage;

    InputOrderInfoPtr order_info;

    struct AliasData
    {
        String name;       /// "size" in  "size String Alias formatReadableSize(size_bytes)"
        DataTypePtr type;  /// String in "size String Alias formatReadableSize(size_bytes)", or something different came from query
        ASTPtr expression; /// formatReadableSize(size_bytes) in "size String Alias formatReadableSize(size_bytes)"
    };

    using Aliases = std::vector<AliasData>;

    class RowPolicyData;
    using RowPolicyDataOpt = std::optional<RowPolicyData>;

    std::vector<Aliases> table_aliases;

    std::vector<RowPolicyDataOpt> table_row_policy_data_opts;

    void createChildPlans();

    void applyFilters(const QueryPlan & plan) const;

    QueryPlan createPlanForTable(
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        QueryProcessingStage::Enum processed_stage,
        UInt64 max_block_size,
        const StorageWithLockAndName & storage_with_lock,
        Names && real_column_names,
        const RowPolicyDataOpt & row_policy_data_opt,
        ContextMutablePtr modified_context,
        size_t streams_num);

    QueryPipelineBuilderPtr createSources(
        QueryPlan & plan,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & modified_query_info,
        QueryProcessingStage::Enum processed_stage,
        const Block & header,
        const Aliases & aliases,
        const RowPolicyDataOpt & row_policy_data_opt,
        const StorageWithLockAndName & storage_with_lock,
        ContextMutablePtr modified_context,
        bool concat_streams = false) const;

    static SelectQueryInfo getModifiedQueryInfo(const SelectQueryInfo & query_info,
        const ContextPtr & modified_context,
        const StorageWithLockAndName & storage_with_lock_and_name,
        const StorageSnapshotPtr & storage_snapshot);

    static void convertAndFilterSourceStream(
        const Block & header,
        const StorageMetadataPtr & metadata_snapshot,
        const Aliases & aliases,
        const RowPolicyDataOpt & row_policy_data_opt,
        ContextPtr context,
        QueryPipelineBuilder & builder,
        QueryProcessingStage::Enum processed_stage);
};

}
