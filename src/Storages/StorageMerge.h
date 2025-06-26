#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/OptimizedRegularExpression.h>


namespace DB
{

struct QueryPlanResourceHolder;

struct RowPolicyFilter;
using RowPolicyFilterPtr = std::shared_ptr<const RowPolicyFilter>;

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
        const std::optional<String> & table_to_write_,
        bool table_to_write_auto_,
        ContextPtr context_);

    StorageMerge(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const String & comment,
        const String & source_database_name_or_regexp_,
        bool database_is_regexp_,
        const String & source_table_regexp_,
        const std::optional<String> & table_to_write_,
        bool table_to_write_auto_,
        ContextPtr context_);

    std::string getName() const override { return "Merge"; }

    bool isRemote() const override;

    /// The check is delayed to the read method. It checks the support of the tables used.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsSubcolumns() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }
    bool supportsPrewhere() const override;
    std::optional<NameSet> supportedPrewhereColumns() const override;

    bool canMoveConditionsToPrewhere() const override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    /// you need to add and remove columns in the sub-tables manually
    /// the structure of sub-tables is not checked
    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & table_lock_holder) override;

    /// Evaluate database name or regexp for StorageMerge and TableFunction merge
    static std::tuple<bool /* is_regexp */, ASTPtr> evaluateDatabaseName(const ASTPtr & node, ContextPtr context);

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override;

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;

    using DatabaseTablesIterators = std::vector<DatabaseTablesIteratorPtr>;
    DatabaseTablesIterators getDatabaseIterators(ContextPtr context) const;

    static ColumnsDescription getColumnsDescriptionFromSourceTables(
        const ContextPtr & query_context,
        const String & source_database_name_or_regexp,
        bool database_is_regexp,
        const String & source_table_regexp,
        size_t max_tables_to_look);

private:
    /// (Database, Table, Lock, TableName)
    using StorageWithLockAndName = std::tuple<String, StoragePtr, TableLockHolder, String>;
    using StorageListWithLocks = std::list<StorageWithLockAndName>;

    struct DatabaseNameOrRegexp
    {
        String source_database_name_or_regexp;
        bool database_is_regexp = false;

        std::optional<OptimizedRegularExpression> source_database_regexp;
        std::optional<OptimizedRegularExpression> source_table_regexp;
        std::optional<DBToTableSetMap> source_databases_and_tables;

        DatabaseNameOrRegexp(
            const String & source_database_name_or_regexp_,
            bool database_is_regexp_,
            std::optional<OptimizedRegularExpression> source_database_regexp_,
            std::optional<OptimizedRegularExpression> source_table_regexp_,
            std::optional<DBToTableSetMap> source_databases_and_tables_);

        DatabaseTablesIteratorPtr getDatabaseIterator(const String & database_name, ContextPtr context) const;

        DatabaseTablesIterators getDatabaseIterators(ContextPtr context) const;
    };

    DatabaseNameOrRegexp database_name_or_regexp;

    std::optional<QualifiedTableName> table_to_write;
    bool table_to_write_auto = false;

    template <typename F>
    StoragePtr traverseTablesUntil(F && predicate) const;

    template <typename F>
    void forEachTable(F && func) const;

    template <typename F>
    void forEachTableName(F && func) const;

    template <typename F>
    static StoragePtr traverseTablesUntilImpl(const ContextPtr & query_context, const IStorage * ignore_self, const DatabaseNameOrRegexp & database_name_or_regexp, F && predicate);

    /// Returns a unified column structure among multiple tables.
    static ColumnsDescription getColumnsDescriptionFromSourceTablesImpl(
        const ContextPtr & context,
        const DatabaseNameOrRegexp & database_name_or_regexp,
        size_t max_tables_to_look,
        const IStorage * ignore_self);

    ColumnSizeByName getColumnSizes() const override;

    ColumnsDescription getColumnsDescriptionFromSourceTables(const ContextPtr & context) const;

    static VirtualColumnsDescription createVirtuals();

    bool tableSupportsPrewhere() const;

    template <typename F>
    std::optional<UInt64> totalRowsOrBytes(F && func) const;

    void setTableToWrite(
        const std::optional<String> & table_to_write_,
        const String & source_database_name_or_regexp_,
        bool database_is_regexp_);

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
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block common_header_,
        size_t max_block_size,
        size_t num_streams,
        StoragePtr storage,
        QueryProcessingStage::Enum processed_stage);

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    const StorageListWithLocks & getSelectedTables();

    /// Returns `false` if requested reading cannot be performed.
    bool requestReadingInOrder(InputOrderInfoPtr order_info_);
    const InputOrderInfoPtr & getInputOrder() const { return order_info; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    QueryPlanRawPtrs getChildPlans() override;

    void addFilter(FilterDAGInfo filter);

private:
    const size_t required_max_block_size;
    const size_t requested_num_streams;
    Block common_header;

    StorageListWithLocks selected_tables;
    Names all_column_names;
    Names column_names;
    bool has_database_virtual_column;
    bool has_table_virtual_column;
    StoragePtr storage_merge;
    StorageSnapshotPtr merge_storage_snapshot;

    QueryProcessingStage::Enum common_processed_stage;

    InputOrderInfoPtr order_info;

    struct AliasData
    {
        String name;       /// "size" in  "size String Alias formatReadableSize(size_bytes)"
        DataTypePtr type;  /// String in "size String Alias formatReadableSize(size_bytes)", or something different came from query
        ASTPtr expression; /// formatReadableSize(size_bytes) in "size String Alias formatReadableSize(size_bytes)"
    };

    using Aliases = std::vector<AliasData>;

    SelectQueryInfo getModifiedQueryInfo(const ContextMutablePtr & modified_context,
        const StorageWithLockAndName & storage_with_lock_and_name,
        const StorageSnapshotPtr & storage_snapshot,
        Names required_column_names,
        Names & column_names_as_aliases,
        bool & is_smallest_column_requested,
        Aliases & aliases) const;

    /// An object of this helper class is created
    ///  when processing a Merge table data source (subordinary table)
    ///  that has row policies
    ///  to guarantee that these row policies are applied
    class RowPolicyData
    {
    public:
        RowPolicyData(RowPolicyFilterPtr, std::shared_ptr<DB::IStorage>, ContextPtr);

        /// Add to data stream columns that are needed only for row policies
        ///  SELECT x from T  if  T has row policy  y=42
        ///  required y in data pipeline
        void extendNames(Names &) const;

        /// Use storage facilities to filter data
        ///  optimization
        ///  does not guarantee accuracy, but reduces number of rows
        void addStorageFilter(SourceStepWithFilter *) const;

        /// Create explicit filter transform to exclude
        /// rows that are not conform to row level policy
        void addFilterTransform(QueryPlan &) const;

    private:
        std::string filter_column_name; // complex filter, may contain logic operations
        ActionsDAG actions_dag;
        ExpressionActionsPtr filter_actions;
        StorageMetadataPtr storage_metadata_snapshot;
    };

    using RowPolicyDataOpt = std::optional<RowPolicyData>;

    struct ChildPlan
    {
        QueryPlan plan;
        QueryProcessingStage::Enum stage;
    };

    /// Store read plan for each child table.
    /// It's needed to guarantee lifetime for child steps to be the same as for this step (mainly for EXPLAIN PIPELINE).
    std::optional<std::vector<ChildPlan>> child_plans;

    /// Store filters pushed down from query plan optimization. Filters are added on top of child plans.
    std::vector<FilterDAGInfo> pushed_down_filters;

    std::vector<ChildPlan> createChildrenPlans(SelectQueryInfo & query_info_) const;

    void filterTablesAndCreateChildrenPlans();

    ChildPlan createPlanForTable(
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        QueryProcessingStage::Enum processed_stage,
        UInt64 max_block_size,
        const StorageWithLockAndName & storage_with_lock,
        const Names & real_column_names_read_from_the_source_table,
        bool & is_smallest_column_requested,
        const RowPolicyDataOpt & row_policy_data_opt,
        ContextMutablePtr modified_context,
        size_t streams_num) const;

    void addVirtualColumns(
        ChildPlan & child,
        SelectQueryInfo & modified_query_info,
        QueryProcessingStage::Enum processed_stage,
        const StorageWithLockAndName & storage_with_lock) const;

    QueryPipelineBuilderPtr buildPipeline(
        ChildPlan & child,
        QueryProcessingStage::Enum processed_stage) const;

    static void convertAndFilterSourceStream(
        const Block & header,
        SelectQueryInfo & modified_query_info,
        const StorageSnapshotPtr & snapshot,
        const Aliases & aliases,
        const RowPolicyDataOpt & row_policy_data_opt,
        ContextPtr context,
        ChildPlan & child,
        bool is_smallest_column_requested);

    StorageMerge::StorageListWithLocks getSelectedTables(
        ContextPtr query_context,
        bool filter_by_database_virtual_column,
        bool filter_by_table_virtual_column) const;
};

}
