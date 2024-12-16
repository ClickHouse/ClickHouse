#pragma once

#include <memory>
#include <optional>

#include <Access/EnabledRowPolicies.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/TableLockHolder.h>
#include <QueryPipeline/Pipe.h>

#include <Columns/FilterDescription.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class InterpreterSelectWithUnionQuery;
class Context;
class QueryPlan;

struct GroupingSetsParams;
using GroupingSetsParamsList = std::vector<GroupingSetsParams>;

struct TreeRewriterResult;
using TreeRewriterResultPtr = std::shared_ptr<const TreeRewriterResult>;

struct RowPolicy;
using RowPolicyPtr = std::shared_ptr<const RowPolicy>;


/** Interprets the SELECT query. Returns the stream of blocks with the results of the query before `to_stage` stage.
  */
class InterpreterSelectQuery : public IInterpreterUnionOrSelectQuery
{
public:
    /**
     * query_ptr
     * - A query AST to interpret.
     *
     * required_result_column_names
     * - don't calculate all columns except the specified ones from the query
     *  - it is used to remove calculation (and reading) of unnecessary columns from subqueries.
     *   empty means - use all columns.
     */

    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        const SelectQueryOptions &,
        const Names & required_result_column_names_ = Names{});

    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextMutablePtr & context_,
        const SelectQueryOptions &,
        const Names & required_result_column_names_ = Names{});

    /// Read data not from the table specified in the query, but from the prepared pipe `input`.
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        Pipe input_pipe_,
        const SelectQueryOptions & = {});

    /// Read data not from the table specified in the query, but from the specified `storage_`.
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        const StoragePtr & storage_,
        const StorageMetadataPtr & metadata_snapshot_ = nullptr,
        const SelectQueryOptions & = {});

    /// Reuse existing prepared_sets for another pass of analysis. It's used for projection.
    /// TODO: Find a general way of sharing sets among different interpreters, such as subqueries.
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        const SelectQueryOptions &,
        PreparedSetsPtr prepared_sets_);

    ~InterpreterSelectQuery() override;

    /// Execute a query. Get the stream of blocks to read.
    BlockIO execute() override;

    /// Builds QueryPlan for current query.
    void buildQueryPlan(QueryPlan & query_plan) override;

    bool ignoreLimits() const override { return options.ignore_limits; }
    bool ignoreQuota() const override { return options.ignore_quota; }

    void ignoreWithTotals() override;

    ASTPtr getQuery() const { return query_ptr; }

    const SelectQueryInfo & getQueryInfo() const { return query_info; }

    SelectQueryExpressionAnalyzer * getQueryAnalyzer() const { return query_analyzer.get(); }

    const ExpressionAnalysisResult & getAnalysisResult() const { return analysis_result; }

    const Names & getRequiredColumns() const { return required_columns; }

    bool hasAggregation() const { return query_analyzer->hasAggregation(); }

    static void addEmptySourceToQueryPlan(
        QueryPlan & query_plan, const Block & source_header, const SelectQueryInfo & query_info);

    Names getRequiredColumns() { return required_columns; }

    bool supportsTransactions() const override { return true; }

    FilterDAGInfoPtr getAdditionalQueryInfo() const { return additional_filter_info; }

    RowPolicyFilterPtr getRowPolicyFilter() const;

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr context) const override;

    static SortDescription getSortDescription(const ASTSelectQuery & query, const ContextPtr & context);
    static UInt64 getLimitForSorting(const ASTSelectQuery & query, const ContextPtr & context);

    static bool isQueryWithFinal(const SelectQueryInfo & info);


    static std::pair<UInt64, UInt64> getLimitLengthAndOffset(const ASTSelectQuery & query, const ContextPtr & context);

    /// Adjust the parallel replicas settings (enabled, disabled) based on the query analysis
    bool adjustParallelReplicasAfterAnalysis();


private:
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        std::optional<Pipe> input_pipe,
        const StoragePtr & storage_,
        const SelectQueryOptions &,
        const Names & required_result_column_names = {},
        const StorageMetadataPtr & metadata_snapshot_ = nullptr,
        PreparedSetsPtr prepared_sets_ = nullptr);

    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextMutablePtr & context_,
        std::optional<Pipe> input_pipe,
        const StoragePtr & storage_,
        const SelectQueryOptions &,
        const Names & required_result_column_names = {},
        const StorageMetadataPtr & metadata_snapshot_ = nullptr,
        PreparedSetsPtr prepared_sets_ = nullptr);

    ASTSelectQuery & getSelectQuery() { return query_ptr->as<ASTSelectQuery &>(); }

    void addPrewhereAliasActions();
    void applyFiltersToPrewhereInAnalysis(ExpressionAnalysisResult & analysis) const;
    bool shouldMoveToPrewhere() const;

    Block getSampleBlockImpl();

    void executeImpl(QueryPlan & query_plan, std::optional<Pipe> prepared_pipe);

    /// Different stages of query execution.
    void executeFetchColumns(QueryProcessingStage::Enum processing_stage, QueryPlan & query_plan);
    void executeWhere(QueryPlan & query_plan, const ActionsAndProjectInputsFlagPtr & expression, bool remove_filter);
    void executeAggregation(
        QueryPlan & query_plan, const ActionsAndProjectInputsFlagPtr & expression, bool overflow_row, bool final, InputOrderInfoPtr group_by_info);
    void executeMergeAggregated(QueryPlan & query_plan, bool overflow_row, bool final, bool has_grouping_sets);
    void executeTotalsAndHaving(QueryPlan & query_plan, bool has_having, const ActionsAndProjectInputsFlagPtr & expression, bool remove_filter, bool overflow_row, bool final);
    void executeHaving(QueryPlan & query_plan, const ActionsAndProjectInputsFlagPtr & expression, bool remove_filter);
    static void executeExpression(QueryPlan & query_plan, const ActionsAndProjectInputsFlagPtr & expression, const std::string & description);
    /// FIXME should go through ActionsDAG to behave as a proper function
    void executeWindow(QueryPlan & query_plan);
    void executeOrder(QueryPlan & query_plan, InputOrderInfoPtr sorting_info);
    void executeOrderOptimized(QueryPlan & query_plan, InputOrderInfoPtr sorting_info, UInt64 limit, SortDescription & output_order_descr);
    void executeWithFill(QueryPlan & query_plan);
    void executeMergeSorted(QueryPlan & query_plan, const std::string & description);
    void executePreLimit(QueryPlan & query_plan, bool do_not_skip_offset);
    void executeLimitBy(QueryPlan & query_plan);
    void executeLimit(QueryPlan & query_plan);
    void executeOffset(QueryPlan & query_plan);
    static void executeProjection(QueryPlan & query_plan, const ActionsAndProjectInputsFlagPtr & expression);
    void executeDistinct(QueryPlan & query_plan, bool before_order, Names columns, bool pre_distinct);
    void executeExtremes(QueryPlan & query_plan);
    void executeSubqueriesInSetsAndJoins(QueryPlan & query_plan);
    bool autoFinalOnQuery(ASTSelectQuery & select_query);
    std::optional<UInt64> getTrivialCount(UInt64 allow_experimental_parallel_reading_from_replicas);
    /// Check if we can limit block size to read based on LIMIT clause
    UInt64 maxBlockSizeByLimit() const;

    enum class Modificator : uint8_t
    {
        ROLLUP = 0,
        CUBE = 1,
    };

    void executeRollupOrCube(QueryPlan & query_plan, Modificator modificator);

    /** If there is a SETTINGS section in the SELECT query, then apply settings from it.
      *
      * Section SETTINGS - settings for a specific query.
      * Normally, the settings can be passed in other ways, not inside the query.
      * But the use of this section is justified if you need to set the settings for one subquery.
      */
    void initSettings();

    TreeRewriterResultPtr syntax_analyzer_result;
    std::unique_ptr<SelectQueryExpressionAnalyzer> query_analyzer;
    SelectQueryInfo query_info;

    /// Is calculated in getSampleBlock. Is used later in readImpl.
    ExpressionAnalysisResult analysis_result;
    /// For row-level security.
    RowPolicyFilterPtr row_policy_filter;
    FilterDAGInfoPtr filter_info;

    /// For additional_filter setting.
    FilterDAGInfoPtr additional_filter_info;

    /// For "per replica" filter when multiple replicas are used
    FilterDAGInfoPtr parallel_replicas_custom_filter_info;

    QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;

    /// List of columns to read to execute the query.
    Names required_columns;
    /// Structure of query source (table, subquery, etc).
    Block source_header;

    /// Actions to calculate ALIAS if required.
    std::optional<ActionsDAG> alias_actions;

    /// The subquery interpreter, if the subquery
    std::unique_ptr<InterpreterSelectWithUnionQuery> interpreter_subquery;

    /// Table from where to read data, if not subquery.
    StoragePtr storage;
    StorageID table_id = StorageID::createEmpty(); /// Will be initialized if storage is not nullptr
    TableLockHolder table_lock;

    /// Used when we read from prepared input, not table or subquery.
    std::optional<Pipe> input_pipe;

    LoggerPtr log;
    StorageMetadataPtr metadata_snapshot;
    StorageSnapshotPtr storage_snapshot;

    /// Reuse already built sets for multiple passes of analysis, possibly across interpreters.
    PreparedSetsPtr prepared_sets;
};

}
