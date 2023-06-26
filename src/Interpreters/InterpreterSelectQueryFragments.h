#pragma once

#include <memory>

#include <Access/EnabledRowPolicies.h>
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

class SubqueryForSet;
class InterpreterSelectWithUnionQuery;
class Context;
class QueryPlan;
using Node = QueryPlan::Node;

struct GroupingSetsParams;
using GroupingSetsParamsList = std::vector<GroupingSetsParams>;

struct TreeRewriterResult;
using TreeRewriterResultPtr = std::shared_ptr<const TreeRewriterResult>;

struct RowPolicy;
using RowPolicyPtr = std::shared_ptr<const RowPolicy>;

class AggregatingStep;
class MergingAggregatedStep;

struct DataPartition;


/** Interprets the SELECT query. Returns the stream of blocks with the results of the query before `to_stage` stage.
  */
class InterpreterSelectQueryFragments : public IInterpreterUnionOrSelectQuery
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

    InterpreterSelectQueryFragments(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        const SelectQueryOptions &,
        const Names & required_result_column_names_ = Names{});

    InterpreterSelectQueryFragments(
        const ASTPtr & query_ptr_,
        const ContextMutablePtr & context_,
        const SelectQueryOptions &,
        const Names & required_result_column_names_ = Names{});

    /// Read data not from the table specified in the query, but from the prepared pipe `input`.
    InterpreterSelectQueryFragments(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        Pipe input_pipe_,
        const SelectQueryOptions & = {});

    /// Read data not from the table specified in the query, but from the specified `storage_`.
    InterpreterSelectQueryFragments(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        const StoragePtr & storage_,
        const StorageMetadataPtr & metadata_snapshot_ = nullptr,
        const SelectQueryOptions & = {});

    /// Reuse existing prepared_sets for another pass of analysis. It's used for projection.
    /// TODO: Find a general way of sharing sets among different interpreters, such as subqueries.
    InterpreterSelectQueryFragments(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        const SelectQueryOptions &,
        PreparedSetsPtr prepared_sets_);

    ~InterpreterSelectQueryFragments() override;

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
        QueryPlan & query_plan, const Block & source_header, const SelectQueryInfo & query_info, const ContextPtr & context_);

    Names getRequiredColumns() { return required_columns; }

    bool supportsTransactions() const override { return true; }

    FilterDAGInfoPtr getAdditionalQueryInfo() const { return additional_filter_info; }

    RowPolicyFilterPtr getRowPolicyFilter() const;

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr context) const override;

    static SortDescription getSortDescription(const ASTSelectQuery & query, const ContextPtr & context);
    static UInt64 getLimitForSorting(const ASTSelectQuery & query, const ContextPtr & context);

    PlanFragmentPtrs buildFragments();

private:
    InterpreterSelectQueryFragments(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        std::optional<Pipe> input_pipe,
        const StoragePtr & storage_,
        const SelectQueryOptions &,
        const Names & required_result_column_names = {},
        const StorageMetadataPtr & metadata_snapshot_ = nullptr,
        PreparedSetsPtr prepared_sets_ = nullptr);

    InterpreterSelectQueryFragments(
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
    bool shouldMoveToPrewhere();

    Block getSampleBlockImpl();

    void executeSinglePlan(QueryPlan & query_plan, std::optional<Pipe> prepared_pipe);
    PlanFragmentPtrs executeDistributedPlan(QueryPlan & query_plan);

    PlanFragmentPtrs createPlanFragments(Node & single_node_plan);
    PlanFragmentPtr createPlanFragments(Node & root_node, PlanFragmentPtrs & all_fragments);
    PlanFragmentPtr createOrderByFragment(QueryPlanStepPtr step, PlanFragmentPtr childFragment);
    PlanFragmentPtr createScanFragment(QueryPlanStepPtr step);
    PlanFragmentPtr createAggregationFragment(PlanFragmentPtr childFragment);
    PlanFragmentPtr createParentFragment(PlanFragmentPtr child_fragment, const DataPartition & partition);

    /// Different stages of query execution.
    void executeFetchColumns(QueryPlan & query_plan);
    void executeWhere(QueryPlan & query_plan, const ActionsDAGPtr & expression, bool remove_filter);
    void executeAggregation(
        QueryPlan & query_plan, const ActionsDAGPtr & expression, bool overflow_row, InputOrderInfoPtr group_by_info);
    std::shared_ptr<AggregatingStep> executeAggregationImpl(const DataStream & output_stream, bool overflow_row, bool final, InputOrderInfoPtr group_by_info);
    std::shared_ptr<MergingAggregatedStep> executeMergeAggregated(const DataStream & output_stream, bool overflow_row, bool final, bool has_grouping_sets);

    void executeTotalsAndHaving(QueryPlan & query_plan, bool has_having, const ActionsDAGPtr & expression, bool remove_filter, bool overflow_row, bool final);
    void executeHaving(QueryPlan & query_plan, const ActionsDAGPtr & expression, bool remove_filter);
    static void executeExpression(QueryPlan & query_plan, const ActionsDAGPtr & expression, const std::string & description);
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
    static void executeProjection(QueryPlan & query_plan, const ActionsDAGPtr & expression);
    void executeDistinct(QueryPlan & query_plan, bool before_order, Names columns, bool pre_distinct);
    void executeExtremes(QueryPlan & query_plan);
    void executeSubqueriesInSetsAndJoins(QueryPlan & query_plan);
    bool autoFinalOnQuery(ASTSelectQuery & select_query);

    enum class Modificator
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

    /// List of columns to read to execute the query.
    Names required_columns;
    /// Structure of query source (table, subquery, etc).
    Block source_header;

    /// Actions to calculate ALIAS if required.
    ActionsDAGPtr alias_actions;

    /// The subquery interpreter, if the subquery
    std::unique_ptr<InterpreterSelectWithUnionQuery> interpreter_subquery;

    /// Table from where to read data, if not subquery.
    StoragePtr storage;
    StorageID table_id = StorageID::createEmpty(); /// Will be initialized if storage is not nullptr
    TableLockHolder table_lock;

    /// Used when we read from prepared input, not table or subquery.
    std::optional<Pipe> input_pipe;

    Poco::Logger * log;
    StorageMetadataPtr metadata_snapshot;
    StorageSnapshotPtr storage_snapshot;

    /// Reuse already built sets for multiple passes of analysis, possibly across interpreters.
    PreparedSetsPtr prepared_sets;

    PlanFragmentPtrs fragments;
};

}
