#pragma once

#include <memory>

#include <Core/QueryProcessingStage.h>
#include <Parsers/ASTSelectQuery.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/TableStructureLockHolder.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Interpreters/StorageID.h>

#include <Processors/QueryPipeline.h>
#include <Columns/FilterDescription.h>

namespace Poco { class Logger; }

namespace DB
{

struct SubqueryForSet;
class InterpreterSelectWithUnionQuery;
class Context;

struct SyntaxAnalyzerResult;
using SyntaxAnalyzerResultPtr = std::shared_ptr<const SyntaxAnalyzerResult>;


/** Interprets the SELECT query. Returns the stream of blocks with the results of the query before `to_stage` stage.
  */
class InterpreterSelectQuery : public IInterpreter
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
        const Context & context_,
        const SelectQueryOptions &,
        const Names & required_result_column_names_ = Names{});

    /// Read data not from the table specified in the query, but from the prepared source `input`.
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        const BlockInputStreamPtr & input_,
        const SelectQueryOptions & = {});

    /// Read data not from the table specified in the query, but from the prepared pipe `input`.
    InterpreterSelectQuery(
            const ASTPtr & query_ptr_,
            const Context & context_,
            Pipe input_pipe_,
            const SelectQueryOptions & = {});

    /// Read data not from the table specified in the query, but from the specified `storage_`.
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        const StoragePtr & storage_,
        const StorageMetadataPtr & metadata_snapshot_ = nullptr,
        const SelectQueryOptions & = {});

    ~InterpreterSelectQuery() override;

    /// Execute a query. Get the stream of blocks to read.
    BlockIO execute() override;

    bool ignoreLimits() const override { return options.ignore_limits; }
    bool ignoreQuota() const override { return options.ignore_quota; }

    Block getSampleBlock();

    void ignoreWithTotals();

    ASTPtr getQuery() const { return query_ptr; }

    size_t getMaxStreams() const { return max_streams; }

    const SelectQueryInfo & getQueryInfo() const { return query_info; }

private:
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        const BlockInputStreamPtr & input_,
        std::optional<Pipe> input_pipe,
        const StoragePtr & storage_,
        const SelectQueryOptions &,
        const Names & required_result_column_names = {},
        const StorageMetadataPtr & metadata_snapshot_= nullptr);

    ASTSelectQuery & getSelectQuery() { return query_ptr->as<ASTSelectQuery &>(); }

    Block getSampleBlockImpl();

    void executeImpl(QueryPipeline & pipeline, const BlockInputStreamPtr & prepared_input, std::optional<Pipe> prepared_pipe);

    /// Different stages of query execution.

    void executeFetchColumns(
        QueryProcessingStage::Enum processing_stage,
        QueryPipeline & pipeline,
        const PrewhereInfoPtr & prewhere_info,
        const Names & columns_to_remove_after_prewhere);

    void executeWhere(QueryPipeline & pipeline, const ExpressionActionsPtr & expression, bool remove_filter);
    void executeAggregation(QueryPipeline & pipeline, const ExpressionActionsPtr & expression, bool overflow_row, bool final, InputOrderInfoPtr group_by_info);
    void executeMergeAggregated(QueryPipeline & pipeline, bool overflow_row, bool final);
    void executeTotalsAndHaving(QueryPipeline & pipeline, bool has_having, const ExpressionActionsPtr & expression, bool overflow_row, bool final);
    void executeHaving(QueryPipeline & pipeline, const ExpressionActionsPtr & expression);
    static void executeExpression(QueryPipeline & pipeline, const ExpressionActionsPtr & expression);
    void executeOrder(QueryPipeline & pipeline, InputOrderInfoPtr sorting_info);
    void executeOrderOptimized(QueryPipeline & pipeline, InputOrderInfoPtr sorting_info, UInt64 limit, SortDescription & output_order_descr);
    void executeWithFill(QueryPipeline & pipeline);
    void executeMergeSorted(QueryPipeline & pipeline);
    void executePreLimit(QueryPipeline & pipeline, bool do_not_skip_offset);
    void executeLimitBy(QueryPipeline & pipeline);
    void executeLimit(QueryPipeline & pipeline);
    void executeOffset(QueryPipeline & pipeline);
    static void executeProjection(QueryPipeline & pipeline, const ExpressionActionsPtr & expression);
    void executeDistinct(QueryPipeline & pipeline, bool before_order, Names columns);
    void executeExtremes(QueryPipeline & pipeline);
    void executeSubqueriesInSetsAndJoins(QueryPipeline & pipeline, const std::unordered_map<String, SubqueryForSet> & subqueries_for_sets);
    void executeMergeSorted(QueryPipeline & pipeline, const SortDescription & sort_description, UInt64 limit);

    String generateFilterActions(
        ExpressionActionsPtr & actions, const ASTPtr & row_policy_filter, const Names & prerequisite_columns = {}) const;

    enum class Modificator
    {
        ROLLUP = 0,
        CUBE = 1
    };

    void executeRollupOrCube(QueryPipeline & pipeline, Modificator modificator);

    /** If there is a SETTINGS section in the SELECT query, then apply settings from it.
      *
      * Section SETTINGS - settings for a specific query.
      * Normally, the settings can be passed in other ways, not inside the query.
      * But the use of this section is justified if you need to set the settings for one subquery.
      */
    void initSettings();

    SelectQueryOptions options;
    ASTPtr query_ptr;
    std::shared_ptr<Context> context;
    SyntaxAnalyzerResultPtr syntax_analyzer_result;
    std::unique_ptr<SelectQueryExpressionAnalyzer> query_analyzer;
    SelectQueryInfo query_info;

    /// Is calculated in getSampleBlock. Is used later in readImpl.
    ExpressionAnalysisResult analysis_result;
    /// For row-level security.
    FilterInfoPtr filter_info;

    QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;

    /// List of columns to read to execute the query.
    Names required_columns;
    /// Structure of query source (table, subquery, etc).
    Block source_header;
    /// Structure of query result.
    Block result_header;

    /// The subquery interpreter, if the subquery
    std::unique_ptr<InterpreterSelectWithUnionQuery> interpreter_subquery;

    /// Table from where to read data, if not subquery.
    StoragePtr storage;
    StorageID table_id = StorageID::createEmpty();  /// Will be initialized if storage is not nullptr
    TableStructureReadLockHolder table_lock;

    /// Used when we read from prepared input, not table or subquery.
    BlockInputStreamPtr input;
    std::optional<Pipe> input_pipe;

    Poco::Logger * log;
    StorageMetadataPtr metadata_snapshot;
};

}
