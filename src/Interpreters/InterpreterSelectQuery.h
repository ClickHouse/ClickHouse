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
        const SelectQueryOptions & = {});

    ~InterpreterSelectQuery() override;

    /// Execute a query. Get the stream of blocks to read.
    BlockIO execute() override;

    /// Execute the query and return multuple streams for parallel processing.
    BlockInputStreams executeWithMultipleStreams(QueryPipeline & parent_pipeline);

    QueryPipeline executeWithProcessors() override;
    bool canExecuteWithProcessors() const override { return true; }

    bool ignoreLimits() const override { return options.ignore_limits; }
    bool ignoreQuota() const override { return options.ignore_quota; }

    Block getSampleBlock();

    void ignoreWithTotals();

    ASTPtr getQuery() const { return query_ptr; }

    size_t getMaxStreams() const { return max_streams; }

private:
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        const BlockInputStreamPtr & input_,
        std::optional<Pipe> input_pipe,
        const StoragePtr & storage_,
        const SelectQueryOptions &,
        const Names & required_result_column_names = {});

    ASTSelectQuery & getSelectQuery() { return query_ptr->as<ASTSelectQuery &>(); }

    Block getSampleBlockImpl();

    struct Pipeline
    {
        /** Streams of data.
          * The source data streams are produced in the executeFetchColumns function.
          * Then they are converted (wrapped in other streams) using the `execute*` functions,
          *  to get the whole pipeline running the query.
          */
        BlockInputStreams streams;

        /** When executing FULL or RIGHT JOIN, there will be a data stream from which you can read "not joined" rows.
          * It has a special meaning, since reading from it should be done after reading from the main streams.
          * It is appended to the main streams in UnionBlockInputStream or ParallelAggregatingBlockInputStream.
          */
        BlockInputStreamPtr stream_with_non_joined_data;
        bool union_stream = false;

        /// Cache value of InterpreterSelectQuery::max_streams
        size_t max_threads = 1;

        BlockInputStreamPtr & firstStream() { return streams.at(0); }

        template <typename Transform>
        void transform(Transform && transformation)
        {
            for (auto & stream : streams)
                transformation(stream);

            if (stream_with_non_joined_data)
                transformation(stream_with_non_joined_data);
        }

        bool hasMoreThanOneStream() const
        {
            return streams.size() + (stream_with_non_joined_data ? 1 : 0) > 1;
        }

        /// Resulting stream is mix of other streams data. Distinct and/or order guaranties are broken.
        bool hasMixedStreams() const
        {
            return hasMoreThanOneStream() || union_stream;
        }

        bool hasDelayedStream() const { return stream_with_non_joined_data != nullptr; }
        bool initialized() const { return !streams.empty(); }

        /// Compatibility with QueryPipeline (Processors)
        void   setMaxThreads(size_t max_threads_) { max_threads = max_threads_; }
        size_t getNumThreads() const { return max_threads; }
    };

    template <typename TPipeline>
    void executeImpl(TPipeline & pipeline, const BlockInputStreamPtr & prepared_input, std::optional<Pipe> prepared_pipe, QueryPipeline & save_context_and_storage);

    /// Different stages of query execution.

    /// dry_run - don't read from table, use empty header block instead.
    void executeWithMultipleStreamsImpl(Pipeline & pipeline, const BlockInputStreamPtr & input, bool dry_run);

    template <typename TPipeline>
    void executeFetchColumns(QueryProcessingStage::Enum processing_stage, TPipeline & pipeline,
        const PrewhereInfoPtr & prewhere_info,
        const Names & columns_to_remove_after_prewhere,
        QueryPipeline & save_context_and_storage);

    void executeWhere(Pipeline & pipeline, const ExpressionActionsPtr & expression, bool remove_filter);
    void executeAggregation(Pipeline & pipeline, const ExpressionActionsPtr & expression, bool overflow_row, bool final);
    void executeMergeAggregated(Pipeline & pipeline, bool overflow_row, bool final);
    void executeTotalsAndHaving(Pipeline & pipeline, bool has_having, const ExpressionActionsPtr & expression, bool overflow_row, bool final);
    void executeHaving(Pipeline & pipeline, const ExpressionActionsPtr & expression);
    static void executeExpression(Pipeline & pipeline, const ExpressionActionsPtr & expression);
    void executeOrder(Pipeline & pipeline, InputSortingInfoPtr sorting_info);
    void executeWithFill(Pipeline & pipeline);
    void executeMergeSorted(Pipeline & pipeline);
    void executePreLimit(Pipeline & pipeline);
    void executeUnion(Pipeline & pipeline, Block header);
    void executeLimitBy(Pipeline & pipeline);
    void executeLimit(Pipeline & pipeline);
    void executeOffset(Pipeline & pipeline);
    static void executeProjection(Pipeline & pipeline, const ExpressionActionsPtr & expression);
    void executeDistinct(Pipeline & pipeline, bool before_order, Names columns);
    void executeExtremes(Pipeline & pipeline);
    void executeSubqueriesInSetsAndJoins(Pipeline & pipeline, const std::unordered_map<String, SubqueryForSet> & subqueries_for_sets);
    void executeMergeSorted(Pipeline & pipeline, const SortDescription & sort_description, UInt64 limit);

    void executeWhere(QueryPipeline & pipeline, const ExpressionActionsPtr & expression, bool remove_filter);
    void executeAggregation(QueryPipeline & pipeline, const ExpressionActionsPtr & expression, bool overflow_row, bool final);
    void executeMergeAggregated(QueryPipeline & pipeline, bool overflow_row, bool final);
    void executeTotalsAndHaving(QueryPipeline & pipeline, bool has_having, const ExpressionActionsPtr & expression, bool overflow_row, bool final);
    void executeHaving(QueryPipeline & pipeline, const ExpressionActionsPtr & expression);
    static void executeExpression(QueryPipeline & pipeline, const ExpressionActionsPtr & expression);
    void executeOrder(QueryPipeline & pipeline, InputSortingInfoPtr sorting_info);
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

    String generateFilterActions(ExpressionActionsPtr & actions, const ASTPtr & row_policy_filter, const Names & prerequisite_columns = {}) const;

    /// Add ConvertingBlockInputStream to specified header.
    static void unifyStreams(Pipeline & pipeline, Block header);

    enum class Modificator
    {
        ROLLUP = 0,
        CUBE = 1
    };

    void executeRollupOrCube(Pipeline & pipeline, Modificator modificator);

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
};

}
