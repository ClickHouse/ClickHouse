#pragma once

#include <memory>

#include <Core/QueryProcessingStage.h>
#include <Parsers/ASTSelectQuery.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Storages/SelectQueryInfo.h>


namespace Poco { class Logger; }

namespace DB
{

struct SubqueryForSet;
class InterpreterSelectWithUnionQuery;

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
        const Names & required_result_column_names = Names{});

    /// Read data not from the table specified in the query, but from the prepared source `input`.
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        const BlockInputStreamPtr & input_,
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
    BlockInputStreams executeWithMultipleStreams();

    Block getSampleBlock();

    void ignoreWithTotals();

    ASTPtr getQuery() const { return query_ptr; }

private:
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        const BlockInputStreamPtr & input_,
        const StoragePtr & storage_,
        const SelectQueryOptions &,
        const Names & required_result_column_names = {});

    ASTSelectQuery & getSelectQuery() { return query_ptr->as<ASTSelectQuery &>(); }


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
    };

    void executeImpl(Pipeline & pipeline, const BlockInputStreamPtr & prepared_input, bool dry_run);


    struct AnalysisResult
    {
        bool hasJoin() const { return before_join.get(); }
        bool has_where      = false;
        bool need_aggregate = false;
        bool has_having     = false;
        bool has_order_by   = false;
        bool has_limit_by   = false;

        bool remove_where_filter = false;

        ExpressionActionsPtr before_join;   /// including JOIN
        ExpressionActionsPtr before_where;
        ExpressionActionsPtr before_aggregation;
        ExpressionActionsPtr before_having;
        ExpressionActionsPtr before_order_and_select;
        ExpressionActionsPtr before_limit_by;
        ExpressionActionsPtr final_projection;

        /// Columns from the SELECT list, before renaming them to aliases.
        Names selected_columns;

        /// Columns will be removed after prewhere actions execution.
        Names columns_to_remove_after_prewhere;

        /// Do I need to perform the first part of the pipeline - running on remote servers during distributed processing.
        bool first_stage = false;
        /// Do I need to execute the second part of the pipeline - running on the initiating server during distributed processing.
        bool second_stage = false;

        SubqueriesForSets subqueries_for_sets;
        PrewhereInfoPtr prewhere_info;
        FilterInfoPtr filter_info;
    };

    AnalysisResult analyzeExpressions(QueryProcessingStage::Enum from_stage, bool dry_run, const FilterInfoPtr & filter_info);


    /** From which table to read. With JOIN, the "left" table is returned.
     */
    void getDatabaseAndTableNames(String & database_name, String & table_name);

    /// Different stages of query execution.

    /// dry_run - don't read from table, use empty header block instead.
    void executeWithMultipleStreamsImpl(Pipeline & pipeline, const BlockInputStreamPtr & input, bool dry_run);

    void executeFetchColumns(QueryProcessingStage::Enum processing_stage, Pipeline & pipeline,
                             const PrewhereInfoPtr & prewhere_info, const Names & columns_to_remove_after_prewhere);

    void executeWhere(Pipeline & pipeline, const ExpressionActionsPtr & expression, bool remove_filter);
    void executeAggregation(Pipeline & pipeline, const ExpressionActionsPtr & expression, bool overflow_row, bool final);
    void executeMergeAggregated(Pipeline & pipeline, bool overflow_row, bool final);
    void executeTotalsAndHaving(Pipeline & pipeline, bool has_having, const ExpressionActionsPtr & expression, bool overflow_row, bool final);
    void executeHaving(Pipeline & pipeline, const ExpressionActionsPtr & expression);
    void executeExpression(Pipeline & pipeline, const ExpressionActionsPtr & expression);
    void executeOrder(Pipeline & pipeline);
    void executeMergeSorted(Pipeline & pipeline);
    void executePreLimit(Pipeline & pipeline);
    void executeUnion(Pipeline & pipeline);
    void executeLimitBy(Pipeline & pipeline);
    void executeLimit(Pipeline & pipeline);
    void executeProjection(Pipeline & pipeline, const ExpressionActionsPtr & expression);
    void executeDistinct(Pipeline & pipeline, bool before_order, Names columns);
    void executeExtremes(Pipeline & pipeline);
    void executeSubqueriesInSetsAndJoins(Pipeline & pipeline, std::unordered_map<String, SubqueryForSet> & subqueries_for_sets);

    /// If pipeline has several streams with different headers, add ConvertingBlockInputStream to first header.
    void unifyStreams(Pipeline & pipeline);

    enum class Modificator
    {
        ROLLUP = 0,
        CUBE = 1
    };

    void executeRollupOrCube(Pipeline & pipeline, Modificator modificator);

    /** If there is a SETTINGS section in the SELECT query, then apply settings from it.
      *
      * Section SETTINGS - settings for a specific query.
      * Normally, the settings can be passed in other ways, not inside the query.
      * But the use of this section is justified if you need to set the settings for one subquery.
      */
    void initSettings();

    const SelectQueryOptions options;
    ASTPtr query_ptr;
    Context context;
    NamesAndTypesList source_columns;
    SyntaxAnalyzerResultPtr syntax_analyzer_result;
    std::unique_ptr<ExpressionAnalyzer> query_analyzer;

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
    TableStructureReadLockHolder table_lock;

    /// Used when we read from prepared input, not table or subquery.
    BlockInputStreamPtr input;

    Poco::Logger * log;
};

}
