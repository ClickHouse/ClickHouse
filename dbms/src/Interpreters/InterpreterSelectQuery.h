#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/ExpressionActions.h>
#include <DataStreams/IBlockInputStream.h>


namespace Poco { class Logger; }

namespace DB
{

class ExpressionAnalyzer;
class ASTSelectQuery;
struct SubqueryForSet;


/** Interprets the SELECT query. Returns the stream of blocks with the results of the query before `to_stage` stage.
  */
class InterpreterSelectQuery : public IInterpreter
{
public:
    /** `to_stage`
     * - the stage to which the query is to be executed. By default - till to the end.
     *   You can perform till the intermediate aggregation state, which are combined from different servers for distributed query processing.
     *
     * subquery_depth
     * - to control the restrictions on the depth of nesting of subqueries. For subqueries, a value that is incremented by one is passed.
     *
     * input
     * - if given - read not from the table specified in the query, but from ready source.
     *
     * required_column_names
     * - delete all columns except the specified ones from the query - it is used to delete unnecessary columns from subqueries.
     *
     * table_column_names
     * - the list of available columns of the table.
     *   Used, for example, with reference to `input`.
     */

    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
        size_t subquery_depth_ = 0,
        BlockInputStreamPtr input = nullptr);

    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        const Names & required_column_names,
        QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
        size_t subquery_depth_ = 0,
        BlockInputStreamPtr input = nullptr);

    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        const Names & required_column_names,
        const NamesAndTypesList & table_column_names_,
        QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
        size_t subquery_depth_ = 0,
        BlockInputStreamPtr input = nullptr);

    ~InterpreterSelectQuery();

    /** Execute a query, possibly part of UNION ALL chain.
     *  Get the stream of blocks to read
     */
    BlockIO execute() override;

    /** Execute the query without union of threads, if it is possible.
     */
    const BlockInputStreams & executeWithoutUnion();

    DataTypes getReturnTypes();
    Block getSampleBlock();

    static Block getSampleBlock(
        const ASTPtr & query_ptr_,
        const Context & context_);

private:
    /**
     * - Optimization if an object is created only to call getSampleBlock(): consider only the first SELECT of the UNION ALL chain, because
     *   the first SELECT is sufficient to determine the required columns.
     */
    struct OnlyAnalyzeTag {};
    InterpreterSelectQuery(
        OnlyAnalyzeTag,
        const ASTPtr & query_ptr_,
        const Context & context_);

    void init(BlockInputStreamPtr input, const Names & required_column_names = Names{});
    void basicInit(BlockInputStreamPtr input);
    void initQueryAnalyzer();

    /// Execute one SELECT query from the UNION ALL chain.
    void executeSingleQuery();

    /** Leave only the necessary columns of the SELECT section in each query of the UNION ALL chain.
     *  However, if you use at least one DISTINCT in the chain, then all the columns are considered necessary,
     *   since otherwise DISTINCT would work differently.
     *
     *  Always leave arrayJoin, because it changes number of rows.
     *
     *  TODO If query doesn't have GROUP BY, but have aggregate functions,
     *   then leave at least one aggregate function,
     *   In order that fact of aggregation has not been lost.
     */
    void rewriteExpressionList(const Names & required_column_names);

    /// Does the request contain at least one asterisk?
    bool hasAsterisk() const;

    // Rename the columns of each query for the UNION ALL chain into the same names as in the first query.
    void renameColumns();

    /** From which table to read. With JOIN, the "left" table is returned.
     */
    void getDatabaseAndTableNames(String & database_name, String & table_name);

    /** Select from the list of columns any, better - with minimum size.
     */
    String getAnyColumn();

    /// Different stages of query execution.

    /// Fetch data from the table. Returns the stage to which the query was processed in Storage.
    QueryProcessingStage::Enum executeFetchColumns();

    void executeWhere(ExpressionActionsPtr expression);
    void executeAggregation(ExpressionActionsPtr expression, bool overflow_row, bool final);
    void executeMergeAggregated(bool overflow_row, bool final);
    void executeTotalsAndHaving(bool has_having, ExpressionActionsPtr expression, bool overflow_row);
    void executeHaving(ExpressionActionsPtr expression);
    void executeExpression(ExpressionActionsPtr expression);
    void executeOrder();
    void executeMergeSorted();
    void executePreLimit();
    void executeUnion();
    void executeLimitBy();
    void executeLimit();
    void executeProjection(ExpressionActionsPtr expression);
    void executeDistinct(bool before_order, Names columns);
    void executeSubqueriesInSetsAndJoins(std::unordered_map<String, SubqueryForSet> & subqueries_for_sets);

    template <typename Transform>
    void transformStreams(Transform && transform);

    bool hasNoData() const;

    bool hasMoreThanOneStream() const;

    void ignoreWithTotals();

    /** If there is a SETTINGS section in the SELECT query, then apply settings from it.
      *
      * Section SETTINGS - settings for a specific query.
      * Normally, the settings can be passed in other ways, not inside the query.
      * But the use of this section is justified if you need to set the settings for one subquery.
      */
    void initSettings();

    ASTPtr query_ptr;
    ASTSelectQuery & query;
    Context context;
    QueryProcessingStage::Enum to_stage;
    size_t subquery_depth;
    std::unique_ptr<ExpressionAnalyzer> query_analyzer;
    NamesAndTypesList table_column_names;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;

    /** Streams of data.
      * The source data streams are produced in the executeFetchColumns function.
      * Then they are converted (wrapped in other streams) using the `execute*` functions,
      *  to get the whole pipeline running the query.
      */
    BlockInputStreams streams;

    /** When executing FULL or RIGHT JOIN, there will be a data stream from which you can read "not joined" rows.
      * It has a special meaning, since reading from it should be done after reading from the main streams.
      * It is joined to the main streams in UnionBlockInputStream or ParallelAggregatingBlockInputStream.
      */
    BlockInputStreamPtr stream_with_non_joined_data;

    /// Is it the first SELECT query of the UNION ALL chain?
    bool is_first_select_inside_union_all;

    /// The object was created only for query analysis.
    bool only_analyze = false;

    /// The next SELECT query in the UNION ALL chain, if any.
    std::unique_ptr<InterpreterSelectQuery> next_select_in_union_all;

    /// Table from where to read data, if not subquery.
    StoragePtr storage;
    TableStructureReadLockPtr table_lock;

    /// Do union of streams within a SELECT query?
    bool union_within_single_query = false;

    Poco::Logger * log;
};

}
