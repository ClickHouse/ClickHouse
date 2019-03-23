#pragma once

#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/AggregateDescription.h>
#include <Core/Settings.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Block;
class Context;

struct ExpressionActionsChain;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

struct ASTTableJoin;

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;
using Tables = std::map<String, StoragePtr>;

class ASTFunction;
class ASTExpressionList;
class ASTSelectQuery;

struct SyntaxAnalyzerResult;
using SyntaxAnalyzerResultPtr = std::shared_ptr<const SyntaxAnalyzerResult>;

/// ExpressionAnalyzer sources, intermediates and results. It splits data and logic, allows to test them separately.
/// If you are not writing a test you probably don't need it. Use ExpressionAnalyzer itself.
struct ExpressionAnalyzerData
{
    /// Original columns.
    /// First, all available columns of the table are placed here. Then (when analyzing the query), unused columns are deleted.
    NamesAndTypesList source_columns;

    /// If non-empty, ignore all expressions in  not from this list.
    NameSet required_result_columns;

    SubqueriesForSets subqueries_for_sets;
    PreparedSets prepared_sets;

    /// Columns after ARRAY JOIN, JOIN, and/or aggregation.
    NamesAndTypesList aggregated_columns;
    NamesAndTypesList array_join_columns;

    bool has_aggregation = false;
    NamesAndTypesList aggregation_keys;
    AggregateDescriptions aggregate_descriptions;

    bool has_global_subqueries = false;

    /// All new temporary tables obtained by performing the GLOBAL IN/JOIN subqueries.
    Tables external_tables;

    /// Predicate optimizer overrides the sub queries
    bool rewrite_subqueries = false;

    /// Columns will be added to block by join.
    JoinedColumnsList columns_added_by_join;  /// Subset of analyzed_join.available_joined_columns

protected:
    ExpressionAnalyzerData(const NamesAndTypesList & source_columns_,
                           const NameSet & required_result_columns_,
                           const SubqueriesForSets & subqueries_for_sets_)
    :   source_columns(source_columns_),
        required_result_columns(required_result_columns_),
        subqueries_for_sets(subqueries_for_sets_)
    {}
};


/** Transforms an expression from a syntax tree into a sequence of actions to execute it.
  *
  * NOTE: if `ast` is a SELECT query from a table, the structure of this table should not change during the lifetime of ExpressionAnalyzer.
  */
class ExpressionAnalyzer : private ExpressionAnalyzerData, private boost::noncopyable
{
private:
    /// Extracts settings to enlight which are used (and avoid copy of others).
    struct ExtractedSettings
    {
        /// for QueryNormalizer
        const UInt64 max_ast_depth;
        const UInt64 max_expanded_ast_elements;
        const String count_distinct_implementation;

        /// for PredicateExpressionsOptimizer
        const bool enable_optimize_predicate_expression;

        /// for ExpressionAnalyzer
        const bool asterisk_left_columns_only;
        const bool use_index_for_in_with_subqueries;
        const bool join_use_nulls;
        const SizeLimits size_limits_for_set;
        const SizeLimits size_limits_for_join;
        const String join_default_strictness;
        const UInt64 min_equality_disjunction_chain_length;

        ExtractedSettings(const Settings & settings)
        :   max_ast_depth(settings.max_ast_depth),
            max_expanded_ast_elements(settings.max_expanded_ast_elements),
            count_distinct_implementation(settings.count_distinct_implementation),
            enable_optimize_predicate_expression(settings.enable_optimize_predicate_expression),
            asterisk_left_columns_only(settings.asterisk_left_columns_only),
            use_index_for_in_with_subqueries(settings.use_index_for_in_with_subqueries),
            join_use_nulls(settings.join_use_nulls),
            size_limits_for_set(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode),
            size_limits_for_join(settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode),
            join_default_strictness(settings.join_default_strictness.toString()),
            min_equality_disjunction_chain_length(settings.optimize_min_equality_disjunction_chain_length)
        {}
    };

public:
    ExpressionAnalyzer(
        const ASTPtr & query_,
        const SyntaxAnalyzerResultPtr & syntax_analyzer_result_,
        const Context & context_,
        const NamesAndTypesList & additional_source_columns = {},
        const NameSet & required_result_columns_ = {},
        size_t subquery_depth_ = 0,
        bool do_global_ = false,
        const SubqueriesForSets & subqueries_for_set_ = {});

    /// Does the expression have aggregate functions or a GROUP BY or HAVING section.
    bool hasAggregation() const { return has_aggregation; }

    /// Get a list of aggregation keys and descriptions of aggregate functions if the query contains GROUP BY.
    void getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates) const;

    /** Get a set of columns that are enough to read from the table to evaluate the expression.
      * Columns added from another table by JOIN are not counted.
      */
    Names getRequiredSourceColumns() const { return source_columns.getNames(); }

    /** These methods allow you to build a chain of transformations over a block, that receives values in the desired sections of the query.
      *
      * Example usage:
      *   ExpressionActionsChain chain;
      *   analyzer.appendWhere(chain);
      *   chain.addStep();
      *   analyzer.appendSelect(chain);
      *   analyzer.appendOrderBy(chain);
      *   chain.finalize();
      *
      * If only_types = true set, does not execute subqueries in the relevant parts of the query. The actions got this way
      *  shouldn't be executed, they are only needed to get a list of columns with their types.
      */

    /// Before aggregation:
    bool appendArrayJoin(ExpressionActionsChain & chain, bool only_types);
    bool appendJoin(ExpressionActionsChain & chain, bool only_types);
    /// remove_filter is set in ExpressionActionsChain::finalize();
    /// Columns in `additional_required_columns` will not be removed (they can be used for e.g. sampling or FINAL modifier).
    bool appendPrewhere(ExpressionActionsChain & chain, bool only_types, const Names & additional_required_columns);
    bool appendWhere(ExpressionActionsChain & chain, bool only_types);
    bool appendGroupBy(ExpressionActionsChain & chain, bool only_types);
    void appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types);

    /// After aggregation:
    bool appendHaving(ExpressionActionsChain & chain, bool only_types);
    void appendSelect(ExpressionActionsChain & chain, bool only_types);
    bool appendOrderBy(ExpressionActionsChain & chain, bool only_types);
    bool appendLimitBy(ExpressionActionsChain & chain, bool only_types);
    /// Deletes all columns except mentioned by SELECT, arranges the remaining columns and renames them to aliases.
    void appendProjectResult(ExpressionActionsChain & chain) const;

    void appendExpression(ExpressionActionsChain & chain, const ASTPtr & expr, bool only_types);

    /// If `ast` is not a SELECT query, just gets all the actions to evaluate the expression.
    /// If add_aliases, only the calculated values in the desired order and add aliases.
    ///     If also project_result, than only aliases remain in the output block.
    /// Otherwise, only temporary columns will be deleted from the block.
    ExpressionActionsPtr getActions(bool add_aliases, bool project_result = true);

    /// Actions that can be performed on an empty block: adding constants and applying functions that depend only on constants.
    /// Does not execute subqueries.
    ExpressionActionsPtr getConstActions();

    /** Sets that require a subquery to be create.
      * Only the sets needed to perform actions returned from already executed `append*` or `getActions`.
      * That is, you need to call getSetsWithSubqueries after all calls of `append*` or `getActions`
      *  and create all the returned sets before performing the actions.
      */
    const SubqueriesForSets & getSubqueriesForSets() const { return subqueries_for_sets; }

    const PreparedSets & getPreparedSets() const { return prepared_sets; }

    /** Tables that will need to be sent to remote servers for distributed query processing.
      */
    const Tables & getExternalTables() const { return external_tables; }

    /// Get intermediates for tests
    const ExpressionAnalyzerData & getAnalyzedData() const { return *this; }

    /// Create Set-s that we can from IN section to use the index on them.
    void makeSetsForIndex();

    bool isRewriteSubqueriesPredicate() { return rewrite_subqueries; }

    bool hasGlobalSubqueries() { return has_global_subqueries; }

private:
    ASTPtr query;
    const Context & context;
    const ExtractedSettings settings;
    StoragePtr storage; /// The main table in FROM clause, if exists.
    size_t subquery_depth;
    bool do_global; /// Do I need to prepare for execution global subqueries when analyzing the query.

    SyntaxAnalyzerResultPtr syntax;
    const AnalyzedJoin & analyzedJoin() const { return syntax->analyzed_join; }

    /** Remove all unnecessary columns from the list of all available columns of the table (`columns`).
      * At the same time, form a set of columns added by JOIN (`columns_added_by_join`).
      */
    void collectUsedColumns();

    /// Find global subqueries in the GLOBAL IN/JOIN sections. Fills in external_tables.
    void initGlobalSubqueriesAndExternalTables();

    void addMultipleArrayJoinAction(ExpressionActionsPtr & actions, bool is_left) const;

    void addJoinAction(ExpressionActionsPtr & actions, bool only_types) const;

    /// If ast is ASTSelectQuery with JOIN, add actions for JOIN key columns.
    void getActionsFromJoinKeys(const ASTTableJoin & table_join, bool no_subqueries, ExpressionActionsPtr & actions);

    void getRootActions(const ASTPtr & ast, bool no_subqueries, ExpressionActionsPtr & actions, bool only_consts = false);

    void getActionsBeforeAggregation(const ASTPtr & ast, ExpressionActionsPtr & actions, bool no_subqueries);

    /** Add aggregation keys to aggregation_keys, aggregate functions to aggregate_descriptions,
      * Create a set of columns aggregated_columns resulting after the aggregation, if any,
      *  or after all the actions that are normally performed before aggregation.
      * Set has_aggregation = true if there is GROUP BY or at least one aggregate function.
      */
    void analyzeAggregation();
    void getAggregates(const ASTPtr & ast, ExpressionActionsPtr & actions);
    void assertNoAggregates(const ASTPtr & ast, const char * description);

    /// columns - the columns that are present before the transformations begin.
    void initChain(ExpressionActionsChain & chain, const NamesAndTypesList & columns) const;

    void assertSelect() const;
    void assertAggregation() const;

    /**
      * Create Set from a subuquery or a table expression in the query. The created set is suitable for using the index.
      * The set will not be created if its size hits the limit.
      */
    void tryMakeSetForIndexFromSubquery(const ASTPtr & subquery_or_table_name);

    void makeSetsForIndexImpl(const ASTPtr & node);

    bool isRemoteStorage() const;
};

}
