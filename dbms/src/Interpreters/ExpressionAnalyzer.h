#pragma once

#include <Core/Settings.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/SubqueryForSet.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>


namespace DB
{

class Block;
class Context;

struct ExpressionActionsChain;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

struct ASTTableJoin;

class ASTFunction;
class ASTExpressionList;
class ASTSelectQuery;
struct ASTTablesInSelectQueryElement;

/// ExpressionAnalyzer sources, intermediates and results. It splits data and logic, allows to test them separately.
struct ExpressionAnalyzerData
{
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
};


/** Transforms an expression from a syntax tree into a sequence of actions to execute it.
  *
  * NOTE: if `ast` is a SELECT query from a table, the structure of this table should not change during the lifetime of ExpressionAnalyzer.
  */
class ExpressionAnalyzer : protected ExpressionAnalyzerData, private boost::noncopyable
{
private:
    /// Extracts settings to enlight which are used (and avoid copy of others).
    struct ExtractedSettings
    {
        const bool use_index_for_in_with_subqueries;
        const bool join_use_nulls;
        const SizeLimits size_limits_for_set;
        const SizeLimits size_limits_for_join;

        ExtractedSettings(const Settings & settings_)
        :   use_index_for_in_with_subqueries(settings_.use_index_for_in_with_subqueries),
            join_use_nulls(settings_.join_use_nulls),
            size_limits_for_set(settings_.max_rows_in_set, settings_.max_bytes_in_set, settings_.set_overflow_mode),
            size_limits_for_join(settings_.max_rows_in_join, settings_.max_bytes_in_join, settings_.join_overflow_mode)
        {}
    };

public:
    /// Ctor for non-select queries. Generally its usage is:
    /// auto actions = ExpressionAnalyzer(query, syntax, context).getActions();
    ExpressionAnalyzer(
        const ASTPtr & query_,
        const SyntaxAnalyzerResultPtr & syntax_analyzer_result_,
        const Context & context_)
    :   ExpressionAnalyzer(query_, syntax_analyzer_result_, context_, 0, false)
    {}

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

    /// Get intermediates for tests
    const ExpressionAnalyzerData & getAnalyzedData() const { return *this; }

protected:
    ExpressionAnalyzer(
        const ASTPtr & query_,
        const SyntaxAnalyzerResultPtr & syntax_analyzer_result_,
        const Context & context_,
        size_t subquery_depth_,
        bool do_global_);

    ASTPtr query;
    const Context & context;
    const ExtractedSettings settings;
    size_t subquery_depth;

    SyntaxAnalyzerResultPtr syntax;

    const StoragePtr & storage() const { return syntax->storage; } /// The main table in FROM clause, if exists.
    const AnalyzedJoin & analyzedJoin() const { return *syntax->analyzed_join; }
    const NamesAndTypesList & sourceColumns() const { return syntax->required_source_columns; }
    const std::vector<const ASTFunction *> & aggregates() const { return syntax->aggregates; }

    /// Find global subqueries in the GLOBAL IN/JOIN sections. Fills in external_tables.
    void initGlobalSubqueriesAndExternalTables(bool do_global);

    void addMultipleArrayJoinAction(ExpressionActionsPtr & actions, bool is_left) const;

    void addJoinAction(ExpressionActionsPtr & actions) const;

    void getRootActions(const ASTPtr & ast, bool no_subqueries, ExpressionActionsPtr & actions, bool only_consts = false);

    /** Add aggregation keys to aggregation_keys, aggregate functions to aggregate_descriptions,
      * Create a set of columns aggregated_columns resulting after the aggregation, if any,
      *  or after all the actions that are normally performed before aggregation.
      * Set has_aggregation = true if there is GROUP BY or at least one aggregate function.
      */
    void analyzeAggregation();
    bool makeAggregateDescriptions(ExpressionActionsPtr & actions);

    /// columns - the columns that are present before the transformations begin.
    void initChain(ExpressionActionsChain & chain, const NamesAndTypesList & columns) const;

    const ASTSelectQuery * getSelectQuery() const;

    bool isRemoteStorage() const;
};

/// SelectQuery specific ExpressionAnalyzer part.
class SelectQueryExpressionAnalyzer : public ExpressionAnalyzer
{
public:
    SelectQueryExpressionAnalyzer(
        const ASTPtr & query_,
        const SyntaxAnalyzerResultPtr & syntax_analyzer_result_,
        const Context & context_,
        const NameSet & required_result_columns_ = {},
        size_t subquery_depth_ = 0,
        bool do_global_ = false)
    :   ExpressionAnalyzer(query_, syntax_analyzer_result_, context_, subquery_depth_, do_global_)
    ,   required_result_columns(required_result_columns_)
    {}

    /// Does the expression have aggregate functions or a GROUP BY or HAVING section.
    bool hasAggregation() const { return has_aggregation; }
    bool hasGlobalSubqueries() { return has_global_subqueries; }

    /// Get a list of aggregation keys and descriptions of aggregate functions if the query contains GROUP BY.
    void getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates) const;

    const PreparedSets & getPreparedSets() const { return prepared_sets; }

    /// Tables that will need to be sent to remote servers for distributed query processing.
    const Tables & getExternalTables() const { return external_tables; }

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

    /// Create Set-s that we can from IN section to use the index on them.
    void makeSetsForIndex(const ASTPtr & node);

private:
    /// If non-empty, ignore all expressions not from this list.
    NameSet required_result_columns;

    /**
      * Create Set from a subquery or a table expression in the query. The created set is suitable for using the index.
      * The set will not be created if its size hits the limit.
      */
    void tryMakeSetForIndexFromSubquery(const ASTPtr & subquery_or_table_name);

    void makeTableJoin(const ASTTablesInSelectQueryElement & join_element);
    void makeSubqueryForJoin(const ASTTablesInSelectQueryElement & join_element, const ExpressionActionsPtr & joined_block_actions,
                             SubqueryForSet & subquery_for_set) const;

    const ASTSelectQuery * getAggregatingQuery() const;
};

}
