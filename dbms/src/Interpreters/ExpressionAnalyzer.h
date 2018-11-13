#pragma once

#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ActionsVisitor.h>

#include <Core/Block.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

class Context;

class ExpressionActions;
struct ExpressionActionsChain;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;
using Tables = std::map<String, StoragePtr>;

class ASTFunction;
class ASTExpressionList;
class ASTSelectQuery;


inline SizeLimits getSetSizeLimits(const Settings & settings)
{
    return SizeLimits(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode);
}


/** Transforms an expression from a syntax tree into a sequence of actions to execute it.
  *
  * NOTE: if `ast` is a SELECT query from a table, the structure of this table should not change during the lifetime of ExpressionAnalyzer.
  */
class ExpressionAnalyzer : private boost::noncopyable
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

public:
    ExpressionAnalyzer(
        const ASTPtr & query_,
        const Context & context_,
        const StoragePtr & storage_,
        const NamesAndTypesList & source_columns_ = {},
        const Names & required_result_columns_ = {},
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
    Names getRequiredSourceColumns() const;

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

    /// Create Set-s that we can from IN section to use the index on them.
    void makeSetsForIndex();

    bool isRewriteSubqueriesPredicate() { return rewrite_subqueries; }

    bool hasGlobalSubqueries() { return has_global_subqueries; }

private:
    ASTPtr query;
    ASTSelectQuery * select_query;
    const Context & context;
    const Settings settings;
    size_t subquery_depth;

    /** Original columns.
      * First, all available columns of the table are placed here. Then (when analyzing the query), unused columns are deleted.
      */
    NamesAndTypesList source_columns;

    /** If non-empty, ignore all expressions in  not from this list.
      */
    Names required_result_columns;

    /// Columns after ARRAY JOIN, JOIN, and/or aggregation.
    NamesAndTypesList aggregated_columns;

    NamesAndTypesList array_join_columns;

    /// The main table in FROM clause, if exists.
    StoragePtr storage;

    bool has_aggregation = false;
    NamesAndTypesList aggregation_keys;
    AggregateDescriptions aggregate_descriptions;

    /// Do I need to prepare for execution global subqueries when analyzing the query.
    bool do_global;
    bool has_global_subqueries = false;

    SubqueriesForSets subqueries_for_sets;

    PreparedSets prepared_sets;

    struct AnalyzedJoin
    {

        /// NOTE: So far, only one JOIN per query is supported.

        /** Query of the form `SELECT expr(x) AS k FROM t1 ANY LEFT JOIN (SELECT expr(x) AS k FROM t2) USING k`
          * The join is made by column k.
          * During the JOIN,
          *  - in the "right" table, it will be available by alias `k`, since `Project` action for the subquery was executed.
          *  - in the "left" table, it will be accessible by the name `expr(x)`, since `Project` action has not been executed yet.
          * You must remember both of these options.
          *
          * Query of the form `SELECT ... from t1 ANY LEFT JOIN (SELECT ... from t2) ON expr(t1 columns) = expr(t2 columns)`
          *     to the subquery will be added expression `expr(t2 columns)`.
          * It's possible to use name `expr(t2 columns)`.
          */
        Names key_names_left;
        Names key_names_right; /// Duplicating names are qualified.
        ASTs key_asts_left;
        ASTs key_asts_right;

        struct JoinedColumn
        {
            /// Column will be joined to block.
            NameAndTypePair name_and_type;
            /// original column name from joined source.
            String original_name;

            JoinedColumn(const NameAndTypePair & name_and_type_, const String & original_name_)
                    : name_and_type(name_and_type_), original_name(original_name_) {}
        };

        using JoinedColumnsList = std::list<JoinedColumn>;

        /// All columns which can be read from joined table. Duplicating names are qualified.
        JoinedColumnsList columns_from_joined_table;
        /// Columns which will be used in query to the joined query. Duplicating names are qualified.
        NameSet required_columns_from_joined_table;

        /// Columns which will be added to block, possible including some columns from right join key.
        JoinedColumnsList columns_added_by_join;
        /// Such columns will be copied from left join keys during join.
        NameSet columns_added_by_join_from_right_keys;
        /// Actions which need to be calculated on joined block.
        ExpressionActionsPtr joined_block_actions;

        void createJoinedBlockActions(const NamesAndTypesList & source_columns,
                                      const ASTSelectQuery * select_query_with_join,
                                      const Context & context);

        NamesAndTypesList getColumnsAddedByJoin() const;

        const JoinedColumnsList & getColumnsFromJoinedTable(const NamesAndTypesList & source_columns,
                                                            const Context & context,
                                                            const ASTSelectQuery * select_query_with_join);
    };

    AnalyzedJoin analyzed_join;

    using Aliases = std::unordered_map<String, ASTPtr>;
    Aliases aliases;

    using SetOfASTs = std::set<const IAST *>;
    using MapOfASTs = std::map<ASTPtr, ASTPtr>;

    /// Which column is needed to be ARRAY-JOIN'ed to get the specified.
    /// For example, for `SELECT s.v ... ARRAY JOIN a AS s` will get "s.v" -> "a.v".
    NameToNameMap array_join_result_to_source;

    /// For the ARRAY JOIN section, mapping from the alias to the full column name.
    /// For example, for `ARRAY JOIN [1,2] AS b` "b" -> "array(1,2)" will enter here.
    NameToNameMap array_join_alias_to_name;

    /// The backward mapping for array_join_alias_to_name.
    NameToNameMap array_join_name_to_alias;


    /// All new temporary tables obtained by performing the GLOBAL IN/JOIN subqueries.
    Tables external_tables;

    /// Predicate optimizer overrides the sub queries
    bool rewrite_subqueries = false;

    /** Remove all unnecessary columns from the list of all available columns of the table (`columns`).
      * At the same time, form a set of unknown columns (`unknown_required_source_columns`),
      * as well as the columns added by JOIN (`columns_added_by_join`).
      */
    void collectUsedColumns();

    /** Find the columns that are obtained by JOIN.
      */
    void collectJoinedColumns(NameSet & joined_columns);
    /// Parse JOIN ON expression and collect ASTs for joined columns.
    void collectJoinedColumnsFromJoinOnExpr();

    /** For star nodes(`*`), expand them to a list of all columns.
      * For literal nodes, substitute aliases.
      */
    void normalizeTree();

    ///    Eliminates injective function calls and constant expressions from group by statement
    void optimizeGroupBy();

    /// Remove duplicate items from ORDER BY.
    void optimizeOrderBy();

    void optimizeLimitBy();

    /// Remove duplicated columns from USING(...).
    void optimizeUsing();

    /// remove Function_if AST if condition is constant
    void optimizeIfWithConstantCondition();
    void optimizeIfWithConstantConditionImpl(ASTPtr & current_ast);
    bool tryExtractConstValueFromCondition(const ASTPtr & condition, bool & value) const;

    /// Adds a list of ALIAS columns from the table.
    void addAliasColumns();

    /// Replacing scalar subqueries with constant values.
    void executeScalarSubqueries();

    /// Find global subqueries in the GLOBAL IN/JOIN sections. Fills in external_tables.
    void initGlobalSubqueriesAndExternalTables();

    /** Initialize InterpreterSelectQuery for a subquery in the GLOBAL IN/JOIN section,
      * create a temporary table of type Memory and store it in the external_tables dictionary.
      */
    void addExternalStorage(ASTPtr & subquery_or_table_name);

    void getArrayJoinedColumns();
    void addMultipleArrayJoinAction(ExpressionActionsPtr & actions) const;

    void addJoinAction(ExpressionActionsPtr & actions, bool only_types) const;

    bool isThereArrayJoin(const ASTPtr & ast);

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
      * Create Set from a subuqery or a table expression in the query. The created set is suitable for using the index.
      * The set will not be created if its size hits the limit.
      */
    void tryMakeSetForIndexFromSubquery(const ASTPtr & subquery_or_table_name);

    void makeSetsForIndexImpl(const ASTPtr & node, const Block & sample_block);

    /** Translate qualified names such as db.table.column, table.column, table_alias.column
      *  to unqualified names. This is done in a poor transitional way:
      *  only one ("main") table is supported. Ambiguity is not detected or resolved.
      */
    void translateQualifiedNames();

    /** Sometimes we have to calculate more columns in SELECT clause than will be returned from query.
      * This is the case when we have DISTINCT or arrayJoin: we require more columns in SELECT even if we need less columns in result.
      */
    void removeUnneededColumnsFromSelectClause();

    bool isRemoteStorage() const;
};

}
