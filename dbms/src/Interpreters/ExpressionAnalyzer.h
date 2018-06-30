#pragma once

#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Settings.h>
#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ProjectionManipulation.h>
#include <Parsers/StringRange.h>

namespace DB
{

class Context;

class ExpressionActions;
struct ExpressionActionsChain;

class Join;
using JoinPtr = std::shared_ptr<Join>;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class Set;
using SetPtr = std::shared_ptr<Set>;
/// Will compare sets by their position in query string. It's possible because IAST::clone() doesn't chane IAST::range.
/// It should be taken into account when we want to change AST part which contains sets.
using PreparedSets = std::unordered_map<StringRange, SetPtr, StringRangePointersHash, StringRangePointersEqualTo>;

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;
using Tables = std::map<String, StoragePtr>;

class ASTFunction;
class ASTExpressionList;
class ASTSelectQuery;

struct ProjectionManipulatorBase;
using ProjectionManipulatorPtr = std::shared_ptr<ProjectionManipulatorBase>;

/** Information on what to do when executing a subquery in the [GLOBAL] IN/JOIN section.
  */
struct SubqueryForSet
{
    /// The source is obtained using the InterpreterSelectQuery subquery.
    BlockInputStreamPtr source;

    /// If set, build it from result.
    SetPtr set;
    JoinPtr join;

    /// If set, put the result into the table.
    /// This is a temporary table for transferring to remote servers for distributed query processing.
    StoragePtr table;
};

/// ID of subquery -> what to do with it.
using SubqueriesForSets = std::unordered_map<String, SubqueryForSet>;

struct ScopeStack
{
    struct Level
    {
        ExpressionActionsPtr actions;
        NameSet new_columns;
    };

    using Levels = std::vector<Level>;

    Levels stack;
    Settings settings;

    ScopeStack(const ExpressionActionsPtr & actions, const Settings & settings_);

    void pushLevel(const NamesAndTypesList & input_columns);

    size_t getColumnLevel(const std::string & name);

    void addAction(const ExpressionAction & action);

    ExpressionActionsPtr popLevel();

    const Block & getSampleBlock() const;
};

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
        const ASTPtr & ast_,
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

    /// If `ast` is not a SELECT query, just gets all the actions to evaluate the expression.
    /// If project_result, only the calculated values in the desired order, renamed to aliases, remain in the output block.
    /// Otherwise, only temporary columns will be deleted from the block.
    ExpressionActionsPtr getActions(bool project_result);

    /// Actions that can be performed on an empty block: adding constants and applying functions that depend only on constants.
    /// Does not execute subqueries.
    ExpressionActionsPtr getConstActions();

    /** Sets that require a subquery to be create.
      * Only the sets needed to perform actions returned from already executed `append*` or `getActions`.
      * That is, you need to call getSetsWithSubqueries after all calls of `append*` or `getActions`
      *  and create all the returned sets before performing the actions.
      */
    SubqueriesForSets getSubqueriesForSets() const { return subqueries_for_sets; }

    PreparedSets getPreparedSets() { return prepared_sets; }

    /** Tables that will need to be sent to remote servers for distributed query processing.
      */
    const Tables & getExternalTables() const { return external_tables; }

    /// Create Set-s that we can from IN section to use the index on them.
    void makeSetsForIndex();


private:
    ASTPtr ast;
    ASTSelectQuery * select_query;
    const Context & context;
    Settings settings;
    size_t subquery_depth;

    /** Original columns.
      * First, all available columns of the table are placed here. Then (when analyzing the query), unused columns are deleted.
      */
    NamesAndTypesList source_columns;

    /** If non-empty, ignore all expressions in  not from this list.
      */
    NameSet required_result_columns;

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

    SubqueriesForSets subqueries_for_sets;

    PreparedSets prepared_sets;

    /// NOTE: So far, only one JOIN per query is supported.

    /** Query of the form `SELECT expr(x) AS FROM t1 ANY LEFT JOIN (SELECT expr(x) AS k FROM t2) USING k`
      * The join is made by column k.
      * During the JOIN,
      *  - in the "right" table, it will be available by alias `k`, since `Project` action for the subquery was executed.
      *  - in the "left" table, it will be accessible by the name `expr(x)`, since `Project` action has not been executed yet.
      * You must remember both of these options.
      */
    Names join_key_names_left;
    Names join_key_names_right;

    NamesAndTypesList columns_added_by_join;

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
    size_t external_table_id = 1;

    /** Remove all unnecessary columns from the list of all available columns of the table (`columns`).
      * At the same time, form a set of unknown columns (`unknown_required_source_columns`),
      * as well as the columns added by JOIN (`columns_added_by_join`).
      */
    void collectUsedColumns();

    /** Find the columns that are obtained by JOIN.
      */
    void collectJoinedColumns(NameSet & joined_columns, NamesAndTypesList & joined_columns_name_type);

    /** Create a dictionary of aliases.
      */
    void addASTAliases(ASTPtr & ast, int ignore_levels = 0);

    /** For star nodes(`*`), expand them to a list of all columns.
      * For literal nodes, substitute aliases.
      */
    void normalizeTree();
    void normalizeTreeImpl(ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts, std::string current_alias, size_t level);

    ///    Eliminates injective function calls and constant expressions from group by statement
    void optimizeGroupBy();

    /// Remove duplicate items from ORDER BY.
    void optimizeOrderBy();

    void optimizeLimitBy();

    /// remove Function_if AST if condition is constant
    void optimizeIfWithConstantCondition();
    void optimizeIfWithConstantConditionImpl(ASTPtr & current_ast, Aliases & aliases) const;
    bool tryExtractConstValueFromCondition(const ASTPtr & condition, bool & value) const;

    void makeSet(const ASTFunction * node, const Block & sample_block);

    /// Adds a list of ALIAS columns from the table.
    void addAliasColumns();

    /// Replacing scalar subqueries with constant values.
    void executeScalarSubqueries();
    void executeScalarSubqueriesImpl(ASTPtr & ast);

    /// Find global subqueries in the GLOBAL IN/JOIN sections. Fills in external_tables.
    void initGlobalSubqueriesAndExternalTables();
    void initGlobalSubqueries(ASTPtr & ast);

    /// Finds in the query the usage of external tables (as table identifiers). Fills in external_tables.
    void findExternalTables(ASTPtr & ast);

    /** Initialize InterpreterSelectQuery for a subquery in the GLOBAL IN/JOIN section,
      * create a temporary table of type Memory and store it in the external_tables dictionary.
      */
    void addExternalStorage(ASTPtr & subquery_or_table_name);

    void getArrayJoinedColumns();
    void getArrayJoinedColumnsImpl(const ASTPtr & ast);
    void addMultipleArrayJoinAction(ExpressionActionsPtr & actions) const;

    void addJoinAction(ExpressionActionsPtr & actions, bool only_types) const;

    bool isThereArrayJoin(const ASTPtr & ast);

    void getActionsImpl(const ASTPtr & ast, bool no_subqueries, bool only_consts, ScopeStack & actions_stack,
                        ProjectionManipulatorPtr projection_manipulator);

    void getRootActions(const ASTPtr & ast, bool no_subqueries, bool only_consts, ExpressionActionsPtr & actions);

    void getActionsBeforeAggregation(const ASTPtr & ast, ExpressionActionsPtr & actions, bool no_subqueries);

    /** Add aggregation keys to aggregation_keys, aggregate functions to aggregate_descriptions,
      * Create a set of columns aggregated_columns resulting after the aggregation, if any,
      *  or after all the actions that are normally performed before aggregation.
      * Set has_aggregation = true if there is GROUP BY or at least one aggregate function.
      */
    void analyzeAggregation();
    void getAggregates(const ASTPtr & ast, ExpressionActionsPtr & actions);
    void assertNoAggregates(const ASTPtr & ast, const char * description);

    /** Get a set of necessary columns to read from the table.
      * In this case, the columns specified in ignored_names are considered unnecessary. And the ignored_names parameter can be modified.
      * The set of columns available_joined_columns are the columns available from JOIN, they are not needed for reading from the main table.
      * Put in required_joined_columns the set of columns available from JOIN and needed.
      */
    void getRequiredSourceColumnsImpl(const ASTPtr & ast,
        const NameSet & available_columns, NameSet & required_source_columns, NameSet & ignored_names,
        const NameSet & available_joined_columns, NameSet & required_joined_columns);

    /// columns - the columns that are present before the transformations begin.
    void initChain(ExpressionActionsChain & chain, const NamesAndTypesList & columns) const;

    void assertSelect() const;
    void assertAggregation() const;

    /** Create Set from an explicit enumeration of values in the query.
      * If create_ordered_set = true - create a data structure suitable for using the index.
      */
    void makeExplicitSet(const ASTFunction * node, const Block & sample_block, bool create_ordered_set);

    /**
      * Create Set from a subuqery or a table expression in the query. The created set is suitable for using the index.
      * The set will not be created if its size hits the limit.
      */
    void tryMakeSetFromSubquery(const ASTPtr & subquery_or_table_name);

    void makeSetsForIndexImpl(const ASTPtr & node, const Block & sample_block);

    /** Translate qualified names such as db.table.column, table.column, table_alias.column
      *  to unqualified names. This is done in a poor transitional way:
      *  only one ("main") table is supported. Ambiguity is not detected or resolved.
      */
    void translateQualifiedNames();
    void translateQualifiedNamesImpl(ASTPtr & node, const String & database_name, const String & table_name, const String & alias);

    /** Sometimes we have to calculate more columns in SELECT clause than will be returned from query.
      * This is the case when we have DISTINCT or arrayJoin: we require more columns in SELECT even if we need less columns in result.
      */
    void removeUnneededColumnsFromSelectClause();
};

}
