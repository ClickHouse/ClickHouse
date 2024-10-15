#pragma once

#include <Core/ColumnNumbers.h>
#include <Columns/FilterDescription.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/WindowDescription.h>
#include <Interpreters/JoinUtils.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class Block;
struct Settings;

struct ExpressionActionsChain;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
using ManyExpressionActions = std::vector<ExpressionActionsPtr>;

struct ASTTableJoin;
class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class ASTFunction;
class ASTExpressionList;
class ASTSelectQuery;
struct ASTTablesInSelectQueryElement;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

/// Create columns in block or return false if not possible
bool sanitizeBlock(Block & block, bool throw_if_cannot_create_column = false);

/// ExpressionAnalyzer sources, intermediates and results. It splits data and logic, allows to test them separately.
struct ExpressionAnalyzerData
{
    ~ExpressionAnalyzerData();

    PreparedSetsPtr prepared_sets;

    std::unique_ptr<QueryPlan> joined_plan;

    /// Columns after ARRAY JOIN. If there is no ARRAY JOIN, it's source_columns.
    NamesAndTypesList columns_after_array_join;
    /// Columns after Columns after ARRAY JOIN and JOIN. If there is no JOIN, it's columns_after_array_join.
    NamesAndTypesList columns_after_join;
    /// Columns after ARRAY JOIN, JOIN, and/or aggregation.
    NamesAndTypesList aggregated_columns;
    /// Columns after window functions.
    NamesAndTypesList columns_after_window;
    /// Keys of ORDER BY
    NameSet order_by_keys;

    bool has_aggregation = false;
    NamesAndTypesList aggregation_keys;
    NamesAndTypesLists aggregation_keys_list;
    ColumnNumbersList aggregation_keys_indexes_list;
    bool has_const_aggregation_keys = false;
    AggregateDescriptions aggregate_descriptions;

    WindowDescriptions window_descriptions;
    NamesAndTypesList window_columns;

    bool has_global_subqueries = false;

    /// All new temporary tables obtained by performing the GLOBAL IN/JOIN subqueries.
    TemporaryTablesMapping external_tables;

    GroupByKind group_by_kind = GroupByKind::NONE;
};


/** Transforms an expression from a syntax tree into a sequence of actions to execute it.
  *
  * NOTE: if `ast` is a SELECT query from a table, the structure of this table should not change during the lifetime of ExpressionAnalyzer.
  */
class ExpressionAnalyzer : protected ExpressionAnalyzerData, private boost::noncopyable, protected WithContext
{
private:
    /// Extracts settings to enlight which are used (and avoid copy of others).
    struct ExtractedSettings
    {
        const SizeLimits size_limits_for_set;
        const SizeLimits size_limits_for_set_used_with_index;
        const UInt64 distributed_group_by_no_merge;

        explicit ExtractedSettings(const Settings & settings_);
    };

public:
    /// Ctor for non-select queries. Generally its usage is:
    /// auto actions = ExpressionAnalyzer(query, syntax, context).getActions();
    ExpressionAnalyzer(const ASTPtr & query_, const TreeRewriterResultPtr & syntax_analyzer_result_, ContextPtr context_)
        : ExpressionAnalyzer(query_, syntax_analyzer_result_, context_, 0, false, false, {})
    {
    }

    ~ExpressionAnalyzer();

    void appendExpression(ExpressionActionsChain & chain, const ASTPtr & expr, bool only_types);

    /// If `ast` is not a SELECT query, just gets all the actions to evaluate the expression.
    /// If add_aliases, only the calculated values in the desired order and add aliases.
    ///     If also remove_unused_result, than only aliases remain in the output block.
    /// Otherwise, only temporary columns will be deleted from the block.
    ActionsDAG getActionsDAG(bool add_aliases, bool remove_unused_result = true);
    ExpressionActionsPtr getActions(bool add_aliases, bool remove_unused_result = true, CompileExpressions compile_expressions = CompileExpressions::no);

    /// Get actions to evaluate a constant expression. The function adds constants and applies functions that depend only on constants.
    /// Does not execute subqueries.
    ActionsDAG getConstActionsDAG(const ColumnsWithTypeAndName & constant_inputs = {});
    ExpressionActionsPtr getConstActions(const ColumnsWithTypeAndName & constant_inputs = {});

    /** Sets that require a subquery to be create.
      * Only the sets needed to perform actions returned from already executed `append*` or `getActions`.
      * That is, you need to call getSetsWithSubqueries after all calls of `append*` or `getActions`
      *  and create all the returned sets before performing the actions.
      */
    PreparedSetsPtr getPreparedSets() { return prepared_sets; }

    /// Get intermediates for tests
    const ExpressionAnalyzerData & getAnalyzedData() const { return *this; }

    /// A list of windows for window functions.
    const WindowDescriptions & windowDescriptions() const { return window_descriptions; }

    void makeWindowDescriptionFromAST(
        const Context & context,
        const WindowDescriptions & existing_descriptions,
        AggregateFunctionPtr aggregate_function,
        WindowDescription & desc,
        const IAST * ast);
    void makeWindowDescriptions(ActionsDAG & actions);

    /** Checks if subquery is not a plain StorageSet.
      * Because while making set we will read data from StorageSet which is not allowed.
      * Returns valid SetPtr from StorageSet if the latter is used after IN or nullptr otherwise.
      */
    SetPtr isPlainStorageSetInSubquery(const ASTPtr & subquery_or_table_name);

protected:
    ExpressionAnalyzer(
        const ASTPtr & query_,
        const TreeRewriterResultPtr & syntax_analyzer_result_,
        ContextPtr context_,
        size_t subquery_depth_,
        bool do_global_,
        bool is_explain_,
        PreparedSetsPtr prepared_sets_,
        bool is_create_parameterized_view_ = false);

    ASTPtr query;
    const ExtractedSettings settings;
    size_t subquery_depth;

    TreeRewriterResultPtr syntax;
    bool is_create_parameterized_view;

    const ConstStoragePtr & storage() const { return syntax->storage; } /// The main table in FROM clause, if exists.
    const TableJoin & analyzedJoin() const { return *syntax->analyzed_join; }
    const NamesAndTypesList & sourceColumns() const { return syntax->required_source_columns; }
    const ASTs & aggregates() const { return syntax->aggregates; }
    /// Find global subqueries in the GLOBAL IN/JOIN sections. Fills in external_tables.
    void initGlobalSubqueriesAndExternalTables(bool do_global, bool is_explain);

    ArrayJoin addMultipleArrayJoinAction(ActionsDAG & actions, bool is_left) const;

    void getRootActions(const ASTPtr & ast, bool no_makeset_for_subqueries, ActionsDAG & actions, bool only_consts = false);

    /** Similar to getRootActions but do not make sets when analyzing IN functions. It's used in
      * analyzeAggregation which happens earlier than analyzing PREWHERE and WHERE. If we did, the
      * prepared sets would not be applicable for MergeTree index optimization.
      */
    void getRootActionsNoMakeSet(const ASTPtr & ast, ActionsDAG & actions, bool only_consts = false);

    void getRootActionsForHaving(const ASTPtr & ast, bool no_makeset_for_subqueries, ActionsDAG & actions, bool only_consts = false);

    void getRootActionsForWindowFunctions(const ASTPtr & ast, bool no_makeset_for_subqueries, ActionsDAG & actions);

    /** Add aggregation keys to aggregation_keys, aggregate functions to aggregate_descriptions,
      * Create a set of columns aggregated_columns resulting after the aggregation, if any,
      *  or after all the actions that are normally performed before aggregation.
      * Set has_aggregation = true if there is GROUP BY or at least one aggregate function.
      */
    void analyzeAggregation(ActionsDAG & temp_actions);
    void makeAggregateDescriptions(ActionsDAG & actions, AggregateDescriptions & descriptions);

    const ASTSelectQuery * getSelectQuery() const;

    bool isRemoteStorage() const;

    NamesAndTypesList getColumnsAfterArrayJoin(ActionsDAG & actions, const NamesAndTypesList & src_columns);
    NamesAndTypesList analyzeJoin(ActionsDAG & actions, const NamesAndTypesList & src_columns);

    AggregationKeysInfo getAggregationKeysInfo() const noexcept
    {
      return { aggregation_keys, aggregation_keys_indexes_list, group_by_kind };
    }
};

class SelectQueryExpressionAnalyzer;

/// Result of SelectQueryExpressionAnalyzer: expressions for InterpreterSelectQuery
struct ExpressionAnalysisResult
{
    std::string dump() const;

    /// Do I need to perform the first part of the pipeline - running on remote servers during distributed processing.
    bool first_stage = false;
    /// Do I need to execute the second part of the pipeline - running on the initiating server during distributed processing.
    bool second_stage = false;

    bool need_aggregate = false;
    bool has_order_by   = false;
    bool has_window = false;

    String where_column_name;
    bool remove_where_filter = false;
    bool optimize_read_in_order = false;
    bool optimize_aggregation_in_order = false;
    bool join_has_delayed_stream = false;

    bool use_grouping_set_key = false;

    ActionsAndProjectInputsFlagPtr before_array_join;
    std::optional<ArrayJoin> array_join;
    ActionsAndProjectInputsFlagPtr before_join;
    ActionsAndProjectInputsFlagPtr converting_join_columns;
    JoinPtr join;
    ActionsAndProjectInputsFlagPtr before_where;
    ActionsAndProjectInputsFlagPtr before_aggregation;
    ActionsAndProjectInputsFlagPtr before_having;
    String having_column_name;
    bool remove_having_filter = false;
    ActionsAndProjectInputsFlagPtr before_window;
    ActionsAndProjectInputsFlagPtr before_order_by;
    ActionsAndProjectInputsFlagPtr before_limit_by;
    ActionsAndProjectInputsFlagPtr final_projection;

    /// Columns from the SELECT list, before renaming them to aliases. Used to
    /// perform SELECT DISTINCT.
    Names selected_columns;

    /// Columns to read from storage if any.
    Names required_columns;

    /// Columns will be removed after prewhere actions execution.
    NameSet columns_to_remove_after_prewhere;

    PrewhereInfoPtr prewhere_info;
    FilterDAGInfoPtr filter_info;
    ConstantFilterDescription prewhere_constant_filter_description;
    ConstantFilterDescription where_constant_filter_description;
    /// Actions by every element of ORDER BY
    ManyExpressionActions order_by_elements_actions;
    ManyExpressionActions group_by_elements_actions;

    ExpressionAnalysisResult() = default;

    ExpressionAnalysisResult(
        SelectQueryExpressionAnalyzer & query_analyzer,
        const StorageMetadataPtr & metadata_snapshot,
        bool first_stage,
        bool second_stage,
        bool only_types,
        const FilterDAGInfoPtr & filter_info,
        const FilterDAGInfoPtr & additional_filter, /// for setting additional_filters
        const Block & source_header);

    /// Filter for row-level security.
    bool hasFilter() const { return filter_info.get(); }

    bool hasJoin() const { return join.get(); }
    bool hasPrewhere() const { return prewhere_info.get(); }
    bool hasWhere() const { return before_where.get(); }
    bool hasHaving() const { return before_having.get(); }
    bool hasLimitBy() const { return before_limit_by.get(); }

    void removeExtraColumns() const;
    void checkActions() const;
    void finalize(
        const ExpressionActionsChain & chain,
        ssize_t & prewhere_step_num,
        ssize_t & where_step_num,
        ssize_t & having_step_num,
        const ASTSelectQuery & query);
};

/// SelectQuery specific ExpressionAnalyzer part.
class SelectQueryExpressionAnalyzer : public ExpressionAnalyzer
{
public:
    friend struct ExpressionAnalysisResult;

    SelectQueryExpressionAnalyzer(
        const ASTPtr & query_,
        const TreeRewriterResultPtr & syntax_analyzer_result_,
        ContextPtr context_,
        const StorageMetadataPtr & metadata_snapshot_,
        const Names & required_result_columns_ = {},
        bool do_global_ = false,
        const SelectQueryOptions & options_ = {},
        PreparedSetsPtr prepared_sets_ = nullptr)
        : ExpressionAnalyzer(
            query_,
            syntax_analyzer_result_,
            context_,
            options_.subquery_depth,
            do_global_,
            options_.is_explain,
            prepared_sets_,
            options_.is_create_parameterized_view)
        , metadata_snapshot(metadata_snapshot_)
        , required_result_columns(required_result_columns_)
        , query_options(options_)
    {
    }

    /// Does the expression have aggregate functions or a GROUP BY or HAVING section.
    bool hasAggregation() const { return has_aggregation; }
    bool hasWindow() const { return !syntax->window_function_asts.empty(); }
    bool hasGlobalSubqueries() { return has_global_subqueries; }
    bool hasTableJoin() const { return syntax->ast_join; }

    /// When there is only one group in GROUPING SETS
    /// it is a special case that is equal to GROUP BY, i.e.:
    ///
    ///     GROUPING SETS ((a, b)) -> GROUP BY a, b
    ///
    /// But it is rewritten by GroupingSetsRewriterVisitor to GROUP BY,
    /// so instead of aggregation_keys_list.size() > 1,
    /// !aggregation_keys_list.empty() can be used.
    bool useGroupingSetKey() const { return !aggregation_keys_list.empty(); }

    const NamesAndTypesList & aggregationKeys() const { return aggregation_keys; }
    bool hasConstAggregationKeys() const { return has_const_aggregation_keys; }
    const NamesAndTypesLists & aggregationKeysList() const { return aggregation_keys_list; }
    const AggregateDescriptions & aggregates() const { return aggregate_descriptions; }

    std::unique_ptr<QueryPlan> getJoinedPlan();

    /// Tables that will need to be sent to remote servers for distributed query processing.
    const TemporaryTablesMapping & getExternalTables() const { return external_tables; }

    ActionsAndProjectInputsFlagPtr simpleSelectActions();

    /// These appends are public only for tests
    void appendSelect(ExpressionActionsChain & chain, bool only_types);
    /// Deletes all columns except mentioned by SELECT, arranges the remaining columns and renames them to aliases.
    ActionsAndProjectInputsFlagPtr appendProjectResult(ExpressionActionsChain & chain) const;

private:
    StorageMetadataPtr metadata_snapshot;
    /// If non-empty, ignore all expressions not from this list.
    Names required_result_columns;
    SelectQueryOptions query_options;

    JoinPtr makeJoin(
        const ASTTablesInSelectQueryElement & join_element,
        const ColumnsWithTypeAndName & left_columns,
        std::optional<ActionsDAG> & left_convert_actions);

    const ASTSelectQuery * getAggregatingQuery() const;

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
    std::optional<ArrayJoin> appendArrayJoin(ExpressionActionsChain & chain, ActionsAndProjectInputsFlagPtr & before_array_join, bool only_types);
    bool appendJoinLeftKeys(ExpressionActionsChain & chain, bool only_types);
    JoinPtr appendJoin(ExpressionActionsChain & chain, ActionsAndProjectInputsFlagPtr & converting_join_columns);

    /// remove_filter is set in ExpressionActionsChain::finalize();
    /// Columns in `additional_required_columns` will not be removed (they can be used for e.g. sampling or FINAL modifier).
    ActionsAndProjectInputsFlagPtr appendPrewhere(ExpressionActionsChain & chain, bool only_types);
    bool appendWhere(ExpressionActionsChain & chain, bool only_types);
    bool appendGroupBy(ExpressionActionsChain & chain, bool only_types, bool optimize_aggregation_in_order, ManyExpressionActions &);
    void appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types);
    void appendWindowFunctionsArguments(ExpressionActionsChain & chain, bool only_types);

    void appendExpressionsAfterWindowFunctions(ExpressionActionsChain & chain, bool only_types);
    void appendSelectSkipWindowExpressions(ExpressionActionsChain::Step & step, ASTPtr const & node);

    void appendGroupByModifiers(ActionsDAG & before_aggregation, ExpressionActionsChain & chain, bool only_types);

    /// After aggregation:
    bool appendHaving(ExpressionActionsChain & chain, bool only_types);
    ///  appendSelect
    ActionsAndProjectInputsFlagPtr appendOrderBy(ExpressionActionsChain & chain, bool only_types, bool optimize_read_in_order, ManyExpressionActions &);
    bool appendLimitBy(ExpressionActionsChain & chain, bool only_types);
    ///  appendProjectResult
};

}
