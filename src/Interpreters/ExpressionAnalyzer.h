#pragma once

#include <Core/Settings.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Columns/FilterDescription.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/SubqueryForSet.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/DatabaseCatalog.h>

namespace DB
{

class Block;
class Context;

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

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

/// Create columns in block or return false if not possible
bool sanitizeBlock(Block & block, bool throw_if_cannot_create_column = false);

/// ExpressionAnalyzer sources, intermediates and results. It splits data and logic, allows to test them separately.
struct ExpressionAnalyzerData
{
    SubqueriesForSets subqueries_for_sets;
    PreparedSets prepared_sets;

    /// Columns after ARRAY JOIN. If there is no ARRAY JOIN, it's source_columns.
    NamesAndTypesList columns_after_array_join;
    /// Columns after Columns after ARRAY JOIN and JOIN. If there is no JOIN, it's columns_after_array_join.
    NamesAndTypesList columns_after_join;
    /// Columns after ARRAY JOIN, JOIN, and/or aggregation.
    NamesAndTypesList aggregated_columns;

    bool has_aggregation = false;
    NamesAndTypesList aggregation_keys;
    AggregateDescriptions aggregate_descriptions;

    bool has_global_subqueries = false;

    /// All new temporary tables obtained by performing the GLOBAL IN/JOIN subqueries.
    TemporaryTablesMapping external_tables;
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
        const SizeLimits size_limits_for_set;

        ExtractedSettings(const Settings & settings_)
        :   use_index_for_in_with_subqueries(settings_.use_index_for_in_with_subqueries),
            size_limits_for_set(settings_.max_rows_in_set, settings_.max_bytes_in_set, settings_.set_overflow_mode)
        {}
    };

public:
    /// Ctor for non-select queries. Generally its usage is:
    /// auto actions = ExpressionAnalyzer(query, syntax, context).getActions();
    ExpressionAnalyzer(
        const ASTPtr & query_,
        const TreeRewriterResultPtr & syntax_analyzer_result_,
        const Context & context_)
    :   ExpressionAnalyzer(query_, syntax_analyzer_result_, context_, 0, false, {})
    {}

    void appendExpression(ExpressionActionsChain & chain, const ASTPtr & expr, bool only_types);

    /// If `ast` is not a SELECT query, just gets all the actions to evaluate the expression.
    /// If add_aliases, only the calculated values in the desired order and add aliases.
    ///     If also project_result, than only aliases remain in the output block.
    /// Otherwise, only temporary columns will be deleted from the block.
    ActionsDAGPtr getActionsDAG(bool add_aliases, bool project_result = true);
    ExpressionActionsPtr getActions(bool add_aliases, bool project_result = true);

    /// Actions that can be performed on an empty block: adding constants and applying functions that depend only on constants.
    /// Does not execute subqueries.
    ExpressionActionsPtr getConstActions();

    /** Sets that require a subquery to be create.
      * Only the sets needed to perform actions returned from already executed `append*` or `getActions`.
      * That is, you need to call getSetsWithSubqueries after all calls of `append*` or `getActions`
      *  and create all the returned sets before performing the actions.
      */
    SubqueriesForSets & getSubqueriesForSets() { return subqueries_for_sets; }

    /// Get intermediates for tests
    const ExpressionAnalyzerData & getAnalyzedData() const { return *this; }

protected:
    ExpressionAnalyzer(
        const ASTPtr & query_,
        const TreeRewriterResultPtr & syntax_analyzer_result_,
        const Context & context_,
        size_t subquery_depth_,
        bool do_global_,
        SubqueriesForSets subqueries_for_sets_);

    ASTPtr query;
    const Context & context;
    const ExtractedSettings settings;
    size_t subquery_depth;

    TreeRewriterResultPtr syntax;

    const ConstStoragePtr & storage() const { return syntax->storage; } /// The main table in FROM clause, if exists.
    const TableJoin & analyzedJoin() const { return *syntax->analyzed_join; }
    const NamesAndTypesList & sourceColumns() const { return syntax->required_source_columns; }
    const std::vector<const ASTFunction *> & aggregates() const { return syntax->aggregates; }
    /// Find global subqueries in the GLOBAL IN/JOIN sections. Fills in external_tables.
    void initGlobalSubqueriesAndExternalTables(bool do_global);

    ArrayJoinActionPtr addMultipleArrayJoinAction(ActionsDAGPtr & actions, bool is_left) const;

    void getRootActions(const ASTPtr & ast, bool no_subqueries, ActionsDAGPtr & actions, bool only_consts = false);

    /** Similar to getRootActions but do not make sets when analyzing IN functions. It's used in
      * analyzeAggregation which happens earlier than analyzing PREWHERE and WHERE. If we did, the
      * prepared sets would not be applicable for MergeTree index optimization.
      */
    void getRootActionsNoMakeSet(const ASTPtr & ast, bool no_subqueries, ActionsDAGPtr & actions, bool only_consts = false);

    void getRootActionsForHaving(const ASTPtr & ast, bool no_subqueries, ActionsDAGPtr & actions, bool only_consts = false);

    /** Add aggregation keys to aggregation_keys, aggregate functions to aggregate_descriptions,
      * Create a set of columns aggregated_columns resulting after the aggregation, if any,
      *  or after all the actions that are normally performed before aggregation.
      * Set has_aggregation = true if there is GROUP BY or at least one aggregate function.
      */
    void analyzeAggregation();
    bool makeAggregateDescriptions(ActionsDAGPtr & actions);

    const ASTSelectQuery * getSelectQuery() const;

    bool isRemoteStorage() const { return syntax->is_remote_storage; }
};

class SelectQueryExpressionAnalyzer;

/// Result of SelectQueryExpressionAnalyzer: expressions for InterpreterSelectQuery
struct ExpressionAnalysisResult
{
    /// Do I need to perform the first part of the pipeline - running on remote servers during distributed processing.
    bool first_stage = false;
    /// Do I need to execute the second part of the pipeline - running on the initiating server during distributed processing.
    bool second_stage = false;

    bool need_aggregate = false;
    bool has_order_by   = false;

    bool remove_where_filter = false;
    bool optimize_read_in_order = false;
    bool optimize_aggregation_in_order = false;
    bool join_has_delayed_stream = false;

    ActionsDAGPtr before_array_join;
    ArrayJoinActionPtr array_join;
    ActionsDAGPtr before_join;
    JoinPtr join;
    ActionsDAGPtr before_where;
    ActionsDAGPtr before_aggregation;
    ActionsDAGPtr before_having;
    ActionsDAGPtr before_order_and_select;
    ActionsDAGPtr before_limit_by;
    ActionsDAGPtr final_projection;

    /// Columns from the SELECT list, before renaming them to aliases.
    Names selected_columns;

    /// Columns will be removed after prewhere actions execution.
    NameSet columns_to_remove_after_prewhere;

    PrewhereDAGInfoPtr prewhere_info;
    FilterInfoPtr filter_info;
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
        const FilterInfoPtr & filter_info,
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
    void finalize(const ExpressionActionsChain & chain, size_t where_step_num);
};

/// SelectQuery specific ExpressionAnalyzer part.
class SelectQueryExpressionAnalyzer : public ExpressionAnalyzer
{
public:
    friend struct ExpressionAnalysisResult;

    SelectQueryExpressionAnalyzer(
        const ASTPtr & query_,
        const TreeRewriterResultPtr & syntax_analyzer_result_,
        const Context & context_,
        const StorageMetadataPtr & metadata_snapshot_,
        const NameSet & required_result_columns_ = {},
        bool do_global_ = false,
        const SelectQueryOptions & options_ = {},
        SubqueriesForSets subqueries_for_sets_ = {})
        : ExpressionAnalyzer(query_, syntax_analyzer_result_, context_, options_.subquery_depth, do_global_, std::move(subqueries_for_sets_))
        , metadata_snapshot(metadata_snapshot_)
        , required_result_columns(required_result_columns_)
        , query_options(options_)
    {
    }

    /// Does the expression have aggregate functions or a GROUP BY or HAVING section.
    bool hasAggregation() const { return has_aggregation; }
    bool hasGlobalSubqueries() { return has_global_subqueries; }
    bool hasTableJoin() const { return syntax->ast_join; }

    const NamesAndTypesList & aggregationKeys() const { return aggregation_keys; }
    const AggregateDescriptions & aggregates() const { return aggregate_descriptions; }

    const PreparedSets & getPreparedSets() const { return prepared_sets; }

    /// Tables that will need to be sent to remote servers for distributed query processing.
    const TemporaryTablesMapping & getExternalTables() const { return external_tables; }

    ActionsDAGPtr simpleSelectActions();

    /// These appends are public only for tests
    void appendSelect(ExpressionActionsChain & chain, bool only_types);
    /// Deletes all columns except mentioned by SELECT, arranges the remaining columns and renames them to aliases.
    ActionsDAGPtr appendProjectResult(ExpressionActionsChain & chain) const;

private:
    StorageMetadataPtr metadata_snapshot;
    /// If non-empty, ignore all expressions not from this list.
    NameSet required_result_columns;
    SelectQueryOptions query_options;

    /**
      * Create Set from a subquery or a table expression in the query. The created set is suitable for using the index.
      * The set will not be created if its size hits the limit.
      */
    void tryMakeSetForIndexFromSubquery(const ASTPtr & subquery_or_table_name);

    /**
      * Checks if subquery is not a plain StorageSet.
      * Because while making set we will read data from StorageSet which is not allowed.
      * Returns valid SetPtr from StorageSet if the latter is used after IN or nullptr otherwise.
      */
    SetPtr isPlainStorageSetInSubquery(const ASTPtr & subquery_or_table_name);

    /// Create Set-s that we make from IN section to use index on them.
    void makeSetsForIndex(const ASTPtr & node);

    JoinPtr makeTableJoin(const ASTTablesInSelectQueryElement & join_element);

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
    ArrayJoinActionPtr appendArrayJoin(ExpressionActionsChain & chain, ActionsDAGPtr & before_array_join, bool only_types);
    bool appendJoinLeftKeys(ExpressionActionsChain & chain, bool only_types);
    JoinPtr appendJoin(ExpressionActionsChain & chain);
    /// Add preliminary rows filtration. Actions are created in other expression analyzer to prevent any possible alias injection.
    void appendPreliminaryFilter(ExpressionActionsChain & chain, ActionsDAGPtr actions_dag, String column_name);
    /// remove_filter is set in ExpressionActionsChain::finalize();
    /// Columns in `additional_required_columns` will not be removed (they can be used for e.g. sampling or FINAL modifier).
    ActionsDAGPtr appendPrewhere(ExpressionActionsChain & chain, bool only_types, const Names & additional_required_columns);
    bool appendWhere(ExpressionActionsChain & chain, bool only_types);
    bool appendGroupBy(ExpressionActionsChain & chain, bool only_types, bool optimize_aggregation_in_order, ManyExpressionActions &);
    void appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types);

    /// After aggregation:
    bool appendHaving(ExpressionActionsChain & chain, bool only_types);
    ///  appendSelect
    ActionsDAGPtr appendOrderBy(ExpressionActionsChain & chain, bool only_types, bool optimize_read_in_order, ManyExpressionActions &);
    bool appendLimitBy(ExpressionActionsChain & chain, bool only_types);
    ///  appendProjectResult
};

}
