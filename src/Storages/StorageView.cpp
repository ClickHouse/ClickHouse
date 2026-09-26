#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/DefinerDependencies.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Storages/AlterCommands.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageView.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageFactory.h>
#include <Storages/SelectQueryDescription.h>

#include <Common/CurrentThread.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Common/typeid_cast.h>

#include <Core/ServerSettings.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Core/SettingsFields.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Parsers/QueryParameterVisitor.h>
#include <Storages/StorageWithCommonVirtualColumns.h>

#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Planner/findQueryForParallelReplicas.h>
#include <Poco/String.h>

#include <algorithm>

namespace DB
{
namespace Setting
{
    extern const SettingsString additional_result_filter;
    extern const SettingsMap additional_table_filters;
    extern const SettingsSetOperationMode except_default_mode;
    extern const SettingsBool extremes;
    extern const SettingsSetOperationMode intersect_default_mode;
    extern const SettingsDouble limit;
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsUInt64 max_result_bytes;
    extern const SettingsDouble offset;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool parallel_replicas_allow_view_over_mergetree;
    extern const SettingsBool parallel_replicas_plan_based;
    extern const SettingsBool enable_positional_arguments;
    extern const SettingsBool final;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsUInt64 max_bytes_to_read_leaf;
    extern const SettingsOverflowMode read_overflow_mode_leaf;
    extern const SettingsSeconds max_execution_time;
    extern const SettingsOverflowMode timeout_overflow_mode;
    extern const SettingsSeconds max_execution_time_leaf;
    extern const SettingsOverflowMode timeout_overflow_mode_leaf;
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsUInt64 max_rows_to_sort;
    extern const SettingsUInt64 max_bytes_to_sort;
    extern const SettingsOverflowMode sort_overflow_mode;
    extern const SettingsUInt64 max_rows_in_distinct;
    extern const SettingsUInt64 max_bytes_in_distinct;
    extern const SettingsOverflowMode distinct_overflow_mode;
}

namespace ServerSetting
{
    extern const ServerSettingsBool sql_security_views_are_optimization_barriers;
}

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


namespace
{

/// The analyzer names every table expression of the query text it ships to other replicas with a
/// synthetic alias of the exact form `__table<N>`, where `N` is a non-empty sequence of digits
/// (see `createUniqueAliasesIfNecessary`). Only that exact form is an internal alias: a user-visible
/// name that merely starts with `__table`, like `__table_prod`, can never be synthesized, so an
/// `additional_table_filters` entry keyed to it is an entry for an unrelated table. The same rule
/// is applied by `isPlannerGeneratedTableAlias` in `QueryResultCache.cpp`.
bool isAnalyzerGeneratedTableAlias(std::string_view name)
{
    constexpr std::string_view prefix = "__table";
    if (!name.starts_with(prefix))
        return false;
    const auto digits = name.substr(prefix.size());
    return !digits.empty() && std::ranges::all_of(digits, [](char c) { return c >= '0' && c <= '9'; });
}

/// A step is row-preserving when it cannot drop rows, so an expression evaluated below it sees
/// exactly the rows that reach the step above it. Only such steps may separate an outer predicate
/// from the view's data without weakening what the view filters out.
bool isRowPreservingStep(const IQueryPlanStep & step)
{
    if (typeid_cast<const ExpressionStep *>(&step))
        return true;

    /// Sorting keeps every row unless it also truncates to a top-N.
    if (const auto * sorting = typeid_cast<const SortingStep *>(&step))
        return sorting->getLimit() == 0;

    /// Reading the source: PREWHERE written in the view drops rows, and an outer predicate must
    /// not join that same PREWHERE chain. A row policy of the definer needs no barrier — the
    /// reading step always applies it before PREWHERE and before any pushed-down filter.
    ///
    /// `typeid_cast` compares types exactly, so matching these two base classes needs
    /// `dynamic_cast` — every reading step is a subclass of them, never one of them.
    if (const auto * source_with_filter = dynamic_cast<const SourceStepWithFilter *>(&step))
        return source_with_filter->getPrewhereInfo() == nullptr;

    if (dynamic_cast<const ISourceStep *>(&step))
        return true;

    return false;
}

/// Mark every step of a view's subplan that decides which rows the view exposes, so that the
/// optimizer will not evaluate anything from the outer query below it. See
/// `IQueryPlanStep::isSecurityBarrier`. Returns whether the view can drop rows at all.
///
/// Nothing is marked when it cannot, which keeps a plain projection view exactly as optimizable
/// as it is today.
bool markSecurityBarriers(QueryPlan::Node * node)
{
    if (!node || !node->step)
        return false;

    bool marked = false;
    if (!isRowPreservingStep(*node->step))
    {
        node->step->setSecurityBarrier();
        marked = true;
    }

    for (auto * child : node->children)
        marked |= markSecurityBarriers(child);

    return marked;
}

/// Walks `ast` and reports whether `predicate` holds for any node visited, looking through SQL
/// user-defined functions: `CREATE FUNCTION f AS (a) -> arrayJoin(a)`, `(x) -> sum(x)`,
/// `(x) -> x IN (SELECT ...)` may wrap any construct a classifier of this file looks for.
///
/// A view defined through `CREATE VIEW` or `ALTER TABLE ... MODIFY QUERY` is stored with its SQL
/// UDFs already substituted (`UserDefinedSQLFunctionVisitor` in `InterpreterCreateQuery` and
/// `InterpreterAlterQuery`), so the classifiers normally never meet one. The descent is defense in
/// depth for a stored query that did not pass through those interpreters - metadata written before
/// the substitution existed, or edited by hand - and costs nothing otherwise. Any chain that cannot
/// be followed - a recursive definition, an implausibly deep nesting, a body that is not a SQL
/// lambda - counts as a match: every caller uses a match to refuse an optimization, so this is the
/// fail-closed direction.
///
/// With `descend_into_subqueries = false` the nodes of `ASTSubquery` / `ASTSelectQuery` children
/// are skipped: a subquery has its own scope, and `StorageView::canHideRows` descends into `FROM`
/// subqueries separately.
template <typename Predicate>
bool containsThroughSQLUserDefinedFunctions(
    const IAST & ast, const Predicate & predicate, bool descend_into_subqueries, std::unordered_set<String> & udfs_in_progress, size_t depth)
{
    if (predicate(ast))
        return true;

    if (const auto * function = ast.as<ASTFunction>())
    {
        if (auto user_defined_function = UserDefinedSQLFunctionFactory::instance().tryGet(function->name))
        {
            if (depth >= 16 || udfs_in_progress.contains(function->name))
                return true;

            const auto * create_function_query = user_defined_function->as<ASTCreateSQLFunctionQuery>();
            if (!create_function_query || !create_function_query->function_core
                || create_function_query->function_core->children.empty())
                return true;

            /// `function_core` is `lambda(tuple(args...), body)`; the body is the second element.
            const auto & lambda_arguments = create_function_query->function_core->children.front()->children;
            if (lambda_arguments.size() != 2 || !lambda_arguments[1])
                return true;

            udfs_in_progress.insert(function->name);
            bool body_matches = containsThroughSQLUserDefinedFunctions(
                *lambda_arguments[1], predicate, descend_into_subqueries, udfs_in_progress, depth + 1);
            udfs_in_progress.erase(function->name);
            if (body_matches)
                return true;
        }
    }

    for (const auto & child : ast.children)
    {
        if (!descend_into_subqueries && (child->as<ASTSubquery>() || child->as<ASTSelectQuery>()))
            continue;
        if (containsThroughSQLUserDefinedFunctions(*child, predicate, descend_into_subqueries, udfs_in_progress, depth))
            return true;
    }
    return false;
}

template <typename Predicate>
bool containsThroughSQLUserDefinedFunctions(const IAST & ast, const Predicate & predicate, bool descend_into_subqueries)
{
    std::unordered_set<String> udfs_in_progress;
    return containsThroughSQLUserDefinedFunctions(ast, predicate, descend_into_subqueries, udfs_in_progress, 0);
}

/// The row-hiding carriers that live in expressions rather than in a clause of the `SELECT`:
/// an aggregation without `GROUP BY` collapses all rows into one, and the `arrayJoin` function
/// (with its case-insensitive alias `unnest`) is the expression-level twin of the `ARRAY JOIN`
/// clause - it drops the rows whose array is empty and multiplies the rest, and it never shows
/// up in `arrayJoinExpressionList`. For the purpose of `StorageView::canHideRows` both hide rows
/// just like a filter does. Subqueries have their own scope: a carrier inside one does not change
/// the rows of the enclosing query.
bool hasRowHidingFunctionOutsideSubqueries(const IAST & ast)
{
    return containsThroughSQLUserDefinedFunctions(
        ast,
        [](const IAST & node)
        {
            const auto * function = node.as<ASTFunction>();
            if (!function)
                return false;
            if (!function->isWindowFunction() && AggregateUtils::isAggregateFunction(*function))
                return true;
            const auto name = Poco::toLower(function->name);
            return name == "arrayjoin" || name == "unnest";
        },
        /*descend_into_subqueries=*/ false);
}

bool isNullableOrLcNullable(DataTypePtr type)
{
    if (type->isNullable())
        return true;

    if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return lc_type->getDictionaryType()->isNullable();

    return false;
}

/// Returns `true` if there are nullable column in src but corresponding column in dst is not
bool changedNullabilityOneWay(const Block & src_block, const Block & dst_block)
{
    std::unordered_map<String, bool> src_nullable;
    for (const auto & col : src_block)
        src_nullable[col.name] = isNullableOrLcNullable(col.type);

    for (const auto & col : dst_block)
    {
        if (!isNullableOrLcNullable(col.type) && src_nullable[col.name])
            return true;
    }
    return false;
}

bool hasJoin(const ASTSelectQuery & select)
{
    const auto & tables = select.tables();
    if (!tables || tables->children.size() < 2)
        return false;

    const auto & joined_table = tables->children[1]->as<ASTTablesInSelectQueryElement &>();
    return joined_table.table_join != nullptr;
}

bool hasJoin(const ASTSelectWithUnionQuery & ast)
{
    for (const auto & child : ast.list_of_selects->children)
    {
        if (const auto * select = child->as<ASTSelectQuery>(); select && hasJoin(*select))
            return true;
    }
    return false;
}

/// The three classifiers below serve `tryGetTrivialViewUnderlyingStorage`. They look through
/// SQL user-defined functions like `hasRowHidingFunctionOutsideSubqueries` does: a view body such
/// as `SELECT f(x) FROM dist` with `CREATE FUNCTION f AS (x) -> sum(x)` aggregates just like
/// `SELECT sum(x) FROM dist`, and shipping the whole outer query to the shards would count one
/// aggregate row per shard instead of one in total if such a body were ever classified as trivial.

/// Returns true if the expression contains a subquery anywhere in its tree.
bool hasSubquery(const ASTPtr & expr)
{
    if (!expr)
        return false;
    return containsThroughSQLUserDefinedFunctions(
        *expr, [](const IAST & node) { return node.as<ASTSubquery>() != nullptr; }, /*descend_into_subqueries=*/ true);
}

/// Returns true if the expression contains an aggregate function anywhere in its tree.
bool hasAggregate(const ASTPtr & expr)
{
    if (!expr)
        return false;
    return containsThroughSQLUserDefinedFunctions(
        *expr,
        [](const IAST & node)
        {
            const auto * func = node.as<ASTFunction>();
            return func && AggregateFunctionFactory::instance().isAggregateFunctionName(func->name);
        },
        /*descend_into_subqueries=*/ true);
}

/// Returns true if the expression contains a scalar subquery or a window function anywhere in its tree.
bool hasSubqueryOrWindow(const ASTPtr & expr)
{
    if (!expr)
        return false;
    return containsThroughSQLUserDefinedFunctions(
        *expr,
        [](const IAST & node)
        {
            if (node.as<ASTSubquery>())
                return true;
            const auto * func = node.as<ASTFunction>();
            return func && (!func->window_name.empty() || func->window_definition);
        },
        /*descend_into_subqueries=*/ true);
}

/// Returns the underlying storage if the view's inner query is "trivial":
/// a plain SELECT of columns, expressions, or * from a single table, optionally with a simple WHERE
/// (no subqueries), and no other transformations. Scalar subqueries, window functions, and aggregate
/// functions in the SELECT list are not allowed.
/// Returns nullptr if any condition is not met.
StoragePtr tryGetTrivialViewUnderlyingStorage(const ASTPtr & inner_query, ContextPtr context)
{
    const auto * select_with_union = inner_query->as<ASTSelectWithUnionQuery>();
    if (!select_with_union || select_with_union->list_of_selects->children.size() != 1)
    {
        return nullptr;
    }

    const auto * select = select_with_union->list_of_selects->children[0]->as<ASTSelectQuery>();
    if (!select)
    {
        return nullptr;
    }

    /// Non-deterministic / server-local functions (hostName, nowInBlock, ...) inside the view
    /// body are intentionally not checked here: the body is read through StorageDistributed::read
    /// in both the pushdown and non-pushdown paths, so those expressions run on the shards either
    /// way. Only the outer query needs that gate, applied in PlannerJoinTree.cpp.
    ///
    /// A SETTINGS clause in the view body is rejected outright (fail close). Some query-level
    /// settings (notably `limit` and `offset`) are turned into QueryNode limit/offset by
    /// QueryTreeBuilder, so a body such as `SELECT id FROM dist SETTINGS limit = 1` would be limited
    /// once globally on the normal path but once per shard on the pushdown path, changing the
    /// result. Rather than enumerate every result-changing setting, disqualify any SETTINGS clause.
    /// GROUP BY ALL and LIMIT BY ALL set boolean flags (group_by_all / limit_by_all) while leaving the
    /// corresponding expression lists (groupBy() / limitBy()) empty, so the list checks above miss them
    /// and they must be checked via the flags. Like their explicit counterparts, they aggregate or
    /// limit per shard under the pushdown instead of once globally on the normal path, changing the
    /// result. The WITH TOTALS/ROLLUP/CUBE/GROUPING SETS modifiers are likewise aggregation markers,
    /// and limitByLength()/limitByOffset() carry the N/OFFSET of a LIMIT BY — all rejected fail-close.
    /// A `LIMIT [n] AFTER/UNTIL` range is applied once on the initiator on the normal path
    /// (StorageDistributed::getOptimizedQueryProcessingStageAnalyzer keeps the default stage for it), whereas
    /// the pushdown would apply it on every shard to that shard's rows, so it is rejected as well.
    ///
    /// ORDER BY ALL differs: the parser populates orderBy() with a placeholder `all` element in
    /// addition to setting order_by_all, so the orderBy() check above already rejects it (ORDER BY ALL
    /// with an outer LIMIT would otherwise let the coordinator return a shard-local first row instead
    /// of the globally first one). order_by_all is still checked here as defense-in-depth in case the
    /// body AST is ever produced without that placeholder.
    if (select->with() || select->prewhere()
        || (select->where() && hasSubquery(select->where()))
        || select->groupBy() || select->group_by_all
        || select->group_by_with_totals || select->group_by_with_rollup
        || select->group_by_with_cube || select->group_by_with_grouping_sets
        || select->having() || select->qualify()
        || select->orderBy() || select->order_by_all
        || select->limitLength() || select->limitOffset()
        || select->limitAfter() || select->limitUntil()
        || select->limitBy() || select->limit_by_all
        || select->limitByLength() || select->limitByOffset()
        || select->distinct || select->arrayJoinExpressionList().first
        || select->settings())
    {
        return nullptr;
    }

    const auto * select_expr_list = select->select().get();
    if (!select_expr_list)
    {
        return nullptr;
    }
    for (const auto & expr : select_expr_list->children)
    {
        if (const auto * asterisk = expr->as<ASTAsterisk>())
        {
            /// Column transformers (APPLY/REPLACE/EXCEPT) can carry aggregate, window, or
            /// non-deterministic expressions, making the view non-trivial.
            if (asterisk->transformers)
                return nullptr;
            continue;
        }
        if (const auto * qualified_asterisk = expr->as<ASTQualifiedAsterisk>())
        {
            if (qualified_asterisk->transformers)
                return nullptr;
            continue;
        }
        if (hasSubqueryOrWindow(expr) || hasAggregate(expr))
        {
            return nullptr;
        }
    }

    const auto * tables = select->tables().get();
    if (!tables || tables->children.size() != 1)
    {
        return nullptr;
    }

    const auto * table_element = tables->children[0]->as<ASTTablesInSelectQueryElement>();
    if (!table_element || !table_element->table_expression
        || table_element->table_join || table_element->array_join)
    {
        return nullptr;
    }

    const auto * table_expr = table_element->table_expression->as<ASTTableExpression>();
    if (!table_expr || !table_expr->database_and_table_name
        || table_expr->subquery || table_expr->table_function
        || table_expr->final || table_expr->sample_size)
    {
        return nullptr;
    }

    const auto * table_id_node = table_expr->database_and_table_name->as<ASTTableIdentifier>();
    if (!table_id_node)
    {
        return nullptr;
    }

    StorageID storage_id = table_id_node->getTableId();
    if (storage_id.database_name.empty())
    {
        storage_id.database_name = context->getCurrentDatabase();
    }

    return DatabaseCatalog::instance().tryGetTable(storage_id, context);
}


/** There are no limits on the maximum size of the result for the view.
  *  Since the result of the view is not the result of the entire query.
  *
  * The context is also marked as a view inner context so that the query analyzer
  * resolves positional arguments inside the view even on remote/secondary nodes
  * (views are expanded on remote nodes, unlike the outer query).
  */
ContextMutablePtr getViewContext(ContextPtr context, const StorageSnapshotPtr & storage_snapshot, const StorageView * view, const std::optional<String> & view_alias)
{
    auto view_context = storage_snapshot->metadata->getSQLSecurityOverriddenContext(context);
    Settings view_settings = view_context->getSettingsCopy();

    /// With plan-based parallel replicas we always build local, so there is no need to disable parallel replicas
    if (context->canUseParallelReplicasOnInitiator() && view_settings[Setting::parallel_replicas_allow_view_over_mergetree]
        && !view_settings[Setting::parallel_replicas_plan_based])
    {
        if (auto storage = view->getUnderlyingMergeTreeStorageForParallelReplicas(context, view_alias))
            view_settings[Setting::allow_experimental_parallel_reading_from_replicas] = Field{0};
    }

    view_settings[Setting::max_result_rows] = 0;
    view_settings[Setting::max_result_bytes] = 0;
    view_settings[Setting::extremes] = false;
    view_context->setSettings(view_settings);
    view_context->setIsViewInnerQuery(true);
    return view_context;
}

}

VirtualColumnsDescription StorageView::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

StorageView::StorageView(
    const StorageID & table_id_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    const String & comment,
    bool is_parameterized_view_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    if (!is_parameterized_view_)
    {
        /// If CREATE query is to create parameterized view, then we dont want to set columns
        if (!query.isParameterizedView())
            storage_metadata.setColumns(columns_);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setComment(comment);
    if (query.sql_security)
        storage_metadata.setSQLSecurity(query.sql_security->as<ASTSQLSecurity &>());

    if (storage_metadata.sql_security_type == SQLSecurityType::DEFINER)
        DefinerDependencies::instance().addDependency(*storage_metadata.definer, table_id_);

    if (!query.select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for {}", getName());
    SelectQueryDescription description;

    description.inner_query = query.select->ptr();

    NormalizeSelectWithUnionQueryVisitor::Data data{SetOperationMode::Unspecified};
    NormalizeSelectWithUnionQueryVisitor{data}.visit(description.inner_query);

    is_parameterized_view = is_parameterized_view_ || query.isParameterizedView();
    storage_metadata.setSelectQuery(description);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

/// Build and resolve the view's inner query tree
/// Then find the leftmost underlying MT storage eligible for parallel replicas.
/// Returns nullptr if the view is too complex or resolution fails.
StoragePtr StorageView::getUnderlyingMergeTreeStorageForParallelReplicas(const ContextPtr & context, const std::optional<String> & alias) const
{
    if (isParameterizedView())
        return nullptr;

    /// When called from INSERT ... SELECT context, the context carries insertion table info.
    /// If we resolve the view's inner query with this context, table functions like file()
    /// may incorrectly infer schema from the insertion table (via use_structure_from_insertion_table_in_table_functions),
    /// poisoning the schema cache with wrong column names.
    if (context->hasInsertionTable())
        return nullptr;

    auto metadata_snapshot = getInMemoryMetadataPtr(context, false);
    auto inner_query_ast = metadata_snapshot->getSelectQuery().inner_query;

    /// Query-based parallel replicas over a view (`parallel_replicas_allow_view_over_mergetree`)
    /// do not read the view: the callers of this function take the storage it returns and read
    /// that storage with parallel replicas, under the *outer* query's context and identity. The
    /// view's filtering - its own row policies, the definer's row policies on the inner table,
    /// the definer profile's filters - is not part of that read, and the announcement and the
    /// per-replica reads see every row of the inner table. For a security barrier view whose
    /// filtering must be trusted this is the same disclosure the rest of the barrier closes, so
    /// fail closed here as well: the view is then read through `readImpl`, which builds the
    /// filtering subplan and seals it.
    ///
    /// A view that provably hides nothing keeps the optimization - there is nothing below it that
    /// the invoker could not read directly.
    if (isSecurityBarrier(*metadata_snapshot, context))
    {
        auto view_id = getStorageID();
        auto view_row_policy_filter = context->getRowPolicyFilter(
            view_id.getDatabaseName(), view_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
        const bool has_row_policy = view_row_policy_filter && !view_row_policy_filter->isAlwaysTrue();
        if (has_row_policy || canHideRows(inner_query_ast, metadata_snapshot->getSQLSecurityOverriddenContext(context)))
            return nullptr;

        /// The shortcut only decides that the query *can* use parallel replicas; the query the
        /// replicas receive still reads the view. A replica plans it on its own, and when an
        /// `additional_table_filters` entry of the invoker applies to the view, the barrier keeps
        /// the view a table expression instead of inlining it, so the replica reads it through
        /// `readImpl` - which switches parallel replicas off for the inner query, because a
        /// shipped fragment would run under the connection's identity. Every replica then reads
        /// the whole view with no coordination and the result is the union of all of them, so the
        /// rows are returned once per replica. So decline the shortcut exactly when the replica
        /// will decline to inline the view: when an entry applies to the view by its name or by the
        /// alias the outer query gives it (`hasAdditionalTableFilter`, the same rule as in
        /// `QueryAnalyzer::inlineViewSubqueryIfNeeded`). An entry keyed to an unrelated table
        /// cannot make the replica take that path, so it keeps the shortcut. An entry keyed to an
        /// internal `__table<N>` alias counts as applying: the query text a replica receives names the
        /// view by such an alias, so the replica would match the entry even though the outer query
        /// never wrote it. A caller that does not know the alias fails closed on any entry.
        const auto & additional_table_filters = context->getSettingsRef()[Setting::additional_table_filters].value;
        if (!additional_table_filters.empty())
        {
            if (!alias)
                return nullptr;
            if (hasAdditionalTableFilter(view_id, *alias, context))
                return nullptr;
            if (additionalTableFiltersApplyToInternalAlias(additional_table_filters))
                return nullptr;
        }
    }

    QueryTreeNodePtr inner_query_tree;
    try
    {
        inner_query_tree = buildQueryTree(inner_query_ast->clone(), context);
        QueryTreePassManager pass_manager(context);
        addQueryTreePasses(pass_manager);
        pass_manager.runOnlyResolve(inner_query_tree);
    }
    catch (const Exception &)
    {
        /// The view may reference table functions, use SQL SECURITY DEFINER,
        /// or have other constructs that prevent resolution with the current user's context.
        /// Example: 03667_view_with_s3_cluster_and_sql_security_definer.
        /// Just return nullptr to indicate the view is not suitable for this optimization.
        tryLogCurrentException(
            __func__, fmt::format("Failed to resolve inner query of view {}", getStorageID().getFullTableName()), LogsLevel::trace);
        return nullptr;
    }

    /// Recursively walk the resolved query tree to find the underlying MergeTree storage.
    /// For UNION nodes, all branches must be eligible.
    /// Returns nullptr if the view is not suitable for parallel replicas.
    std::function<StoragePtr(const IQueryTreeNode *)> find_storage = [&](const IQueryTreeNode * node) -> StoragePtr
    {
        while (node)
        {
            switch (node->getNodeType())
            {
                case QueryTreeNodeType::QUERY:
                {
                    const auto & query_node = node->as<QueryNode &>();
                    /// Only simple pass-through views are eligible. Any clause that changes
                    /// result semantics when evaluated per-replica must disqualify the view.
                    if (query_node.hasGroupBy() || query_node.hasHaving()
                        || query_node.hasWindow() || query_node.hasQualify()
                        || query_node.hasOrderBy() || query_node.isDistinct()
                        || query_node.hasLimitByLimit() || query_node.hasLimitByOffset()
                        || query_node.hasLimitBy()
                        || query_node.hasLimit() || query_node.hasOffset()
                        || query_node.hasLimitAfter() || query_node.hasLimitUntil()
                        || hasWindowFunctionNodes(query_node.getProjectionNode()))
                        return nullptr;

                    node = query_node.getJoinTreeNode().get();
                    break;
                }
                case QueryTreeNodeType::UNION:
                {
                    const auto & union_node = node->as<UnionNode &>();

                    /// Only UNION ALL is safe to parallelize.
                    if (union_node.getUnionMode() != SelectUnionMode::UNION_ALL)
                        return nullptr;

                    const auto & queries = union_node.getQueries().getNodes();
                    if (queries.empty())
                        return nullptr;

                    /// Check ALL branches of the UNION — not just the first one.
                    /// Every branch must resolve to an eligible MergeTree storage.
                    /// The branches may reference different tables, but if the same
                    /// table appears in multiple branches, reject it —
                    /// we avoid supporting it, since it requires to complicate parallel replicas protocol
                    /// and considered as not very practical case
                    StoragePtr result;
                    std::unordered_set<StorageID, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual> seen_ids;
                    for (const auto & query : queries)
                    {
                        auto branch_storage = find_storage(query.get());
                        if (!branch_storage)
                            return nullptr;

                        if (!seen_ids.insert(branch_storage->getStorageID()).second)
                            return nullptr;

                        if (!result)
                            result = branch_storage;
                    }
                    return result;
                }
                case QueryTreeNodeType::TABLE:
                {
                    const auto & table_node = node->as<const TableNode &>();
                    const auto & storage = table_node.getStorage();

                    /// If the table is itself a view, recursively check its inner query.
                    const auto * nested_view = typeid_cast<const StorageView *>(storage.get());
                    if (nested_view)
                        return nested_view->getUnderlyingMergeTreeStorageForParallelReplicas(context, table_node.getOriginalAlias());

                    if (!isTableNodeEligibleForParallelReplicas(table_node, storage, context))
                        return nullptr;

                    return table_node.getStorage();
                }
                default:
                    return nullptr;
            }
        }
        return nullptr;
    };

    return find_storage(inner_query_tree.get());
}

StoragePtr StorageView::tryGetUnderlyingDistributed(const StorageSnapshotPtr & snapshot, ContextPtr context) const
{
    if (is_parameterized_view || snapshot->metadata->sql_security_type == SQLSecurityType::DEFINER)
    {
        return nullptr;
    }

    /// The pushdown replaces the view with its inner query and reads the `Distributed` table
    /// directly, so `StorageView::readImpl` never runs and the plan carries no security-barrier
    /// step: the outer predicate is shipped to the shards and evaluated there, below anything that
    /// hides rows from the caller. For a barrier view that is exactly what the barrier forbids, and
    /// it cannot be excused by a proof over the view's definition: a shard resolves the
    /// `Distributed` table to a table of its own and runs the shipped query as the cluster's user
    /// (or as the initial user when the cluster has a `secret`), so a row policy on the shard-local
    /// table hides rows on the non-pushdown path while nothing on the initiator can see that policy
    /// - `canHideRows` can only inspect the tables of this server. Decline the rewrite for every
    /// barrier view, the same fail-closed rule the other pre-plan decisions apply to a remote source;
    /// `readImpl` then keeps the invoker's predicate on the initiator, above the view's read.
    /// (`DEFINER` is rejected above regardless of the setting.)
    if (isSecurityBarrier(*snapshot->metadata, context))
    {
        return nullptr;
    }

    const auto & inner_query = snapshot->metadata->getSelectQuery().inner_query;
    auto underlying = tryGetTrivialViewUnderlyingStorage(inner_query, context);
    if (!underlying || !typeid_cast<const StorageDistributed *>(underlying.get()))
    {
        return nullptr;
    }

    return underlying;
}

bool StorageView::additionalTableFiltersApplyTo(
    const Field & additional_table_filters, const std::vector<StorageID> & table_ids, const String & alias, const String & current_database)
{
    /// The setting accepts a map literal as well as its string form; `SettingFieldMap` is what
    /// applies either to a context, so it is also what decides whether the value is well-formed.
    Map filters;
    try
    {
        filters = SettingFieldMap(additional_table_filters).value;
    }
    catch (const Exception &)
    {
        /// Not a map of string to string: cannot be proven to apply to no table, so it counts as applying.
        return true;
    }

    for (const auto & additional_filter : filters)
    {
        if (additional_filter.getType() != Field::Types::Tuple)
            return true;
        const auto & tuple = additional_filter.safeGet<Tuple>();
        if (tuple.size() != 2 || tuple[0].getType() != Field::Types::String)
            return true;

        const auto & table = tuple[0].safeGet<String>();
        if (!alias.empty() && table == alias)
            return true;
        for (const auto & table_id : table_ids)
        {
            if ((table == table_id.getTableName() && current_database == table_id.getDatabaseName())
                || table == table_id.getFullNameNotQuoted())
                return true;
        }
    }

    return false;
}

bool StorageView::hasAdditionalTableFilter(const StorageID & storage_id, const String & alias, const ContextPtr & context)
{
    return additionalTableFiltersApplyTo(
        context->getSettingsRef()[Setting::additional_table_filters].value, {storage_id}, alias, context->getCurrentDatabase());
}

bool StorageView::additionalTableFiltersApplyToInternalAlias(const Field & additional_table_filters)
{
    Map filters;
    try
    {
        filters = SettingFieldMap(additional_table_filters).value;
    }
    catch (const Exception &)
    {
        return true;
    }

    for (const auto & additional_filter : filters)
    {
        if (additional_filter.getType() != Field::Types::Tuple)
            return true;
        const auto & tuple = additional_filter.safeGet<Tuple>();
        if (tuple.size() != 2 || tuple[0].getType() != Field::Types::String)
            return true;

        if (isAnalyzerGeneratedTableAlias(tuple[0].safeGet<String>()))
            return true;
    }

    return false;
}

void StorageView::readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        const size_t /*max_block_size*/,
        const size_t /*num_streams*/)
{
    ASTPtr current_inner_query = storage_snapshot->metadata->getSelectQuery().inner_query;

    if (query_info.view_query)
    {
        if (!query_info.view_query->as<ASTSelectWithUnionQuery>())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected optimized VIEW query");
        current_inner_query = query_info.view_query->clone();
    }

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);

    const bool security_barrier = isSecurityBarrier(*storage_snapshot->metadata, context);

    /// The outer filter is handed to the inner query so that a view over `Distributed` can still
    /// skip unused shards. For a security barrier view it must stay outside: the analysis it feeds
    /// reaches the inner tables' index analysis, which then skips parts and granules by the values
    /// of the rows the view hides, and the `read_rows` of the query tells the invoker about them.
    /// A view that provably hides no rows keeps it — there is nothing below it to observe.
    ///
    /// The decision looks at the view's definition and not at `current_inner_query`: on the
    /// legacy `InterpreterSelectQuery` path the latter is the rewritten query, into which the
    /// outer predicate has already been pushed, and the invoker's own predicate is not something
    /// to protect.
    auto storage_id = getStorageID();
    auto row_policy_filter = context->getRowPolicyFilter(
        storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
    const bool has_row_policy = row_policy_filter && !row_policy_filter->isAlwaysTrue();
    const bool has_additional_filter = query_info.additional_filter_ast != nullptr;
    /// The alias the outer query gives this view, for the parallel-replicas shortcut decision
    /// inside `getViewContext`; the legacy planner does not hand the table expression over.
    std::optional<String> view_alias;
    if (query_info.table_expression)
        view_alias = query_info.table_expression->getOriginalAlias();
    auto view_context = getViewContext(context, storage_snapshot, this, view_alias);
    const bool hides_rows = security_barrier
        && (has_row_policy || has_additional_filter || canHideRows(storage_snapshot->metadata->getSelectQuery().inner_query, view_context));
    const ActionsDAG * post_filter = hides_rows ? nullptr : query_info.filter_actions_dag.get();

    /// Task-based parallel replicas ship the inner query as SQL text to the other replicas, where
    /// it is re-planned under the connection's own identity: the replica applies the row policies
    /// of its connecting user and of the *initial* user (the invoker), but the definer is neither
    /// of those, so the definer's row policies on the inner tables and the definer profile's
    /// `additional_table_filters` are silently dropped and the rows they hide come back through
    /// the union into the invoker's plan, above the barrier. Fail closed: a view whose filtering
    /// must be trusted reads its inner query without parallel replicas.
    ///
    /// The setting is changed on the view context itself instead of on a copy of it: for a
    /// `DEFINER`/`NONE` view that context is its own query context (`makeQueryContext`), and a copy
    /// keeps a weak pointer to the original, so replacing the original with the copy destroys the
    /// query context the inner query resolves against.
    if (hides_rows && view_context->getSettingsRef()[Setting::allow_experimental_parallel_reading_from_replicas] != 0)
        view_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field{0});

    {
        InterpreterSelectQueryAnalyzer interpreter(
            current_inner_query, view_context, options, column_names, post_filter);
        interpreter.addStorageLimits(*query_info.storage_limits);
        query_plan = std::move(interpreter).extractQueryPlan();
    }

    /// And also convert to expected structure.
    const auto & expected_header = storage_snapshot->getSampleBlockForColumns(column_names);
    const auto & header = query_plan.getCurrentHeader();

    const auto * select_with_union = current_inner_query->as<ASTSelectWithUnionQuery>();
    if (select_with_union && hasJoin(*select_with_union) && changedNullabilityOneWay(*header, expected_header))
    {
        throw DB::Exception(ErrorCodes::INCORRECT_QUERY,
                            "Query from view {} returned Nullable column having not Nullable type in structure. "
                            "If query from view has JOIN, it may be cause by different values of 'join_use_nulls' setting. "
                            "You may explicitly specify 'join_use_nulls' in 'CREATE VIEW' query to avoid this error",
                            getStorageID().getFullTableName());
    }

    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            header->getColumnsWithTypeAndName(),
            expected_header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            context, false, false, nullptr, nullptr, false);

    auto converting = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(convert_actions_dag));
    converting->setStepDescription("Convert VIEW subquery result to VIEW table structure");
    query_plan.addStep(std::move(converting));

    /// A view with `SQL SECURITY DEFINER` or `SQL SECURITY NONE` runs its inner query as somebody
    /// else, so whatever the inner query filters out is data the invoker has no right to observe.
    /// The plan built above is about to be embedded into the invoker's plan, where merging and
    /// pushdown would otherwise let an invoker-supplied expression run on the filtered-out rows.
    /// Mark the steps that do the filtering so the optimizer keeps the outer query above them.
    ///
    /// `INVOKER` views need no barrier: their inner query runs with the invoker's own rights, so
    /// there is nothing the invoker could learn that they are not already entitled to. Neither
    /// does a view that provably hides no rows — and on the legacy `InterpreterSelectQuery` path
    /// its subplan may contain the outer predicate already pushed into it, which must not be
    /// mistaken for the view's own filtering.
    if (hides_rows)
    {
        auto * root = query_plan.getRootNode();
        /// Marking the individual filtering steps lets an outer predicate still sink through the
        /// view's projections and sit right on top of them. Marking the converting step on top as
        /// well seals the view: even if a later optimization rebuilds one of the inner steps and
        /// drops its flag, nothing from outside can enter the subplan.
        markSecurityBarriers(root);
        /// The converting step is sealed unconditionally, not only when the local walk marked a
        /// step. `hides_rows` already means `canHideRows` could not prove the view row-preserving,
        /// which is the case for a wrapper source — `Merge`, a remote table, a table function, or a
        /// nested view — whose row-dropping happens below the source interface or inside its child
        /// plans, where `markSecurityBarriers` (it walks only `node->children`) sees nothing to
        /// mark. Without this seal such a wrapper would execute as an ordinary subplan and later
        /// passes could move invoker-controlled work into it, undoing the fail-closed decision.
        root->step->setSecurityBarrier();
    }
}

void StorageView::drop()
{
    auto table_id = getStorageID();

    auto metadata_snapshot = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    if (metadata_snapshot->sql_security_type == SQLSecurityType::DEFINER)
        DefinerDependencies::instance().removeDependencies(table_id);
}

void StorageView::alter(
    const AlterCommands & params,
    ContextPtr context,
    AlterLockHolder &,
    DDLGuardPtr &)
{
    auto table_id = getStorageID();
    auto metadata_snapshot = getInMemoryMetadataPtr(context, false);
    StorageInMemoryMetadata new_metadata = *metadata_snapshot;
    params.apply(new_metadata, context);

    DatabaseCatalog::instance()
        .getDatabase(table_id.database_name)
        ->alterTable(context, table_id, new_metadata, /*validate_new_create_query=*/true);

    auto & instance = DefinerDependencies::instance();
    if (new_metadata.sql_security_type == SQLSecurityType::DEFINER)
        instance.addDependency(*new_metadata.definer, table_id);
    else
        instance.removeDependencies(table_id);

    setInMemoryMetadata(new_metadata);
}

static ASTTableExpression * getFirstTableExpression(ASTSelectQuery & select_query)
{
    if (!select_query.tables() || select_query.tables()->children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No table expression in view select AST");

    auto * select_element = select_query.tables()->children[0]->as<ASTTablesInSelectQueryElement>();

    if (!select_element->table_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect table expression");

    return select_element->table_expression->as<ASTTableExpression>();
}

void StorageView::replaceQueryParametersIfParameterizedView(ASTPtr & outer_query, const NameToNameMap & parameter_values)
{
    ReplaceQueryParameterVisitor visitor(parameter_values);
    visitor.visit(outer_query);
}

void StorageView::replaceWithSubquery(ASTSelectQuery & outer_query, ASTPtr view_query, ASTPtr & view_name, bool parameterized_view)
{
    ASTTableExpression * table_expression = getFirstTableExpression(outer_query);

    if (!table_expression->database_and_table_name)
    {
        /// If it's a view or merge table function, add a fake db.table name.
        /// For parameterized view, the function name is the db.view name, so add the function name
        if (table_expression->table_function)
        {
            auto table_function_name = table_expression->table_function->as<ASTFunction>()->name;
            if (table_function_name == "view" || table_function_name == "viewIfPermitted" || table_function_name == "eval")
                table_expression->database_and_table_name = make_intrusive<ASTTableIdentifier>("__view");
            else if (table_function_name == "merge")
                table_expression->database_and_table_name = make_intrusive<ASTTableIdentifier>("__merge");
            else if (parameterized_view)
                table_expression->database_and_table_name = make_intrusive<ASTTableIdentifier>(table_function_name);

        }
        if (!table_expression->database_and_table_name)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect table expression");
    }

    DatabaseAndTableWithAlias db_table(table_expression->database_and_table_name);
    String alias = db_table.alias.empty() ? db_table.table : db_table.alias;

    view_name = table_expression->database_and_table_name;
    table_expression->database_and_table_name = {};
    table_expression->subquery = make_intrusive<ASTSubquery>(view_query);
    table_expression->subquery->setAlias(alias);

    for (auto & child : table_expression->children)
        if (child.get() == view_name.get())
            child = view_query;
        else if (child.get()
                 && child->as<ASTFunction>()
                 && table_expression->table_function
                 && table_expression->table_function->as<ASTFunction>()
                 && child->as<ASTFunction>()->name == table_expression->table_function->as<ASTFunction>()->name)
            child = view_query;
}

ASTPtr StorageView::restoreViewName(ASTSelectQuery & select_query, const ASTPtr & view_name)
{
    ASTTableExpression * table_expression = getFirstTableExpression(select_query);

    if (!table_expression->subquery)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect table expression");

    ASTPtr subquery = table_expression->subquery;
    table_expression->subquery = {};
    table_expression->database_and_table_name = view_name;

    for (auto & child : table_expression->children)
        if (child.get() == subquery.get())
            child = view_name;
    return subquery->children[0];
}

void StorageView::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* local_context */) const
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter() && command.type != AlterCommand::MODIFY_SQL_SECURITY)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());
    }
}

void registerStorageView(StorageFactory & factory);
void registerStorageView(StorageFactory & factory)
{
    factory.registerStorage("View", [](const StorageFactory::Arguments & args)
    {
        if (args.query.storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Specifying ENGINE is not allowed for a View");

        /// Resolve INTERSECT/EXCEPT precedence before constructing StorageView.
        /// StorageView's constructor runs NormalizeSelectWithUnionQueryVisitor which
        /// does not understand INTERSECT/EXCEPT modes and would incorrectly drop
        /// SELECT branches connected by these operators.
        /// This is needed when the AST is freshly parsed from stored metadata
        /// (e.g. during ATTACH) and has not been through executeQuery's visitors.
        /// For already-processed ASTs (e.g. from CREATE VIEW via executeQuery),
        /// this is a safe no-op since INTERSECT/EXCEPT modes have already been
        /// converted to ASTSelectIntersectExceptQuery nodes.
        if (args.query.select)
        {
            auto context = args.getContext();
            SelectIntersectExceptQueryVisitor::Data data{
                context->getSettingsRef()[Setting::intersect_default_mode],
                context->getSettingsRef()[Setting::except_default_mode]};
            auto select = args.query.select->ptr();
            SelectIntersectExceptQueryVisitor{data}.visit(select);
        }

        return std::make_shared<StorageView>(args.table_id, args.query, args.columns, args.comment);
    },
    {},
    Documentation{
        .description = R"DOCS_MD(
Used for implementing views (for more information, see the `CREATE VIEW query`). It does not store data, but only stores the specified `SELECT` query. When reading from a table, it runs this query (and deletes all unnecessary columns from the query).
)DOCS_MD",
        .syntax = "CREATE VIEW name AS SELECT ...",
        .related = {"MaterializedView"}});
}

bool StorageView::isSecurityBarrier(const StorageInMemoryMetadata & metadata, const ContextPtr & context)
{
    /// `INVOKER` needs no barrier: the inner query runs with the invoker's own rights, so there is
    /// nothing they could learn that they are not already entitled to.
    if (metadata.sql_security_type != SQLSecurityType::DEFINER && metadata.sql_security_type != SQLSecurityType::NONE)
        return false;

    return context->getServerSettings()[ServerSetting::sql_security_views_are_optimization_barriers];
}

/// A `SETTINGS` clause of the view's query is applied to the context the inner query runs in, so
/// it can hide rows without any clause of the `SELECT` doing so: `limit` and `offset` truncate the
/// result, `final` rewrites every table read as `FINAL`, `additional_table_filters` and
/// `additional_result_filter` add predicates, `max_rows_to_read` under a `read_overflow_mode = 'break'`
/// profile truncates the scan, `prefer_column_name_to_alias` changes which column an identifier
/// binds to, and so on. Rather than enumerate everything that can go wrong,
/// only settings that tune execution and provably cannot change the rows or the names the query
/// produces are accepted; any other change - including one that resets a setting to its default,
/// which may undo a limit of the definer's profile - fails closed. Query parameters bind values
/// into the query text, so a clause carrying them is not a pure tuning clause either.
/// `additional_table_filters` is special: its entries are keyed by table, so a clause whose entries
/// provably name none of the tables the query reads hides nothing. The caller that has resolved
/// the source table decides that through `additional_table_filters_apply`; without it the setting
/// fails closed.
bool StorageView::settingsClauseCanHideRows(
    const ASTPtr & settings_ast,
    bool has_sort,
    bool has_grouping,
    bool has_distinct,
    const std::function<bool(const Field &)> & additional_table_filters_apply)
{
    if (!settings_ast)
        return false;

    /// The counterpart of `shapeDependentOverflowCanHideRows` for a clause: each of these limits
    /// stops one operator early and returns what it has produced so far, but a query without that
    /// operator cannot lose a row to it, so the setting is as harmless as `max_threads` there. The
    /// value is not inspected: a query that does contain the operator fails closed on the mere
    /// presence of the setting (a reset to the default included), like on any other row-hiding one.
    static const std::unordered_set<std::string_view> sort_overflow_settings
    {
        "max_rows_to_sort",
        "max_bytes_to_sort",
        "sort_overflow_mode",
    };
    static const std::unordered_set<std::string_view> group_by_overflow_settings
    {
        "max_rows_to_group_by",
        "group_by_overflow_mode",
    };
    static const std::unordered_set<std::string_view> distinct_overflow_settings
    {
        "max_rows_in_distinct",
        "max_bytes_in_distinct",
        "distinct_overflow_mode",
    };

    static const std::unordered_set<std::string_view> execution_only_settings
    {
        "max_threads",
        "max_block_size",
        "preferred_block_size_bytes",
        "preferred_max_column_in_block_size_bytes",
        "max_memory_usage",
        "max_memory_usage_for_user",
        "max_bytes_before_external_group_by",
        "max_bytes_before_external_sort",
        "max_bytes_ratio_before_external_group_by",
        "max_bytes_ratio_before_external_sort",
        "max_streams_to_max_threads_ratio",
        "max_streams_for_merge_tree_reading",
        "merge_tree_min_rows_for_concurrent_read",
        "merge_tree_min_bytes_for_concurrent_read",
        "merge_tree_max_rows_to_use_cache",
        "merge_tree_max_bytes_to_use_cache",
        "min_bytes_to_use_direct_io",
        "min_bytes_to_use_mmap_io",
        "max_read_buffer_size",
        "use_uncompressed_cache",
        "local_filesystem_read_method",
        "remote_filesystem_read_method",
        "enable_filesystem_cache",
        "read_from_filesystem_cache_if_exists_otherwise_bypass_cache",
        "optimize_move_to_prewhere",
        "optimize_move_to_prewhere_if_final",
        "optimize_read_in_order",
        "optimize_aggregation_in_order",
        "use_skip_indexes",
        "priority",
        "os_thread_priority",
        "log_comment",
        "log_queries",
        "log_query_threads",
        "log_processors_profiles",
    };

    const auto is_execution_only = [&](std::string_view name)
    {
        return execution_only_settings.contains(name)
            || (!has_sort && sort_overflow_settings.contains(name))
            || (!has_grouping && group_by_overflow_settings.contains(name))
            || (!has_distinct && distinct_overflow_settings.contains(name));
    };

    const auto * set_query = settings_ast->as<ASTSetQuery>();
    if (!set_query || !set_query->query_parameters.empty())
        return true;

    for (const auto & change : set_query->changes)
    {
        if (change.name == "additional_table_filters")
        {
            if (!additional_table_filters_apply || additional_table_filters_apply(change.value))
                return true;
            continue;
        }

        if (!is_execution_only(change.name))
            return true;
    }

    for (const auto & name : set_query->default_settings)
        if (!is_execution_only(name))
            return true;

    return false;
}

bool StorageView::effectiveContextCanHideRows(const ContextPtr & context)
{
    /// The view's inner query runs with its effective security context, so a setting inherited
    /// through a `SQL SECURITY DEFINER` view's definer profile restricts the rows visible through
    /// the view just like a clause of its AST. The AST side (`settingsClauseCanHideRows`) is an
    /// allowlist of execution-only settings; here the whole settings set is always populated, so
    /// the row-hiding settings that allowlist rejects are enumerated instead.
    const auto & settings = context->getSettingsRef();

    /// A profile-level limit or offset truncates the view's result.
    if (settings[Setting::limit] != 0 || settings[Setting::offset] != 0)
        return true;

    /// `additional_result_filter` grows a filter step on top of the inner query's result (the
    /// inner interpreter runs at subquery depth 0), so it hides rows of any query just like a
    /// clause of the view's AST. `additional_table_filters` is deliberately absent: its entries
    /// are keyed by table and hide rows only of a query that reads one of those tables, so
    /// `canHideRows` matches them against the source table once it has resolved it. Checking the
    /// setting here would make every `SQL SECURITY DEFINER` view whose definer profile filters
    /// some unrelated table a barrier for no semantic reason.
    if (!settings[Setting::additional_result_filter].value.empty())
        return true;

    /// `final` makes every source read of the inner query a `FINAL` read, which hides the
    /// overwritten and deleted versions of a row exactly like a `FINAL` clause in the AST.
    if (settings[Setting::final])
        return true;

    /// A quota-like limit with a non-throwing overflow mode stops the query early and returns the
    /// rows read so far, so the view exposes an arbitrary prefix of its rows instead of all of
    /// them. Every such pair hides rows; with the default `throw` mode none of them do.
    /// `max_result_rows` / `max_result_bytes` are deliberately absent: `getViewSubqueryContext`
    /// resets them for the view's own subquery, so they never truncate the inner query's result.
    const bool read_breaks = settings[Setting::read_overflow_mode] == OverflowMode::BREAK
        && (settings[Setting::max_rows_to_read] != 0 || settings[Setting::max_bytes_to_read] != 0);
    const bool read_leaf_breaks = settings[Setting::read_overflow_mode_leaf] == OverflowMode::BREAK
        && (settings[Setting::max_rows_to_read_leaf] != 0 || settings[Setting::max_bytes_to_read_leaf] != 0);
    const bool timeout_breaks = settings[Setting::timeout_overflow_mode] == OverflowMode::BREAK
        && settings[Setting::max_execution_time].totalMilliseconds() != 0;
    const bool timeout_leaf_breaks = settings[Setting::timeout_overflow_mode_leaf] == OverflowMode::BREAK
        && settings[Setting::max_execution_time_leaf].totalMilliseconds() != 0;
    /// The `GROUP BY` / sort / `DISTINCT` overflow settings are deliberately absent: they cannot
    /// drop a row of a query that never aggregates, sorts or deduplicates, so they are checked by
    /// `shapeDependentOverflowCanHideRows` once the shape of the query is known. Checking them
    /// here would make every `SQL SECURITY DEFINER` view whose definer profile happens to carry
    /// `group_by_overflow_mode` or `distinct_overflow_mode` a barrier for no semantic reason.
    return read_breaks || read_leaf_breaks || timeout_breaks || timeout_leaf_breaks;
}

bool StorageView::shapeDependentOverflowCanHideRows(const ContextPtr & context, bool has_sort, bool has_grouping, bool has_distinct)
{
    const auto & settings = context->getSettingsRef();

    /// Each of these limits stops one operator early and returns what it has produced so far, so
    /// the query exposes an arbitrary subset of its rows - but only when the query contains that
    /// operator at all. With the default `throw` mode none of them hide anything either.
    const bool group_by_breaks = has_grouping
        && settings[Setting::group_by_overflow_mode] != OverflowMode::THROW
        && settings[Setting::max_rows_to_group_by] != 0;
    const bool sort_breaks = has_sort
        && settings[Setting::sort_overflow_mode] == OverflowMode::BREAK
        && (settings[Setting::max_rows_to_sort] != 0 || settings[Setting::max_bytes_to_sort] != 0);
    const bool distinct_breaks = has_distinct
        && settings[Setting::distinct_overflow_mode] == OverflowMode::BREAK
        && (settings[Setting::max_rows_in_distinct] != 0 || settings[Setting::max_bytes_in_distinct] != 0);

    return group_by_breaks || sort_breaks || distinct_breaks;
}

bool StorageView::canHideRows(const ASTPtr & inner_query, const ContextPtr & context)
{
    if (!inner_query)
        return true;

    /// The view is executed with its effective security context, which can hide rows on its own.
    /// Callers pass the view context, so this also covers `SQL SECURITY DEFINER` profiles.
    if (effectiveContextCanHideRows(context))
        return true;

    const auto * union_query = inner_query->as<ASTSelectWithUnionQuery>();
    if (!union_query || !union_query->list_of_selects)
        return true;

    /// `UNION DISTINCT`, `INTERSECT` and `EXCEPT` drop rows. `UNION ALL` does not, but a view
    /// made of several selects is not worth proving.
    if (union_query->list_of_selects->children.size() != 1)
        return true;

    const auto * select = union_query->list_of_selects->children.front()->as<ASTSelectQuery>();
    if (!select)
        return true;

    /// `GROUP BY ALL` uses a flag instead of a non-empty `groupBy` expression list, so it must
    /// fail closed just like its explicit counterpart. `LIMIT n AFTER expr UNTIL expr` selects a
    /// range of the sorted result, so it hides every row outside that range exactly like
    /// `LIMIT` / `OFFSET` do. The `SETTINGS` clause of the query is checked below, once the
    /// source table it may filter is known.
    if (select->distinct
        || select->where() || select->prewhere() || select->having() || select->qualify()
        || select->groupBy() || select->group_by_all
        || select->limitLength() || select->limitOffset() || select->limitByLength() || select->limitByOffset()
        || select->limitAfter() || select->limitUntil())
        return true;

    /// `ARRAY JOIN` drops rows with an empty array (and `LEFT ARRAY JOIN` is not worth
    /// distinguishing), an aggregation without `GROUP BY` collapses all rows into one, and the
    /// `arrayJoin` function does what the clause does without appearing in the clause list -
    /// possibly behind a SQL user-defined function that is expanded only later.
    if (select->arrayJoinExpressionList().first || hasRowHidingFunctionOutsideSubqueries(*select))
        return true;

    /// Now that the shape of the query is known, the overflow-mode settings of the effective
    /// context that depend on it can be checked as well. Everything that aggregates or
    /// deduplicates has already failed closed above, so in practice only an inner `ORDER BY`
    /// combined with a definer profile `sort_overflow_mode = 'break'` is left; the flags are
    /// spelled out anyway so that the proof does not silently weaken if a check above is relaxed.
    if (shapeDependentOverflowCanHideRows(
            context,
            /*has_sort=*/ select->orderBy() != nullptr || select->order_by_all,
            /*has_grouping=*/ select->groupBy() != nullptr || select->group_by_all || select->having() != nullptr,
            /*has_distinct=*/ select->distinct))
        return true;

    /// A query setting may introduce a limit or otherwise change which rows the view exposes, so a
    /// `SETTINGS` clause fails closed just like the explicit clauses above - but a clause of pure
    /// execution tuning (`SETTINGS max_threads = 1`) must not turn a projection-only view into a
    /// barrier. An `additional_table_filters` entry hides rows only of a query that reads the
    /// table it names, so it is matched against the source table below; a query whose source is
    /// not a single plainly named table (no `FROM`, a subquery) has no such table to match it
    /// against and fails closed on it.
    /// The shape-dependent overflow settings of the clause are gated by the same shape flags as
    /// the ones of the effective context above.
    const auto & settings_clause = select->settings();
    const bool has_sort = select->orderBy() != nullptr || select->order_by_all;
    const bool has_grouping = select->groupBy() != nullptr || select->group_by_all || select->having() != nullptr;
    const bool has_distinct = select->distinct;

    const auto & tables = select->tables();
    if (!tables || tables->children.empty())
        return settingsClauseCanHideRows(settings_clause, has_sort, has_grouping, has_distinct);   /// A `SELECT` without `FROM` reads nothing it could hide.

    /// Any `JOIN` changes which rows are observable below the view.
    if (tables->children.size() != 1)
        return true;

    const auto * element = tables->children.front()->as<ASTTablesInSelectQueryElement>();
    if (!element || element->table_join || element->array_join || !element->table_expression)
        return true;

    const auto * table_expression = element->table_expression->as<ASTTableExpression>();
    if (!table_expression)
        return true;

    /// `SAMPLE` drops rows, and `FINAL` hides the overwritten versions of a row.
    if (table_expression->sample_size || table_expression->final)
        return true;

    if (table_expression->subquery)
    {
        /// The clause applies to the nested query as well, whose source and shape this level does
        /// not see, so every shape-dependent setting fails closed here.
        if (settingsClauseCanHideRows(settings_clause, /*has_sort=*/ true, /*has_grouping=*/ true, /*has_distinct=*/ true))
            return true;

        const auto & subquery_children = table_expression->subquery->children;
        return subquery_children.empty() || canHideRows(subquery_children.front(), context);
    }

    /// A table function may wrap another view (`view`, `viewIfPermitted`, `merge`, ...).
    if (!table_expression->database_and_table_name)
        return true;

    const auto * identifier = table_expression->database_and_table_name->as<ASTTableIdentifier>();
    if (!identifier)
        return true;

    /// `CREATE VIEW` qualifies the table names of the inner query, so an unqualified name is an
    /// oddity not worth resolving against the right current database here.
    auto table_id = identifier->getTableId();
    if (table_id.database_name.empty())
        return true;

    auto table = DatabaseCatalog::instance().tryGetTable(table_id, context);
    if (!table)
        return true;

    /// A proxy (a lazily loaded table of a database with `lazy_load_tables`, or a table created
    /// from a table function) and an `Alias` table forward `read` to another storage while
    /// reporting their own engine, so classify the storage that actually serves the read.
    /// Fail closed on a chain that cannot be resolved. Every table of the chain is remembered:
    /// a read through an `Alias` applies the row policies of the `Alias` and of its target
    /// together (`getEffectiveRowPolicyFilter`, `InterpreterSelectQuery`), so a policy defined on
    /// the `Alias` alone hides rows just like one defined on the table that serves the read.
    std::vector<StorageID> chain_storage_ids;
    for (size_t depth = 0;; ++depth)
    {
        if (depth >= 16)
            return true;

        chain_storage_ids.push_back(table->getStorageID());

        if (const auto * proxy = dynamic_cast<const StorageProxy *>(table.get()))
            table = proxy->getNested();
        else if (const auto * alias = typeid_cast<const StorageAlias *>(table.get()))
            table = alias->tryGetTargetTable();
        else
            break;

        if (!table)
            return true;
    }

    /// An `additional_table_filters` entry keyed by the source table - by the name the view's
    /// query uses, by its alias, or by the storage id of the table that name resolves to - is
    /// applied to the source read exactly like a `WHERE` of the view's query. The entry may come
    /// from the effective context (a definer profile) or from the `SETTINGS` clause of the view's
    /// own query; both are matched exactly the way the interpreters match them
    /// (`parseAdditionalFilterConditionForTable`, `parseAdditionalFilterAstIfNeeded`), so that an
    /// entry for an unrelated table does not turn a projection-only view into a barrier. An entry
    /// keyed by the target of a proxy or `Alias` table is such an unrelated entry: the interpreters
    /// match the table expression of the query, which is the `Alias` itself, and `StorageAlias::read`
    /// and `StorageProxy::read` forward the already parsed filter without matching it again.
    const std::vector<StorageID> source_table_ids = {table_id, chain_storage_ids.front()};
    const String source_alias = identifier->tryGetAlias();
    const auto additional_table_filters_apply = [&](const Field & additional_table_filters)
    {
        return additionalTableFiltersApplyTo(additional_table_filters, source_table_ids, source_alias, context->getCurrentDatabase());
    };
    if (settingsClauseCanHideRows(settings_clause, has_sort, has_grouping, has_distinct, additional_table_filters_apply)
        || additional_table_filters_apply(context->getSettingsRef()[Setting::additional_table_filters].value))
        return true;

    /// A view can hide rows of its own, and these engines read other tables, which may be views.
    const auto & engine = table->getName();
    if (table->isView() || table->isRemote() || engine == "Merge" || engine == "Buffer")
        return true;

    /// `MaterializedPostgreSQL` rewrites every read of its data with `FINAL` and a `_sign = 1`
    /// filter, so it hides the overwritten and deleted versions of a row just like a filtering
    /// view does, and outer predicates must stay above it.
    if (table->needRewriteQueryWithFinal({}))
        return true;

    /// Row policies are evaluated as part of the source read. They are not represented in the
    /// stored view AST, so inspect them under the effective context that runs the inner query,
    /// for the table that serves the read and for every `Alias` (or proxy) the read went through.
    for (const auto & storage_id : chain_storage_ids)
    {
        auto row_policy_filter = context->getRowPolicyFilter(
            storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
        if (row_policy_filter && !row_policy_filter->isAlwaysTrue())
            return true;
    }

    return false;
}

ContextPtr StorageView::getViewSubqueryContext(ContextPtr context, const StorageSnapshotPtr &storage_snapshot)
{
    auto view_context = storage_snapshot->metadata->getSQLSecurityOverriddenContext(context);
    Settings view_settings = view_context->getSettingsCopy();
    view_settings[Setting::max_result_rows] = 0;
    view_settings[Setting::max_result_bytes] = 0;
    view_settings[Setting::extremes] = false;
    view_context->setSettings(view_settings);
    /// The inlined body is resolved on the node that expands the view, exactly like the
    /// `StorageView::read` path (see `getViewContext`): on a shard (`SECONDARY_QUERY`) or on a
    /// local plan of the initiator, `QueryAnalyzer::replaceNodesWithPositionalArguments` skips
    /// positional arguments as already resolved by the initiator unless the context is marked
    /// as a view inner query - the initiator never saw the view body, so `GROUP BY 1` inside it
    /// would stay a literal and mis-group the rows or throw `NOT_AN_AGGREGATE`.
    view_context->setIsViewInnerQuery(true);
    return view_context;
}

}
