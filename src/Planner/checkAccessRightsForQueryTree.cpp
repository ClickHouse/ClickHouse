#include <Planner/checkAccessRightsForQueryTree.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Planner/Utils.h>
#include <Planner/collectSelectedColumnsFromTable.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageView.h>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
}

bool checkReadTimeAccessForTableExpression(
    const StoragePtr & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const Names & column_names,
    const ContextPtr & scope_context,
    bool skip_if_unresolvable)
{
    /// An `Alias` table also requires `SELECT` on the same columns of its target table. Real execution
    /// enforces that in `StorageAlias::read` when the plan is built, so reproduce it through the same
    /// helper `read` uses. A trivial read (empty `column_names`) is covered by the caller's any-column
    /// fallback, which admits an `Alias` column only when its target column is granted too.
    if (const auto * alias = storage->as<StorageAlias>(); alias && !column_names.empty())
        alias->getTargetTable(StorageAlias::TargetAccess{scope_context, AccessType::SELECT, column_names});

    /// A view that is not inlined stays as a single table expression, so its inner query - and the base
    /// tables it reads - never appear in the tree being checked. Real execution still reads them:
    /// `StorageView::readImpl` builds the view's inner query under the view's own context and checks the
    /// base-table privileges there. `checkViewBaseTableAccess` reproduces that inner pass so a user with
    /// `SELECT` on the view but not on its base tables is denied, exactly as a plain `SELECT` through the
    /// view is.
    ///
    /// This covers both a regular, non-parameterized view that is not inlined (`analyzer_inline_views = 0`,
    /// the default) and a parameterized view, whose `TableFunctionNode` storage already holds the
    /// parameter-substituted inner query and the view's SQL security, so exactly the same recursive pass
    /// applies. Without it a user with `SELECT` on the parameterized view but not on its base table could
    /// `EXPLAIN QUERY TREE SELECT * FROM pv(...)` (or `EXPLAIN SYNTAX` when expansion is skipped for
    /// `FINAL` / `SAMPLE` / `SQL SECURITY DEFINER` / `NONE`) and read metadata that real execution rejects
    /// in `StorageView::readImpl`. Inlined views already expose their base tables as `TableNode`s that the
    /// caller checks itself, so they are not double-checked here.
    if (typeid_cast<const StorageView *>(storage.get()))
        return checkViewBaseTableAccess(storage, storage_snapshot, scope_context, column_names, skip_if_unresolvable);

    return true;
}

bool checkAccessRightsForQueryTree(QueryTreeNodePtr & query_tree, const ContextPtr & query_context, bool skip_if_unresolvable)
{
    /// Collect every table referenced anywhere in the query tree, including inside subqueries in
    /// expressions (e.g. `WHERE x IN (SELECT ... FROM t)`), which `extractAllTableReferences` skips.
    /// Each table is paired with the context of the scope it appears in: subtrees do not all execute
    /// under the top-level `query_context`. When a view is inlined (`analyzer_inline_views`), its body
    /// is resolved under `StorageView::getViewSubqueryContext`, and the planner checks the base-table
    /// privileges with that per-scope context (see `buildPlannerContext` /
    /// `prepareBuildQueryPlanForTableExpression`, which use `QueryNode::getContext`). Reproducing the
    /// planner faithfully requires checking each table with the context of its own scope; otherwise a
    /// valid `SQL SECURITY DEFINER` / `NONE` view explain would be denied because the user lacks direct
    /// access to the base table. `InDepthQueryTreeVisitorWithContext` tracks the scope context as it
    /// descends into `QueryNode` / `UnionNode` children.
    ///
    /// A parameterized view is not a `TableNode`: `QueryAnalyzer::resolveTableFunction` resolves `pv(...)`
    /// into a `TableFunctionNode` that owns the view's storage (built by
    /// `Context::buildParameterizedViewStorage`). Real execution checks `SELECT` on that view object all
    /// the same - the planner does it for exactly this node type in
    /// `prepareBuildQueryPlanForTableExpression` - so such nodes are collected here too. Every other table
    /// function is skipped, as in the planner: its own access check happens in `ITableFunction::execute`.
    class CollectTablesVisitor : public InDepthQueryTreeVisitorWithContext<CollectTablesVisitor>
    {
    public:
        struct TableExpression
        {
            IQueryTreeNode * node;
            StoragePtr storage;
            StorageID storage_id;
            StorageSnapshotPtr storage_snapshot;
            ContextPtr scope_context;
        };

        explicit CollectTablesVisitor(const ContextPtr & context)
            : InDepthQueryTreeVisitorWithContext(context)
        {
        }

        void enterImpl(QueryTreeNodePtr & node)
        {
            if (auto * table_node = node->as<TableNode>())
            {
                tables.emplace_back(TableExpression{
                    table_node, table_node->getStorage(), table_node->getStorageID(), table_node->getStorageSnapshot(), getContext()});
                return;
            }

            if (auto * table_function_node = node->as<TableFunctionNode>())
            {
                const auto & storage = table_function_node->getStorage();
                const auto * storage_view = storage ? storage->as<StorageView>() : nullptr;
                if (storage_view && storage_view->isParameterizedView())
                    tables.emplace_back(TableExpression{
                        table_function_node,
                        storage,
                        table_function_node->getStorageID(),
                        table_function_node->getStorageSnapshot(),
                        getContext()});
            }
        }

        std::vector<TableExpression> tables;
    };

    CollectTablesVisitor visitor(query_context);
    visitor.visit(query_tree);

    /// A table expression node instance is unique per scope, so each entry identifies one table in one
    /// scope. Guard against the same node being visited twice (shared subtrees) so we check it only once.
    std::unordered_set<const IQueryTreeNode *> checked_tables;
    bool view_checks_performed = true;
    for (auto & [node, storage, storage_id, storage_snapshot, scope_context] : visitor.tables)
    {
        /// StorageDummy is created on preliminary stages; ignore access check for it (as the planner does).
        if (typeid_cast<const StorageDummy *>(storage.get()))
            continue;

        if (!checked_tables.emplace(node).second)
            continue;

        /// Columns selected from this specific table instance, so a base table referenced both directly
        /// and through an inlined view is checked with the right column set in each scope.
        auto column_names = collectSelectedColumnsForTableExpression(query_tree, *node, storage_id, scope_context);
        const auto * alias = storage->as<StorageAlias>();
        if (!column_names.empty())
        {
            /// In case of cross-replication we don't know what database is used for the table on the
            /// initiator, so the explicit-column check is skipped there, exactly as the planner does
            /// (`PlannerJoinTree::checkAccessRights` only guards this branch with `hasDatabase`).
            if (storage_id.hasDatabase())
                scope_context->checkAccess(AccessType::SELECT, storage_id, column_names);
        }
        else
        {
            /// For trivial queries like "SELECT count() FROM table" access is granted if at least one column is accessible.
            /// This fallback runs even for empty-database table nodes: the planner enforces it unconditionally, and
            /// `ContextAccess` resolves an empty database name to the current database, so skipping it here (as the
            /// early `hasDatabase` guard used to) would let `count()`-style queries bypass the check.
            /// An `Alias` column counts as accessible only when the same column of its target table is also
            /// granted, exactly as the planner's `checkAccessRights` does.
            auto access = scope_context->getAccess();
            bool has_accessible_column = false;
            for (const auto & column : storage_snapshot->metadata->getColumns())
            {
                if (access->isGranted(AccessType::SELECT, storage_id.database_name, storage_id.table_name, column.name)
                    && (!alias || alias->isTargetTableGranted(scope_context, AccessType::SELECT, column.name)))
                {
                    has_accessible_column = true;
                    break;
                }
            }

            if (!has_accessible_column)
                throw Exception(ErrorCodes::ACCESS_DENIED,
                    "{}: Not enough privileges. To execute this query, it's necessary to have the grant SELECT for at least one column on {}",
                    scope_context->getUserName(),
                    storage_id.getFullTableName());
        }

        /// The check above enforces `SELECT` on this table (or view) object, as the planner does for every
        /// table expression it plans. Real execution then performs more checks when the plan is built,
        /// which a tree that is never read skips: the `Alias` target-table check and the recursive
        /// base-table pass of a non-inlined view. That pass skips itself (returning `false`) when the
        /// view's inner query cannot be resolved by the analyzer, so no base-table `SELECT` check ever ran
        /// for that view. Propagate that to the caller instead of dropping it: dumping the resolved tree
        /// then would hand out resolved metadata after only the view-object grant, while the legacy
        /// `EXPLAIN SYNTAX` formatter fail-closes for exactly this case by keeping the view unexpanded.
        view_checks_performed
            = checkReadTimeAccessForTableExpression(storage, storage_snapshot, column_names, scope_context, skip_if_unresolvable)
            && view_checks_performed;
    }

    return view_checks_performed;
}

bool resolveThenCheckAccessRights(
    QueryTreeNodePtr query_tree, QueryTreePassManager & pass_manager, const ContextPtr & query_context, bool skip_if_unresolvable)
{
    bool resolved = false;
    try
    {
        pass_manager.runOnlyResolve(query_tree);
        resolved = true;
    }
    catch (const Exception & e)
    {
        if (!skip_if_unresolvable || e.code() == ErrorCodes::ACCESS_DENIED)
            throw;
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok to swallow when the caller asked for it: a non-ClickHouse exception (e.g. a remote table
        /// function that fails to connect while being resolved) is not an access denial. As above there
        /// is no resolved metadata to protect, so the check is skipped rather than turning a formatting
        /// request into an error.
        if (!skip_if_unresolvable)
            throw;
    }

    return resolved && checkAccessRightsForQueryTree(query_tree, query_context, skip_if_unresolvable);
}

bool checkViewBaseTableAccess(
    const StoragePtr & view_storage,
    const StorageSnapshotPtr & view_snapshot,
    const ContextPtr & scope_context,
    const Names & column_names,
    bool skip_if_unresolvable)
{
    auto view_context = StorageView::getViewSubqueryContext(scope_context, view_snapshot);
    ASTPtr inner_query = view_snapshot->metadata->getSelectQuery().inner_query->clone();

    /// The columns to request from the view. For a normal read they are the columns the outer query
    /// selects from the view. For a trivial read (e.g. `SELECT count() FROM v`) that asks for no specific
    /// view column, real execution does not resolve the whole view body: the planner picks one cheapest
    /// readable view column in `prepareBuildQueryPlanForTableExpression` (via
    /// `chooseSmallestColumnToReadFromStorage`, restricted to the columns the user may read) and
    /// `StorageView::readImpl` passes only that column into the inner query. Reproduce the same choice here
    /// so a user with `SELECT` on the view and only column-level access to the base table is not over-denied.
    Names columns_to_read = column_names;
    if (columns_to_read.empty())
    {
        const auto & storage_id = view_snapshot->storage.getStorageID();
        auto access = scope_context->getAccess();
        NameSet columns_allowed_to_select;
        for (const auto & column : view_snapshot->metadata->getColumns())
            if (access->isGranted(AccessType::SELECT, storage_id.database_name, storage_id.table_name, column.name))
                columns_allowed_to_select.insert(column.name);

        columns_to_read.push_back(
            chooseSmallestColumnToReadFromStorage(view_storage, view_snapshot, columns_allowed_to_select).name);
    }

    /// Wrap the inner query so only `columns_to_read` are selected from it, exactly as
    /// `InterpreterSelectQueryAnalyzer` / `InterpreterSelectWithUnionQuery` do for a real read through the
    /// view (see `StorageView::readImpl`). Resolving the wrapped query still runs the usual query tree
    /// passes, including the one that prunes subquery output columns the outer projection does not use, so
    /// base-table columns the view happens to select but this particular read never asked for are not
    /// resolved - and therefore not checked - matching what real execution actually reads.
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        auto table_expression_ast = make_intrusive<ASTTableExpression>();
        table_expression_ast->children.push_back(make_intrusive<ASTSubquery>(std::move(inner_query)));
        table_expression_ast->subquery = table_expression_ast->children.back();

        auto tables_in_select_query_element_ast = make_intrusive<ASTTablesInSelectQueryElement>();
        tables_in_select_query_element_ast->children.push_back(std::move(table_expression_ast));
        tables_in_select_query_element_ast->table_expression = tables_in_select_query_element_ast->children.back();

        ASTPtr tables_in_select_query_ast = make_intrusive<ASTTablesInSelectQuery>();
        tables_in_select_query_ast->children.push_back(std::move(tables_in_select_query_element_ast));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_in_select_query_ast));

        auto projection_expression_list_ast = make_intrusive<ASTExpressionList>();
        projection_expression_list_ast->children.reserve(columns_to_read.size());
        for (const auto & column_name : columns_to_read)
            /// Build the projected identifier the same way `normalizeAndValidateQuery` does, so a view output
            /// whose name is a compound or subcolumn identifier (`n.x`, `arr.size0`, tuple elements, ...) gets
            /// the correct multi-part identifier shape. A plain `ASTIdentifier(column_name)` would carry the
            /// whole dotted name as a single part, fail to resolve, and - because `resolveThenCheckAccessRights`
            /// swallows non-`ACCESS_DENIED` resolution errors - silently skip the base-table access check.
            projection_expression_list_ast->children.push_back(createIdentifierFromColumnName(column_name));
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(projection_expression_list_ast));

        inner_query = std::move(select_query);
    }

    auto view_query_tree = buildQueryTree(inner_query, view_context);
    QueryTreePassManager view_pass_manager(view_context);
    addQueryTreePasses(view_pass_manager);
    return resolveThenCheckAccessRights(std::move(view_query_tree), view_pass_manager, view_context, skip_if_unresolvable);
}

}
