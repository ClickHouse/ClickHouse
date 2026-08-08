#include <Core/Block.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterExplainQuery.h>

#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/ExecutingGraph.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/TableOverrideUtils.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Formats/FormatFactory.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/FunctionParameterValuesVisitor.h>
#include <Parsers/FunctionSecretArgumentsFinder.h>

#include <Access/Common/SQLSecurityDefs.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/StorageView.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sources/DelayedSource.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/AnalyzePlanStats.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/StepWallClockRegistry.h>
#include <QueryPipeline/printPipeline.h>

#include <Common/CurrentThread.h>
#include <Common/JSONBuilder.h>
#include <Common/ThreadStatus.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Core/Settings.h>
#include <Interpreters/HypotheticalIndexStore.h>
#include <Storages/MergeTree/WhatIfIndexEstimator.h>

#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionSecretArgumentsFinderTreeNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/Utils.h>
#include <Planner/collectSelectedColumnsFromTable.h>
#include <Planner/Utils.h>
#include <Access/ContextAccess.h>
#include <Access/Common/AccessFlags.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageSnapshot.h>


namespace ProfileEvents
{
    extern const Event SelectedRows;
    extern const Event SelectedBytes;
}


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool format_display_secrets_in_show_and_select;
    extern const SettingsUInt64 query_plan_max_step_description_length;
    extern const SettingsUInt64 interactive_delay;
    extern const SettingsBool make_distributed_plan;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsExplainQueryPlanDefault explain_query_plan_default;
}

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int UNKNOWN_SETTING;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int ACCESS_DENIED;
}

/// Forward declaration: reproduces the base-table access check `StorageView::readImpl` performs when
/// actually reading through a regular (non-parameterized) view, by resolving the view's inner query under
/// its own security context and running the same per-table check recursively (covering nested views too).
/// `column_names` are the columns requested from the view itself, matching the `column_names` real
/// execution passes into `InterpreterSelectQueryAnalyzer` / `InterpreterSelectWithUnionQuery` for the
/// view's inner query - so a user who can read only some of the view's output columns is not also required
/// to have access to base columns the view happens to select but this particular read never asked for.
/// When `column_names` is empty (a trivial read such as `SELECT count() FROM v` that asks for no specific
/// view column), the same single cheapest readable view column the planner would pick
/// (`chooseSmallestColumnToReadFromStorage`) is used, mirroring `prepareBuildQueryPlanForTableExpression`.
/// `view_storage` / `view_snapshot` are the view's storage and snapshot. `checkAccessRightsForQueryTree`
/// further down uses it for the analyzer path; `ExplainAnalyzedSyntaxMatcher` below uses it for the legacy
/// AST-based path, so both are consistent. Declared `static` (rather than in one of this file's unnamed
/// namespaces) so this one declaration and its later definition - which needs symbols from the second
/// unnamed namespace below - refer to the same internal-linkage entity throughout.
///
/// Returns whether the base-table access check was actually performed. It is skipped (returning `false`)
/// when the view's inner query cannot be resolved by the analyzer - the same "format but do not resolve"
/// shapes (`NOT_IMPLEMENTED`, `BAD_ARGUMENTS`, remote table-function connection errors, ...) that
/// `resolveThenCheckAccessRights` handles for the top-level query. A legacy-path caller about to expand
/// and dump the view body must then fall back to the unexpanded view reference, since no `SELECT` check
/// on the base tables ever ran.
static bool checkViewBaseTableAccess(
    const StoragePtr & view_storage, const StorageSnapshotPtr & view_snapshot, const ContextPtr & scope_context, const Names & column_names);

namespace
{
    /// Walk the AST and expand parameterized view "table function" calls into their inlined,
    /// parameter-substituted subqueries, so `EXPLAIN SYNTAX` shows the resolved query.
    ///
    /// In the analyzer path, without this expansion the query tree would show the unexpanded
    /// table function call. In the legacy path, `ExplainAnalyzedSyntaxMatcher` covers the FROM
    /// table expression via `StorageView::replaceWithSubquery`, which only rewrites the first
    /// table expression and leaves JOIN sides untouched; this visitor is complementary, expanding
    /// parameterized views that appear on the right side of a JOIN as well.
    struct ExpandParameterizedViewsMatcher
    {
        struct Data : public WithContext
        {
            explicit Data(ContextPtr context_) : WithContext(context_) {}

            /// Set to true once at least one parameterized view has been expanded. `EXPLAIN SYNTAX`
            /// then runs the access check on the original (unexpanded) query, where the parameterized
            /// view still resolves to its own `TableNode` so the view object's `SELECT` grant is
            /// enforced, exactly as real execution does (see the access-check call site).
            bool expanded_parameterized_view = false;

            /// Set to true once the query has been found to reference a parameterized view at all,
            /// whether or not the call was expanded above. Expansion is deliberately skipped for
            /// `FINAL` / `SAMPLE` and for `SQL SECURITY DEFINER` / `NONE` views, but the legacy
            /// rewriting visitor used by `EXPLAIN AST optimize = 1` still inlines the view body, so
            /// the `SELECT` grant on the view object has to be enforced for those calls as well.
            bool referenced_parameterized_view = false;
        };

        static bool needChildVisit(ASTPtr &, ASTPtr &)
        {
            return true;
        }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (auto * select = ast->as<ASTSelectQuery>())
                expandTables(*select, data);
        }

        /// Iterate all table expressions in the SELECT (FROM and JOINs) and expand
        /// any that are parameterized view calls.
        static void expandTables(ASTSelectQuery & select, Data & data)
        {
            if (!select.tables() || select.tables()->children.empty())
                return;

            for (auto & child : select.tables()->children)
            {
                auto * table_element = child->as<ASTTablesInSelectQueryElement>();
                if (!table_element || !table_element->table_expression)
                    continue;

                auto * table_expr = table_element->table_expression->as<ASTTableExpression>();
                if (!table_expr || !table_expr->table_function)
                    continue;

                tryExpandTableExpression(*table_expr, data);
            }
        }

        /// If the table expression is a parameterized view call, replace it with
        /// the parameter-substituted inner query as a subquery.
        static void tryExpandTableExpression(ASTTableExpression & table_expr, Data & data)
        {
            const auto * func = table_expr.table_function->as<ASTFunction>();
            if (!func)
                return;

            auto query_context = data.getContext()->getQueryContext();

            /// A registered table function (e.g. `numbers`) takes precedence over a view with
            /// the same name, matching `QueryAnalyzer::resolveTableFunction`. Without this check
            /// a user view shadowing a built-in table function would be expanded here while
            /// regular execution would still resolve the built-in.
            if (TableFunctionFactory::instance().isTableFunctionName(func->name))
                return;

            String database_name = query_context->getCurrentDatabase();
            String table_name = func->name;
            if (func->isCompoundName())
            {
                std::vector<std::string> parts;
                splitInto<'.'>(parts, func->name);
                if (parts.size() != 2)
                    return;
                database_name = parts[0];
                table_name = parts[1];
            }

            auto storage = DatabaseCatalog::instance().tryGetTable({database_name, table_name}, query_context);
            if (!storage)
                return;

            const auto * storage_view = storage->as<StorageView>();
            if (!storage_view || !storage_view->isParameterizedView())
                return;

            data.referenced_parameterized_view = true;

            /// FINAL and SAMPLE are valid on a parameterized view at execution time, but
            /// rewriting the view call into a subquery here would attach them to the
            /// subquery, where they are rejected with `UNSUPPORTED_METHOD`. Leave the
            /// original call intact so `EXPLAIN SYNTAX` matches what execution accepts.
            if (table_expr.final || table_expr.sample_size || table_expr.sample_offset)
                return;

            auto metadata = storage->getInMemoryMetadataPtr(query_context, false);

            /// For views created with `SQL SECURITY DEFINER` or `NONE`, execution resolves the
            /// inner tables via `StorageView::getSQLSecurityOverriddenContext`. Inlining the view
            /// here would instead re-analyze the inner query under the invoker's context, so
            /// `EXPLAIN SYNTAX` would fail for users that can query the view but not its inner
            /// tables. Leave the original parameterized call intact in that case.
            if (metadata->sql_security_type && metadata->sql_security_type != SQLSecurityType::INVOKER)
                return;

            auto view_query = metadata->getSelectQuery().inner_query->clone();
            NameToNameMap parameter_values = analyzeFunctionParamValues(table_expr.table_function, query_context);
            StorageView::replaceQueryParametersIfParameterizedView(view_query, parameter_values);

            /// Replace the table function with a subquery in-place on this table expression,
            /// rather than using `StorageView::replaceWithSubquery` which only handles the
            /// first table expression in the SELECT. Preserve the explicit alias from the
            /// original table function (e.g. `... FROM my_pv(n=1) AS t`) so identifiers in
            /// the outer query keep resolving; otherwise fall back to the view's table name
            /// so the rendered `EXPLAIN SYNTAX` keeps referring to the view.
            String alias = table_expr.table_function->tryGetAlias();
            if (alias.empty())
                alias = table_name;

            table_expr.table_function = nullptr;
            table_expr.subquery = make_intrusive<ASTSubquery>(std::move(view_query));
            table_expr.subquery->setAlias(alias);

            table_expr.children.clear();
            table_expr.children.push_back(table_expr.subquery);

            data.expanded_parameterized_view = true;
        }
    };

    using ExpandParameterizedViewsVisitor = InDepthNodeVisitor<ExpandParameterizedViewsMatcher, true>;

    struct ExplainAnalyzedSyntaxMatcher
    {
        struct Data : public WithContext
        {
            explicit Data(ContextPtr context_) : WithContext(context_) {}
        };

        static bool needChildVisit(ASTPtr & node, ASTPtr &)
        {
            return !node->as<ASTSelectQuery>();
        }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (auto * select = ast->as<ASTSelectQuery>())
                visit(*select, ast, data);
        }

        /// `InterpreterSelectQuery::analyze` (called below) only checks `SELECT` on the view object itself
        /// (`checkAccessRightsForSelect`, using each view's un-rewritten `table_id`). A real `SELECT` through
        /// a regular, non-parameterized view goes on to check the base-table privileges too, but only once
        /// the pipeline is actually built, in `StorageView::readImpl` - which `EXPLAIN SYNTAX` never reaches.
        /// Look up every view referenced straight from the AST (before the FROM table gets rewritten into a
        /// subquery below) and reproduce that inner check, so a user with `SELECT` on the view but not on its
        /// base table is denied here too, instead of leaking the expanded view body.
        ///
        /// This iterates all table expressions - the FROM table and every JOIN side - not just the leftmost
        /// one: `query_info.view_query` and `StorageView::replaceWithSubquery` only handle the first table
        /// expression, so a `SQL SECURITY INVOKER` view on the JOIN side would otherwise never be checked.
        ///
        /// `main_table_column_names` are the columns real execution requests from the FROM (leftmost) storage,
        /// so a FROM-side view is checked with column-level precision. `query_info.view_query` is populated
        /// only for that main storage, so those columns apply to the leftmost table expression only. For a
        /// JOIN-side view the same column set real execution reads is derived from the analyzed `TableJoin`,
        /// exactly the way `buildJoinedPlan` derives the columns it passes into `interpretSubquery` (and from
        /// there into `StorageView::readImpl`): the join key columns plus the joined columns the query uses,
        /// mapped back to their original (unqualified) names. Using anything less (e.g. the trivial-read
        /// fallback) would let `EXPLAIN SYNTAX` pass with a partial column-level grant where the real
        /// `SELECT` still needs a column the user may not read, and must deny.
        ///
        /// Parameterized views are excluded: their base-table access is already enforced by the recursive
        /// `InterpreterSelectQuery` analysis of the subquery `ExpandParameterizedViewsMatcher` expanded them into.
        ///
        /// Returns whether the base-table check ran for the main (FROM, leftmost) regular view - the only one
        /// `StorageView::replaceWithSubquery` below expands and dumps. When it did not (the view's inner query
        /// is legacy-explainable but analyzer-unresolvable, so `checkViewBaseTableAccess` skipped the check),
        /// the caller must leave the view reference unexpanded rather than leak its body. JOIN-side views are
        /// never expanded in the legacy path, so a skipped check there reveals nothing and does not affect the
        /// result.
        static bool checkNonParameterizedViewBaseTableAccess(
            const ASTSelectQuery & select, const ContextPtr & context, const SelectQueryInfo & query_info, const Names & main_table_column_names)
        {
            bool main_view_access_check_performed = true;
            const auto table_expressions = getTableExpressions(select);
            for (size_t table_number = 0; table_number < table_expressions.size(); ++table_number)
            {
                const auto * table_expression = table_expressions[table_number];
                if (!table_expression || !table_expression->database_and_table_name)
                    continue;

                const auto * table_identifier = table_expression->database_and_table_name->as<ASTTableIdentifier>();
                if (!table_identifier)
                    continue;

                auto storage = DatabaseCatalog::instance().tryGetTable(context->resolveStorageID(table_identifier->getTableId()), context);
                const auto * view = storage ? typeid_cast<const StorageView *>(storage.get()) : nullptr;
                if (!view || view->isParameterizedView())
                    continue;

                const bool is_main_from_view = table_number == 0 && query_info.view_query && !query_info.is_parameterized_view;

                Names column_names;
                if (is_main_from_view)
                {
                    column_names = main_table_column_names;
                }
                else if (table_number > 0 && query_info.syntax_analyzer_result && query_info.syntax_analyzer_result->analyzed_join)
                {
                    /// Reproduce `buildJoinedPlan`: the columns real execution requests from the right side of
                    /// the join are the required inputs of the joined-block actions (the join key expressions)
                    /// plus every joined column the query uses, translated back to original storage names.
                    const auto & analyzed_join = *query_info.syntax_analyzer_result->analyzed_join;
                    auto joined_block_actions = analyzed_join.createJoinedBlockActions(context, std::make_shared<PreparedSets>());
                    for (const auto & required_column : analyzed_join.getRequiredColumns(
                             Block(joined_block_actions.getResultColumns()), joined_block_actions.getRequiredColumns().getNames()))
                        column_names.push_back(required_column.first);
                }

                auto metadata_snapshot = storage->getInMemoryMetadataPtr(context, false);
                const bool access_check_performed = checkViewBaseTableAccess(
                    storage, storage->getStorageSnapshotWithoutData(metadata_snapshot, context), context, column_names);

                if (is_main_from_view)
                    main_view_access_check_performed = access_check_performed;
            }
            return main_view_access_check_performed;
        }

        /// Whether this `SELECT` reads from a regular (non-parameterized) view directly, i.e. whether
        /// `checkNonParameterizedViewBaseTableAccess` would have anything to check for it. Used to keep the
        /// nested-subquery walk below from analyzing every subquery of every explained query.
        static bool referencesNonParameterizedView(const ASTSelectQuery & select, const ContextPtr & context)
        {
            for (const auto * table_expression : getTableExpressions(select))
            {
                if (!table_expression || !table_expression->database_and_table_name)
                    continue;

                const auto * table_identifier = table_expression->database_and_table_name->as<ASTTableIdentifier>();
                if (!table_identifier)
                    continue;

                /// A nested subquery is looked at before anything resolved it, so its table name may well not
                /// name a table at all - a `WITH` table of the enclosing query, an unknown database, ... Only
                /// a name that resolves to a view here is of interest; anything else is left to the query's
                /// own analysis, which must keep reporting whatever it reported before.
                auto storage_id = context->tryResolveStorageID(table_identifier->getTableId());
                if (!storage_id)
                    continue;

                auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);
                const auto * view = storage ? typeid_cast<const StorageView *>(storage.get()) : nullptr;
                if (view && !view->isParameterizedView())
                    return true;
            }
            return false;
        }

        static void collectNestedSelectQueries(const ASTPtr & node, ASTs & nested_selects)
        {
            for (const auto & child : node->children)
            {
                if (child->as<ASTSelectQuery>())
                    nested_selects.push_back(child);
                collectNestedSelectQueries(child, nested_selects);
            }
        }

        /// This visitor stops descending at an `ASTSelectQuery` (see `needChildVisit`), so the check above
        /// only ever runs for the outermost `SELECT` of the explained query. A regular `SQL SECURITY INVOKER`
        /// view read from a nested subquery - `WHERE x IN (SELECT ... FROM v)`, `FROM (SELECT ... FROM v)`,
        /// a scalar subquery, ... - would therefore never get its base tables checked: the outer
        /// `InterpreterSelectQuery::analyze` checks only `SELECT` on the view object, and the base-table
        /// denial of a real query happens in `StorageView::readImpl`, which `EXPLAIN` never reaches. Run the
        /// same check for every nested `SELECT` that reads from such a view, so `EXPLAIN SYNTAX` /
        /// `EXPLAIN AST optimize = 1` are denied exactly where the real query is.
        ///
        /// Each nested `SELECT` is analyzed on a copy, so this analysis cannot alter the query being dumped;
        /// nested view references are never expanded in the legacy path, and this does not expand them either.
        /// A nested `SELECT` the analysis cannot resolve on its own (e.g. one referring to a `WITH` table of
        /// the enclosing query) is left unchecked - as with an unresolvable view body, nothing is revealed,
        /// because the dump only ever prints the user's own subquery text. An `ACCESS_DENIED` is propagated.
        static void checkNestedSelectsViewBaseTableAccess(const ASTPtr & node, const ContextPtr & context)
        {
            ASTs nested_selects;
            collectNestedSelectQueries(node, nested_selects);
            if (nested_selects.empty())
                return;

            /// As in `checkAccessForExplainedSelect`: `joined_subquery_requires_alias` restricts how a query
            /// may be written and is not an access rule, but a subquery lifted out of its enclosing query can
            /// trip it. Relax it for these throwaway analyses only, so the check is not skipped.
            ContextMutablePtr check_context;

            for (const auto & nested_select_node : nested_selects)
            {
                const auto & nested_select = nested_select_node->as<const ASTSelectQuery &>();
                if (!referencesNonParameterizedView(nested_select, context))
                    continue;

                if (!check_context)
                {
                    check_context = Context::createCopy(context);
                    check_context->setSetting("joined_subquery_requires_alias", false);
                }

                ASTPtr nested_select_copy = nested_select_node->clone();
                try
                {
                    InterpreterSelectQuery interpreter(
                        nested_select_copy, check_context, SelectQueryOptions(QueryProcessingStage::FetchColumns).analyze().modify());

                    checkNonParameterizedViewBaseTableAccess(
                        nested_select_copy->as<const ASTSelectQuery &>(),
                        check_context,
                        interpreter.getQueryInfo(),
                        interpreter.getRequiredColumns());
                }
                catch (const Exception & e)
                {
                    if (e.code() == ErrorCodes::ACCESS_DENIED)
                        throw;
                }
            }
        }

        static void visit(ASTSelectQuery & select, ASTPtr & node, Data & data)
        {
            /// Check the views read from nested subqueries first, while the query is still exactly as the
            /// user wrote it - before the analysis below rewrites the main table expression in place.
            checkNestedSelectsViewBaseTableAccess(node, data.getContext());

            /// A parameterized view call carrying `FINAL` or `SAMPLE` must stay unexpanded, mirroring the
            /// skip in `ExpandParameterizedViewsMatcher`: those modifiers are valid on the view call at
            /// execution time, but attaching them to a subquery produces a form the executor rejects, so
            /// rewriting would make the `EXPLAIN` output non-executable even though the real `SELECT` is
            /// valid. The `analyze()`-mode interpreter below mutates the call in place (its own
            /// `StorageView::replaceWithSubquery` / `restoreViewName` round trip leaves a fake table
            /// identifier holding the view name instead of the original `pv(...)` call), so snapshot the
            /// original table expression up front and restore it afterwards.
            ASTPtr main_table_expression_backup;
            if (const auto * main_table_expression = getTableExpression(select, 0);
                main_table_expression && main_table_expression->table_function
                && (main_table_expression->final || main_table_expression->sample_size || main_table_expression->sample_offset))
                main_table_expression_backup = main_table_expression->clone();

            InterpreterSelectQuery interpreter(
                node, data.getContext(), SelectQueryOptions(QueryProcessingStage::FetchColumns).analyze().modify());

            const SelectQueryInfo & query_info = interpreter.getQueryInfo();

            /// `getRequiredColumns` reflects `syntax_analyzer_result->requiredSourceColumns()`, i.e. the same
            /// columns real execution would request from the FROM storage via `storage->read` (see
            /// `StorageView::readImpl`'s `column_names` parameter), computed by the `analyze()`-mode interpreter
            /// above from this very query. Run the base-table access check for every regular view referenced,
            /// including JOIN sides, before the FROM view (if any) is rewritten into a subquery.
            const bool main_view_access_check_performed
                = checkNonParameterizedViewBaseTableAccess(select, data.getContext(), query_info, interpreter.getRequiredColumns());

            /// Expand the FROM view body only when its base-table access check actually ran. If it was skipped
            /// (analyzer-unresolvable inner query), expanding would print the view body without any `SELECT`
            /// check on the base tables ever having happened - a metadata leak for a `SQL SECURITY INVOKER`
            /// view. Leaving the view reference unexpanded is the fail-safe fallback, matching how the
            /// parameterized-view path falls back to the unexpanded query in the same situation.
            ///
            /// The loop above cannot check a parameterized view: `InterpreterSelectQuery` resolves `pv(...)`
            /// into a freshly built, parameter-substituted storage that is not the view the AST names. Both
            /// callers therefore run `ExpandParameterizedViewsMatcher` before this visitor, so that the
            /// analysis above resolves the expanded body and checks its base tables; by then the view call is
            /// already a plain subquery and `query_info.view_query` is not set. What is left here are the
            /// views that matcher deliberately leaves intact - `SQL SECURITY DEFINER` and `NONE` - whose
            /// bodies a user granted `SELECT` on the view may see, exactly as for a regular view with those
            /// security settings, because their base tables are read under the view's own context.
            if (query_info.is_parameterized_view && main_table_expression_backup)
            {
                /// Put the original `pv(...) FINAL` / `pv(...) SAMPLE ...` call back in place of whatever
                /// the interpreter's in-place analysis left there (see the snapshot above), instead of
                /// expanding it into a subquery the executor would reject.
                auto & tables_element_ast = select.tables()->children.at(0);
                auto & tables_element = tables_element_ast->as<ASTTablesInSelectQueryElement &>();
                for (auto & child : tables_element.children)
                    if (child.get() == tables_element.table_expression.get())
                        child = main_table_expression_backup;
                tables_element.table_expression = main_table_expression_backup;
            }
            else if (query_info.view_query && main_view_access_check_performed)
            {
                ASTPtr tmp;
                StorageView::replaceWithSubquery(select, query_info.view_query->clone(), tmp, query_info.is_parameterized_view);
            }
        }
    };

    using ExplainAnalyzedSyntaxVisitor = InDepthNodeVisitor<ExplainAnalyzedSyntaxMatcher, true>;

    /// Recursively hide every constant inside a secret argument, preserving the expression structure
    /// (e.g. an `encrypt` key built as `leftPad('...', 16, '*')`). Constants already masked by
    /// `resolveFunction` are left untouched so their mask ids survive; the rest are hidden here for the
    /// dump-only path where the analysis passes did not run (`run_passes = 0`).
    void maskConstantsInSubtree(QueryTreeNodePtr & node)
    {
        if (auto * constant = node->as<ConstantNode>())
        {
            if (!constant->isMasked())
                constant->setMaskId();
            return;
        }
        for (auto & child : node->getChildren())
            if (child)
                maskConstantsInSubtree(child);
    }

    class SecretArgumentsDumpVisitor : public InDepthQueryTreeVisitor<SecretArgumentsDumpVisitor>
    {
        friend class InDepthQueryTreeVisitor;
        static bool needChildVisit(VisitQueryTreeNodeType &, VisitQueryTreeNodeType &)
        {
            /// A secret-bearing function can hide under any carrier (a `UNION`, a scalar subquery, an
            /// expression list), so descend everywhere; `visitImpl` selects the ones to mask.
            return true;
        }

        void visitImpl(VisitQueryTreeNodeType & query_tree_node)
        {
            if (auto * table_function_node = query_tree_node->as<TableFunctionNode>())
            {
                auto secret_arguments = TableFunctionSecretArgumentsFinderTreeNode(*table_function_node).getResult();
                if (!secret_arguments.hasSecrets())
                    return;

                /// A table-function secret value that is not a constant (an identifier or a constant
                /// expression, e.g. a computed url) is hidden whole: the whole argument is the
                /// credential carrier, and a tree dump cannot represent partial masking. Fail closed.
                forEachSecretArgumentNode(
                    table_function_node->getArguments().getNodes(),
                    secret_arguments,
                    [](size_t, QueryTreeNodePtr & node)
                    {
                        if (auto * constant = node->as<ConstantNode>())
                            constant->setMaskId();
                        else
                            node = std::make_shared<ConstantNode>(Field("[HIDDEN]"));
                    });
            }
            else if (auto * function_node = query_tree_node->as<FunctionNode>())
            {
                auto secret_arguments = FunctionSecretArgumentsFinderTreeNode(*function_node).getResult();
                if (!secret_arguments.hasSecrets())
                    return;

                /// An ordinary secret function (`encrypt`/`decrypt`/`HMAC`, ...) is not masked by
                /// `resolveFunction` when the dump runs with the analysis passes disabled. Its secret
                /// is carried in constants (a literal key or one built by an expression), so hide every
                /// constant inside the secret argument, keeping the structure visible.
                forEachSecretArgumentNode(
                    function_node->getArguments().getNodes(),
                    secret_arguments,
                    [](size_t, QueryTreeNodePtr & node) { maskConstantsInSubtree(node); });
            }
        }
    };

}

BlockIO InterpreterExplainQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


Block InterpreterExplainQuery::getSampleBlock(const ASTExplainQuery::ExplainKind kind)
{
    if (kind == ASTExplainQuery::ExplainKind::QueryEstimates)
    {
        auto cols = NamesAndTypes{
            {"database", std::make_shared<DataTypeString>()},
            {"table", std::make_shared<DataTypeString>()},
            {"parts", std::make_shared<DataTypeUInt64>()},
            {"rows", std::make_shared<DataTypeUInt64>()},
            {"marks", std::make_shared<DataTypeUInt64>()},
        };
        return Block({
            {cols[0].type->createColumn(), cols[0].type, cols[0].name},
            {cols[1].type->createColumn(), cols[1].type, cols[1].name},
            {cols[2].type->createColumn(), cols[2].type, cols[2].name},
            {cols[3].type->createColumn(), cols[3].type, cols[3].name},
            {cols[4].type->createColumn(), cols[4].type, cols[4].name},
        });
    }

    Block res;
    ColumnWithTypeAndName col;
    col.name = "explain";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    res.insert(col);
    return res;
}

/// Split str by line feed and write as separate row to ColumnString.
static void fillColumn(IColumn & column, const std::string & str)
{
    size_t start = 0;
    size_t end = 0;
    size_t size = str.size();

    while (end < size)
    {
        if (str[end] == '\n')
        {
            column.insertData(str.data() + start, end - start);
            start = end + 1;
        }

        ++end;
    }

    if (start < end)
        column.insertData(str.data() + start, end - start);
}

namespace
{

/// Settings. Different for each explain type.

struct QueryASTSettings
{
    bool graph = false;
    bool optimize = false;

    constexpr static char name[] = "AST";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
        {"graph", graph},
        {"optimize", optimize}
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings;
};

struct QueryTreeSettings
{
    bool run_passes = true;
    bool dump_tree = true;
    bool dump_passes = false;
    bool dump_ast = false;
    Int64 passes = -1;

    /// Only for EXPLAIN SYNTAX
    bool ast_one_line = false;

    constexpr static char name[] = "QUERY TREE";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
        {"run_passes", run_passes},
        {"dump_tree", dump_tree},
        {"dump_passes", dump_passes},
        {"dump_ast", dump_ast}
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings =
    {
        {"passes", passes}
    };
};

struct QueryPlanSettings
{
    ExplainPlanOptions query_plan_options;

    /// Apply query plan optimizations.
    bool optimize = true;
    bool keep_logical_steps = false;
    bool json = false;

    constexpr static char name[] = "PLAN";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
            {"header", query_plan_options.header},
            {"description", query_plan_options.description},
            {"actions", query_plan_options.actions},
            {"indexes", query_plan_options.indexes},
            {"indices", query_plan_options.indexes},
            {"projections", query_plan_options.projections},
            {"optimize", optimize},
            {"json", json},
            {"sorting", query_plan_options.sorting},
            {"distributed", query_plan_options.distributed},
            {"keep_logical_steps", keep_logical_steps},
            {"input_headers", query_plan_options.input_headers},
            {"column_structure", query_plan_options.column_structure},
            {"compact", query_plan_options.compact},
            {"pretty", query_plan_options.pretty},
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings;
};

struct QueryPipelineSettings
{
    QueryPlan::ExplainPipelineOptions query_pipeline_options;
    bool graph = false;
    bool compact = true;

    constexpr static char name[] = "PIPELINE";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
            {"header", query_pipeline_options.header},
            {"graph", graph},
            {"compact", compact},
            {"distributed", query_pipeline_options.distributed},
            {"compact_repeated_processor_chains", query_pipeline_options.compact_repeated_processor_chains},
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings;
};

struct QueryAnalyzeSettings
{
    ExplainPlanOptions query_plan_options
    {.actions = true,
    .indexes = true,
    .compact = true,
    .pretty = true};

    constexpr static char name[] = "ANALYZE";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
        {"actions", query_plan_options.actions},
        {"indexes", query_plan_options.indexes},
        {"compact", query_plan_options.compact},
        {"pretty", query_plan_options.pretty},
        {"header", query_plan_options.header},
        {"description", query_plan_options.description},
        {"projections", query_plan_options.projections},
        {"sorting", query_plan_options.sorting},
        {"input_headers", query_plan_options.input_headers},
        {"column_structure", query_plan_options.column_structure},
        {"processors", query_plan_options.processors_profile},
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings;
};

template <typename Settings>
struct ExplainSettings : public Settings
{
    using Settings::boolean_settings;
    using Settings::integer_settings;

    bool has(const std::string & name_) const
    {
        return hasBooleanSetting(name_) || hasIntegerSetting(name_);
    }

    bool hasBooleanSetting(const std::string & name_) const
    {
        return boolean_settings.count(name_) > 0;
    }

    bool hasIntegerSetting(const std::string & name_) const
    {
        return integer_settings.count(name_) > 0;
    }

    void setBooleanSetting(const std::string & name_, bool value)
    {
        auto it = boolean_settings.find(name_);
        if (it == boolean_settings.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown setting for ExplainSettings: {}", name_);

        it->second.get() = value;
    }

    void setIntegerSetting(const std::string & name_, Int64 value)
    {
        auto it = integer_settings.find(name_);
        if (it == integer_settings.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown setting for ExplainSettings: {}", name_);

        it->second.get() = value;
    }

    std::string getSettingsList() const
    {
        std::string res;
        for (const auto & setting : boolean_settings)
        {
            if (!res.empty())
                res += ", ";

            res += setting.first;
        }
        for (const auto & setting : integer_settings)
        {
            if (!res.empty())
                res += ", ";

            res += setting.first;
        }

        return res;
    }
};

struct QuerySyntaxSettings
{
    bool oneline = false;
    bool run_query_tree_passes = false;
    Int64 query_tree_passes = -1;

    constexpr static char name[] = "SYNTAX";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
        {"oneline", oneline},
        {"run_query_tree_passes", run_query_tree_passes}
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings =
    {
        {"query_tree_passes", query_tree_passes}
    };
};

template <typename Settings>
ExplainSettings<Settings> checkAndGetSettings(const ASTPtr & ast_settings, bool set_default_pretty_explain_settings = true)
{
    ExplainSettings<Settings> settings;

    /// These lines are needed to impose the default settings for EXPLAIN PLAN
    /// We set them here instead of QueryPlanSettings, because internally
    /// we sometimes use EXPLAIN PLAN output for logging
    if constexpr (std::is_same_v<Settings, QueryPlanSettings> || std::is_same_v<Settings, QueryAnalyzeSettings>)
    {
        if (set_default_pretty_explain_settings)
        {
            settings.query_plan_options.actions = true;
            settings.query_plan_options.compact = true;
            settings.query_plan_options.pretty  = true;
        }
    }

    if (!ast_settings)
        return settings;

    const auto & set_query = ast_settings->as<ASTSetQuery &>();

    for (const auto & change : set_query.changes)
    {
        if (!settings.has(change.name))
            throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown setting \"{}\" for EXPLAIN {} query. "
                            "Supported settings: {}", change.name, Settings::name, settings.getSettingsList());

        if (change.value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Invalid type {} for setting \"{}\" only integer settings are supported",
                change.value.getTypeName(), change.name);

        if (settings.hasBooleanSetting(change.name))
        {
            auto value = change.value.safeGet<UInt64>();
            if (value > 1)
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Invalid value {} for setting \"{}\". "
                                "Expected boolean type", value, change.name);

            settings.setBooleanSetting(change.name, value);
        }
        else
        {
            auto value = change.value.safeGet<UInt64>();
            settings.setIntegerSetting(change.name, value);
        }
    }

    return settings;
}

/// Forward declaration: `checkAccessRightsForQueryTree` recurses into non-inlined regular views through
/// `resolveThenCheckAccessRights`, which in turn calls `checkAccessRightsForQueryTree` on the view body.
bool resolveThenCheckAccessRights(QueryTreeNodePtr query_tree, QueryTreePassManager & pass_manager, const ContextPtr & query_context);

/// `EXPLAIN QUERY TREE` (and `EXPLAIN SYNTAX` in the analyzer) resolve the query and dump table
/// metadata such as column names and types. Unlike `EXPLAIN PLAN`, they do not build a query plan,
/// so the access checks the planner performs in `prepareBuildQueryPlanForTableExpression` are skipped.
/// Without an explicit check here the statement leaks metadata of tables the user is not allowed to
/// read (https://github.com/ClickHouse/ClickHouse/issues/78938). Reproduce the planner's SELECT access
/// check for every table referenced anywhere in the query tree.
///
/// Returns whether the check ran in full. The per-table `SELECT` checks always run (denials throw), but
/// the recursive base-table pass for a non-inlined view (`checkViewBaseTableAccess` below) skips itself
/// when the view's inner query cannot be resolved by the analyzer - e.g. a view created under
/// `enable_analyzer = 0` whose body is legacy-explainable but analyzer-unresolvable. `false` means such
/// a skip happened: the caller is about to dump a resolved tree whose base-table access was never
/// verified, and must fall back to a non-resolved dump - the same fail-close the legacy formatter
/// implements by keeping such a view unexpanded (`checkNonParameterizedViewBaseTableAccess` above).
bool checkAccessRightsForQueryTree(QueryTreeNodePtr & query_tree, const ContextPtr & query_context)
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
            auto access = scope_context->getAccess();
            bool has_accessible_column = false;
            for (const auto & column : storage_snapshot->metadata->getColumns())
            {
                if (access->isGranted(AccessType::SELECT, storage_id.database_name, storage_id.table_name, column.name))
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

        /// The check above enforces `SELECT` on this table (or view) object. A view that is not inlined
        /// stays as a single `TableNode`, so its inner query — and the base tables it reads — never appear
        /// in `query_tree`. Real execution still reads them: `StorageView::readImpl` builds the view's inner
        /// query under the view's own context and checks the base-table privileges there.
        /// `checkViewBaseTableAccess` reproduces that inner pass so a user with `SELECT` on the view but not
        /// on its base tables is denied, exactly as a plain `SELECT` through the view is.
        ///
        /// This covers both a regular, non-parameterized view that is not inlined (`analyzer_inline_views = 0`,
        /// the default) and a parameterized view, whose `TableFunctionNode` storage already holds the
        /// parameter-substituted inner query and the view's SQL security, so exactly the same recursive pass
        /// applies. Without it a user with `SELECT` on the parameterized view but not on its base table
        /// could `EXPLAIN QUERY TREE SELECT * FROM pv(...)` (or `EXPLAIN SYNTAX` when expansion is skipped for
        /// `FINAL` / `SAMPLE` / `SQL SECURITY DEFINER` / `NONE`) and read metadata that real execution rejects
        /// in `StorageView::readImpl`. Inlined views already expose their base tables as `TableNode`s handled
        /// by the loop above, so they are not double-checked here.
        ///
        /// `checkViewBaseTableAccess` skips itself (returning `false`) when the view's inner query cannot
        /// be resolved by the analyzer, so no base-table `SELECT` check ever ran for that view. Propagate
        /// that to the caller instead of dropping it: dumping the resolved tree then would hand out
        /// resolved metadata after only the view-object grant, while the legacy formatter fail-closes for
        /// exactly this case by keeping the view unexpanded.
        if (typeid_cast<const StorageView *>(storage.get()))
            view_checks_performed = checkViewBaseTableAccess(storage, storage_snapshot, scope_context, column_names) && view_checks_performed;
    }

    return view_checks_performed;
}

/// Resolve a throwaway `query_tree` (which the caller owns) and run the access check on it. A query
/// that cannot be resolved (an invalid or fuzzed query, or a table function whose arguments the
/// analyzer intentionally does not evaluate for `EXPLAIN SYNTAX`) has no resolved metadata to protect
/// and a real query would fail with the same resolution error before the planner's access check, so
/// the check is skipped rather than turning a formatting request into a resolution error. This also
/// covers a remote table function (e.g. `paimonAzure`, `url`) that throws a non-`DB::Exception` while
/// connecting during resolution: `EXPLAIN QUERY TREE run_passes = 0` dumps the unresolved tree and must
/// not turn into a connection error. An `ACCESS_DENIED` raised during resolution is still propagated.
/// Returns whether the access check was actually performed in full, so a caller about to dump something
/// the check was supposed to protect can tell that it never ran (the query did not resolve) or ran only
/// partially (a non-inlined view's base-table pass skipped itself, see `checkAccessRightsForQueryTree`).
bool resolveThenCheckAccessRights(QueryTreeNodePtr query_tree, QueryTreePassManager & pass_manager, const ContextPtr & query_context)
{
    bool resolved = false;
    try
    {
        pass_manager.runOnlyResolve(query_tree);
        resolved = true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::ACCESS_DENIED)
            throw;
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok to swallow: a non-ClickHouse exception (e.g. a remote table function that fails to
        /// connect while being resolved) is not an access denial. As above there is no resolved
        /// metadata to protect, so the check is skipped rather than turning a formatting request
        /// into an error.
    }

    return resolved && checkAccessRightsForQueryTree(query_tree, query_context);
}

/// Reproduce the planner's per-table `SELECT` access check for the tables referenced by one explained
/// `SELECT`, without producing any dump. Used by `EXPLAIN SYNTAX` / `EXPLAIN AST optimize = 1` when a
/// parameterized view
/// was expanded into its inner subquery: the expanded query no longer contains the view object, so the
/// check must run on the original query, where the parameterized view still resolves to its own
/// `TableNode`. That matches real execution, which turns `pv(...)` into a fake `TableNode` for the view
/// in `QueryAnalyzer::resolveTableFunction` and checks `SELECT` on it in
/// `prepareBuildQueryPlanForTableExpression`. `run_passes` / `passes` mirror the `EXPLAIN SYNTAX`
/// settings so the tree is resolved the same way the dump is.
/// Returns whether the access check was actually performed in full: for a query that cannot be resolved
/// (a "format but do not resolve" shape) `resolveThenCheckAccessRights` deliberately skips the check, and
/// for a query reading from a non-inlined view whose inner query the analyzer cannot resolve the
/// recursive base-table pass skips itself (see `checkAccessRightsForQueryTree`). In both cases the
/// caller must not dump anything the skipped check was supposed to protect.
bool checkAccessForExplainedSelect(const ASTPtr & explained_query, const ContextPtr & query_context, bool run_passes, Int64 passes)
{
    /// `joined_subquery_requires_alias` is a convenience restriction on how a query may be written, not
    /// an access rule, and an unexpanded parameterized view call in a `JOIN` carries no alias of its own.
    /// Resolving the original query for the check must not fail on it: the check would then be skipped
    /// and the caller would fall back to dumping the query unexpanded. The relaxation applies only to
    /// this throwaway tree - the dump itself is still produced under the query's own settings.
    auto check_context = Context::createCopy(query_context);
    check_context->setSetting("joined_subquery_requires_alias", false);

    auto query_tree = buildQueryTree(explained_query, check_context);
    auto query_tree_pass_manager = QueryTreePassManager(check_context);
    addQueryTreePasses(query_tree_pass_manager);
    size_t pass_index = passes < 0 ? query_tree_pass_manager.getPasses().size() : static_cast<size_t>(passes);

    if (run_passes && pass_index >= 1)
    {
        query_tree_pass_manager.run(query_tree, pass_index);
        return checkAccessRightsForQueryTree(query_tree, check_context);
    }

    return resolveThenCheckAccessRights(std::move(query_tree), query_tree_pass_manager, check_context);
}

/// Collect the outermost `SELECT`s of an explained statement. For a plain `EXPLAIN SYNTAX SELECT ...`
/// that is the statement itself, but `EXPLAIN` also accepts statements that merely wrap a `SELECT`
/// (`INSERT INTO ... SELECT`, `CREATE [MATERIALIZED] VIEW ... AS SELECT`, ...), and
/// `ExpandParameterizedViewsMatcher` rewrites the parameterized view calls inside those nested
/// `SELECT`s too. The access check must therefore descend into the wrapper instead of treating every
/// non-`SELECT` root as "no check possible", otherwise the expanded view body would be dumped without
/// the `SELECT` grant on the view object ever being enforced.
void collectExplainedSelects(const ASTPtr & ast, ASTs & selects)
{
    if (!ast)
        return;

    if (ast->as<ASTSelectWithUnionQuery>())
    {
        selects.push_back(ast);
        return;
    }

    for (const auto & child : ast->children)
        collectExplainedSelects(child, selects);
}

bool checkAccessForExplainedQuery(const ASTPtr & explained_query, const ContextPtr & query_context, bool run_passes, Int64 passes)
{
    ASTs selects;
    collectExplainedSelects(explained_query, selects);

    /// Nothing that can be resolved into a query tree - the caller must treat this as "check skipped".
    if (selects.empty())
        return false;

    bool checked = true;
    for (const auto & select : selects)
        checked = checkAccessForExplainedSelect(select, query_context, run_passes, passes) && checked;
    return checked;
}

/// Whether the explained query references anything `checkAccessRightsForQueryTree` could possibly
/// guard: a plain table or view (only a table identifier resolves into a `TableNode`), or a
/// parameterized view, which is called like a table function but with a name no registered table
/// function has (`QueryAnalyzer::resolveTableFunction` considers a parameterized view only after
/// `TableFunctionFactory::tryGet` fails). A query whose table expressions are all genuine table
/// functions can never be denied by the check - the planner does not `SELECT`-check table functions
/// either (their own access check happens in `ITableFunction::execute`) - so resolving such a query
/// for the check alone is pure overhead with observable side effects: resolving a table function may
/// connect to a remote server (e.g. `mysql(...)` fetches the table structure), turning
/// `EXPLAIN QUERY TREE run_passes = 0` - which deliberately dumps the unresolved tree so that no
/// server is contacted - into connection attempts and error-log noise
/// (04657_mysql_tls_credentials_query_tree). A registered table function is not scanned by name only:
/// its arguments may still hold a subquery reading from a table (`view(SELECT ... FROM t)`), which the
/// recursion below keeps visiting through the generic children walk.
bool referencesCheckableTables(const ASTPtr & node)
{
    if (!node)
        return false;

    if (const auto * table_expression = node->as<ASTTableExpression>())
    {
        if (table_expression->database_and_table_name)
            return true;

        if (table_expression->table_function)
        {
            const auto * function = table_expression->table_function->as<ASTFunction>();
            if (function && !TableFunctionFactory::instance().isTableFunctionName(function->name))
                return true;
        }
    }

    for (const auto & child : node->children)
        if (referencesCheckableTables(child))
            return true;

    return false;
}

bool explainQueryTree(
    ASTPtr explained_query,
    ContextPtr query_context,
    const QueryTreeSettings & settings,
    WriteBuffer & buf,
    bool format_ast_as_syntax,
    bool check_access = true)
{
    if (explained_query->as<ASTSelectWithUnionQuery>() == nullptr)
        return false;

    auto query_tree = buildQueryTree(explained_query, query_context);
    bool need_newline = false;

    auto query_tree_pass_manager = QueryTreePassManager(query_context);
    addQueryTreePasses(query_tree_pass_manager);

    size_t pass_index = settings.passes < 0 ? query_tree_pass_manager.getPasses().size() : static_cast<size_t>(settings.passes);

    if (settings.run_passes)
    {
        if (settings.dump_passes)
        {
            query_tree_pass_manager.dump(buf, pass_index);
            need_newline = true;
        }

        query_tree_pass_manager.run(query_tree, pass_index);
    }

    /// Check SELECT access on the referenced tables before dumping any resolved metadata about them.
    /// `buildQueryTree` only builds the tree; table identifiers are bound to storages and columns/types
    /// are resolved only by the query analysis pass. When the passes above already resolved `query_tree`
    /// we check it directly; otherwise (e.g. `run_passes = 0`, which intentionally dumps the unresolved
    /// tree) we resolve a throwaway copy just for the access check. `check_access` is false only when the
    /// caller (`EXPLAIN SYNTAX` after expanding a parameterized view) already ran the check on the
    /// original query, where the view object is still present.
    if (check_access)
    {
        if (settings.run_passes && pass_index >= 1)
        {
            if (!checkAccessRightsForQueryTree(query_tree, query_context))
            {
                /// The query reads from a non-inlined view whose inner query the analyzer cannot resolve
                /// (a view created under `enable_analyzer = 0` with a legacy-only shape, a remote table
                /// function that fails while connecting, ...), so the recursive base-table `SELECT` check
                /// skipped itself after only the view-object grant was verified. Dumping the resolved tree
                /// then would hand out resolved metadata the skipped check was supposed to protect, so
                /// fall back to a freshly built, unresolved tree - it carries no resolved column names or
                /// types, only the user's own query text, matching the fail-close the legacy formatter
                /// implements by keeping such a view unexpanded. A real `SELECT` through such a view fails
                /// while resolving the inner query in `StorageView::readImpl`, so no successfully running
                /// query loses its resolved `EXPLAIN` here.
                query_tree = buildQueryTree(explained_query, query_context);
            }
        }
        else
        {
            /// The dumped tree here is the unresolved `query_tree`, which never carries resolved column
            /// names or types, so the dump itself cannot leak table metadata. We still resolve a throwaway
            /// copy to reproduce the planner's `SELECT` access check on the referenced tables - but only
            /// when the query references something the check could guard (see `referencesCheckableTables`),
            /// so that a query reading only from genuine table functions is dumped without resolving them.
            if (referencesCheckableTables(explained_query))
                resolveThenCheckAccessRights(query_tree->clone(), query_tree_pass_manager, query_context);
        }
    }

    /// Mask secrets only after the passes: the masked tree is used solely for the dump below, so
    /// redaction (which may replace a non-constant secret value with a hidden constant) can never
    /// change how the query is analyzed. With run_passes = 0 the tree is dumped without analysis.
    if (!query_context->getSettingsRef()[Setting::format_display_secrets_in_show_and_select])
    {
        SecretArgumentsDumpVisitor visitor;
        visitor.visit(query_tree);
    }

    if (settings.dump_tree)
    {
        if (need_newline)
            buf << "\n\n";

        query_tree->dumpTree(buf);
        need_newline = true;
    }

    if (settings.dump_ast)
    {
        if (need_newline)
            buf << "\n\n";

        IAST::FormatSettings format_settings(settings.ast_one_line);
        format_settings.show_secrets = query_context->getSettingsRef()[Setting::format_display_secrets_in_show_and_select];

        ConvertToASTOptions ast_options;
        /// `EXPLAIN SYNTAX` shows the query in a canonical, close-to-syntax form, so constants are
        /// rendered as their source expressions and function calls are preferred over operator syntax.
        /// `EXPLAIN QUERY TREE` (dump_ast) must show the query as it actually is after the query tree passes,
        /// so neither source-expression rendering nor operator-to-function conversion is applied there.
        ast_options.use_source_expression_for_constants = format_ast_as_syntax;

        IAST::FormatState format_state;
        IAST::FormatStateStacked format_frame;
        format_frame.allow_operators = !format_ast_as_syntax;
        query_tree->toAST(ast_options)->format(buf, format_settings, format_state, format_frame);
    }

    return true;
}

}

/// See the forward declaration above `ExpandParameterizedViewsMatcher` for why this is `static` at file
/// scope rather than inside one of this file's unnamed namespaces.
static bool checkViewBaseTableAccess(
    const StoragePtr & view_storage, const StorageSnapshotPtr & view_snapshot, const ContextPtr & scope_context, const Names & column_names)
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
    return resolveThenCheckAccessRights(std::move(view_query_tree), view_pass_manager, view_context);
}

static void formatHeaderExplainAnalyze(
        UInt64 total_time_ns,
        UInt64 planning_ns,
        UInt64 execute_ns,
        UInt64 read_rows,
        UInt64 read_bytes,
        Int64 peak_memory,
        WriteBuffer & out)
{
    out << "Query summary:\n";

    /// Total time, split into the planning (logical plan, optimization, physical pipeline) and execution phases.
    out << "  Time:        " << formatReadableTime(static_cast<double>(total_time_ns))
        << " (planning " << formatReadableTime(static_cast<double>(planning_ns))
        << " · execution " << formatReadableTime(static_cast<double>(execute_ns)) << ")\n";

    /// Rows/bytes read from tables, with throughput relative to the execution time.
    out << "  Read:        " << formatReadableQuantity(static_cast<double>(read_rows)) << " rows, "
        << formatReadableSizeWithDecimalSuffix(static_cast<double>(read_bytes));
    if (execute_ns)
    {
        const double rows_per_sec = static_cast<double>(read_rows) * 1e9 / static_cast<double>(execute_ns);
        const double bytes_per_sec = static_cast<double>(read_bytes) * 1e9 / static_cast<double>(execute_ns);
        out << " (" << formatReadableQuantity(rows_per_sec) << " rows/s., "
            << formatReadableSizeWithDecimalSuffix(bytes_per_sec) << "/s.)";
    }
    out << "\n";

    out << "  Peak memory: " << formatReadableSizeWithBinarySuffix(static_cast<double>(peak_memory)) << "\n";

    out << "\n";
}

class RejectStreamingVisitor : public ConstInDepthQueryTreeVisitor<RejectStreamingVisitor>
{
public:
    void visitImpl(const QueryTreeNodePtr & node)
    {
        std::optional<TableExpressionModifiers> modifiers;
        if (const auto * table_node = node->as<TableNode>())
            modifiers = table_node->getTableExpressionModifiers();
        else if (const auto * table_function_node = node->as<TableFunctionNode>())
            modifiers = table_function_node->getTableExpressionModifiers();

        if (modifiers && modifiers->hasStream())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "EXPLAIN ANALYZE is not supported for streaming (FROM ... STREAM) queries");
    }
};

static void rejectStreamingForExplainAnalyze(const QueryTreeNodePtr & query_tree)
{
    /// Walk the whole query tree, not just join-tree table expressions: a streaming read can be nested in a
    /// WHERE/PREWHERE subquery or a CTE, which extractTableExpressions does not descend into.
    RejectStreamingVisitor visitor;
    visitor.visit(query_tree);
}

/// A streaming read behind a view has no table expression modifier in the outer query tree, so only the plan
/// exposes it. Walk it exactly like StepWallClockRegistry::populateFromPlan: a read absent from the plan cannot
/// be timed, so it must not be rejected.
static void rejectStreamingForExplainAnalyze(const QueryPlan & plan)
{
    if (!plan.isInitialized())
        return;

    std::vector<const QueryPlan::Node *> stack;
    stack.push_back(plan.getRootNode());

    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        if (!node || !node->step)
            continue;

        const auto * source_step = dynamic_cast<const SourceStepWithFilter *>(node->step.get());
        if (source_step && source_step->getQueryInfo().isStream())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "EXPLAIN ANALYZE is not supported for streaming (FROM ... STREAM) queries");

        for (const auto * child : node->children)
            stack.push_back(child);
        for (const auto * child_plan : node->step->getChildPlans())
            stack.push_back(child_plan->getRootNode());
    }
}

struct InterpreterExplainQuery::AnalyzedInnerQuery
{
    QueryPlan plan;
    ContextPtr context;
    std::function<std::unique_ptr<QueryPlan>()> parallel_replicas_builder;
    bool ignore_quota = false;
    bool ignore_limits = false;
    UInt64 planning_ns = 0;
    ExplainPlanOptions query_plan_options;
};

InterpreterExplainQuery::InterpreterExplainQuery(const ASTPtr & query_, ContextPtr context_, const SelectQueryOptions & options_)
    : WithContext(context_)
    , query(query_)
    , options(options_)
{
}

InterpreterExplainQuery::~InterpreterExplainQuery() = default;

bool InterpreterExplainQuery::isExecutableAnalyze() const
{
    const auto & ast = query->as<const ASTExplainQuery &>();
    if (ast.getKind() != ASTExplainQuery::Analyze)
        return false;

    /// Only an inner SELECT is executed by EXPLAIN ANALYZE; other inner queries are rejected in executeImpl.
    if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
        return false;

    /// Distributed EXPLAIN ANALYZE is rejected before execution, so do not plan it here (e.g. while
    /// charging quota in executeQuery). The quota is charged as for a generic query and the error follows.
    if (getContext()->getSettingsRef()[Setting::make_distributed_plan])
        return false;

    return true;
}

InterpreterExplainQuery::AnalyzedInnerQuery & InterpreterExplainQuery::getAnalyzedInnerQuery() const
{
    if (analyzed_inner_query)
        return *analyzed_inner_query;

    const auto & ast = query->as<const ASTExplainQuery &>();

    /// Mirror the context and option setup that executeImpl applies before planning the inner SELECT,
    /// so the effective ignore_quota / ignore_limits we expose match what actual execution would use.
    auto inner_options = options;
    inner_options.setExplain();

    auto planning_context = Context::createCopy(getContext());
    inner_options.max_step_description_length = planning_context->getSettingsRef()[Setting::query_plan_max_step_description_length];
    InterpreterSetQuery::applySettingsFromQuery(query, planning_context);

    auto result = std::make_unique<AnalyzedInnerQuery>();

    result->query_plan_options = checkAndGetSettings<QueryAnalyzeSettings>(ast.getSettings()).query_plan_options;

    Stopwatch watch;
    QueryTreeNodePtr query_tree;
    if (planning_context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter(ast.getExplainedQuery(), planning_context, inner_options);
        query_tree = interpreter.getQueryTree();
        result->context = interpreter.getContext();
        result->parallel_replicas_builder = interpreter.getQueryPlanWithParallelReplicasBuilder();
        /// Force planning so the effective ignore flags settle before we read them.
        interpreter.getQueryPlan();
        result->ignore_quota = interpreter.ignoreQuota();
        result->ignore_limits = interpreter.ignoreLimits();
        result->plan = std::move(interpreter).extractQueryPlan();
    }
    else
    {
        InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), planning_context, inner_options);
        interpreter.buildQueryPlan(result->plan);
        result->context = interpreter.getContext();
        result->ignore_quota = interpreter.ignoreQuota();
        result->ignore_limits = interpreter.ignoreLimits();
    }

    if (query_tree)
        rejectStreamingForExplainAnalyze(query_tree);

    rejectStreamingForExplainAnalyze(result->plan);

    result->planning_ns = watch.elapsed();

    analyzed_inner_query = std::move(result);
    return *analyzed_inner_query;
}

bool InterpreterExplainQuery::ignoreQuota() const
{
    if (!isExecutableAnalyze())
        return IInterpreter::ignoreQuota();
    return getAnalyzedInnerQuery().ignore_quota;
}

bool InterpreterExplainQuery::ignoreLimits() const
{
    if (!isExecutableAnalyze())
        return IInterpreter::ignoreLimits();
    return getAnalyzedInnerQuery().ignore_limits;
}

QueryPipeline InterpreterExplainQuery::executeImpl()
{
    const auto & ast = query->as<const ASTExplainQuery &>();

    Block sample_block = getSampleBlock(ast.getKind());
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    WriteBufferFromOwnString buf;
    bool single_line = false;
    bool insert_buf = true;

    ContextPtr query_context = getContext();

    options.setExplain();
    options.max_step_description_length = query_context->getSettingsRef()[Setting::query_plan_max_step_description_length];

    /// https://github.com/ClickHouse/ClickHouse/issues/88467
    /// EXPLAIN is to get a good picture of how the query will execute after *static* planning.
    /// Hence disable any optimizations that stagger the planning or introduce variablility due to caches.
    auto explain_query_context = Context::createCopy(query_context);

    if (ast.getKind() != ASTExplainQuery::Analyze)
    {
        explain_query_context->setSetting("use_skip_indexes_on_data_read", false);
        explain_query_context->setSetting("use_query_condition_cache", false);
    }

    /// `EXPLAIN AST optimize = 1` rewrites the query with the old interpreter's visitor, which the
    /// analyzer does not support, so the explained query is always processed in legacy mode here.
    if (ast.getKind() == ASTExplainQuery::ParsedAST)
        explain_query_context->setSetting("allow_experimental_analyzer", false);

    InterpreterSetQuery::applySettingsFromQuery(query, explain_query_context);

    /// Whether the explained query, run for real, would use the analyzer. For `EXPLAIN AST` that is
    /// not the mode this interpreter works in (see above), but forcing the legacy interpreter is an
    /// implementation detail of the rewriting visitor: which privileges a query requires must follow
    /// what really running it would require, not how `EXPLAIN` happens to process it. The forced
    /// override cannot simply be ignored either, because the explained statement may set the setting
    /// itself (`... SETTINGS allow_experimental_analyzer = 0` under an analyzer-enabled session runs
    /// for real in legacy mode), so the effective value is taken from a copy of the session context
    /// that the override never touched, with the statement's own settings applied to it.
    bool analyzer_enabled_for_explained_query
        = explain_query_context->getSettingsRef()[Setting::allow_experimental_analyzer];
    if (ast.getKind() == ASTExplainQuery::ParsedAST && !analyzer_enabled_for_explained_query)
    {
        auto real_execution_context = Context::createCopy(query_context);
        InterpreterSetQuery::applySettingsFromQuery(query, real_execution_context);
        analyzer_enabled_for_explained_query
            = real_execution_context->getSettingsRef()[Setting::allow_experimental_analyzer];
    }

    query_context = std::move(explain_query_context);

    switch (ast.getKind())
    {
        case ASTExplainQuery::ParsedAST:
        {
            auto settings = checkAndGetSettings<QueryASTSettings>(ast.getSettings());

            ASTPtr query_to_dump = ast.getExplainedQuery();
            if (settings.optimize)
            {
                /// `EXPLAIN AST optimize = 1` runs the same visitor as `EXPLAIN SYNTAX` and therefore dumps
                /// the same rewritten query, so it needs the same parameterized-view handling. Expand the
                /// view calls here too: the visitor's own inlining (`StorageView::replaceWithSubquery`) runs
                /// no base-table access check at all, while resolving the expanded subquery below does check
                /// them, exactly as a real `SELECT ... FROM pv(...)` does in `StorageView::readImpl`.
                ASTPtr explained_query_before_expansion = ast.getExplainedQuery()->clone();

                ExpandParameterizedViewsMatcher::Data expand_views_data(query_context);
                ExpandParameterizedViewsVisitor(expand_views_data).visit(query);
                const bool expanded_parameterized_view = expand_views_data.expanded_parameterized_view;
                const bool referenced_parameterized_view = expand_views_data.referenced_parameterized_view;

                /// The visitor below only enforces the base-table grants (through the recursive analysis of
                /// the expanded subquery). With the analyzer, a real `SELECT ... FROM pv(...)` additionally
                /// requires `SELECT` on the parameterized view object itself, because
                /// `QueryAnalyzer::resolveTableFunction` turns the call into a view `TableNode` that
                /// `prepareBuildQueryPlanForTableExpression` checks. Run that check on the original,
                /// unexpanded query - exactly as the `EXPLAIN SYNTAX` path does - so `EXPLAIN AST` cannot
                /// dump the inlined body for a user who may read the base tables but not the view.
                ///
                /// As there, the check is analyzer-only: the old interpreter resolves `pv(...)` without
                /// requiring any grant on the view object, so running it in the legacy path would deny a
                /// query whose real execution succeeds.
                ///
                /// The check must not be tied to the expansion above having happened: expansion is skipped
                /// for `FINAL` / `SAMPLE` and for `SQL SECURITY DEFINER` / `NONE` parameterized views, and
                /// for those the visitor below still inlines the view body itself
                /// (`StorageView::replaceWithSubquery` on `query_info.view_query`). Every referenced
                /// parameterized view therefore needs the view-object check.
                bool access_check_performed = true;
                if (referenced_parameterized_view && analyzer_enabled_for_explained_query)
                    access_check_performed = checkAccessForExplainedQuery(
                        explained_query_before_expansion, query_context, /*run_passes=*/ false, /*passes=*/ -1);

                if (!access_check_performed)
                {
                    /// The pre-check skips itself for a query the analyzer cannot resolve. Dumping the
                    /// expanded query then would reveal the parameter-substituted view body with no check
                    /// having run at all, so dump the original, unexpanded query - the user's own text.
                    query_to_dump = explained_query_before_expansion;
                }
                else
                {
                    ExplainAnalyzedSyntaxVisitor::Data data(query_context);

                    /// As on the `EXPLAIN SYNTAX` path: the expanded query may be unresolvable, and dumping it
                    /// then would reveal the parameter-substituted view body without any check having run. Fall
                    /// back to the original, unexpanded query - the user's own text - in that case. An
                    /// `ACCESS_DENIED` is still propagated.
                    try
                    {
                        ExplainAnalyzedSyntaxVisitor(data).visit(query);
                    }
                    catch (const Exception & e)
                    {
                        if (e.code() == ErrorCodes::ACCESS_DENIED || !expanded_parameterized_view)
                            throw;
                        query_to_dump = explained_query_before_expansion;
                    }
                }
            }

            if (settings.graph)
                dumpASTInDotFormat(*query_to_dump, buf);
            else
                dumpAST(*query_to_dump, buf);
            break;
        }
        case ASTExplainQuery::AnalyzedSyntax:
        {
            auto settings = checkAndGetSettings<QuerySyntaxSettings>(ast.getSettings());

            /// The expansion below rewrites parameterized view calls in place, dropping the view object
            /// from the query. Keep the original query so the access check can still see and enforce the
            /// `SELECT` grant on the view object, matching real execution.
            ASTPtr explained_query_before_expansion = ast.getExplainedQuery()->clone();

            /// Inline any parameterized view calls with their parameter-substituted inner queries,
            /// so EXPLAIN SYNTAX shows what the view actually expands to.
            ExpandParameterizedViewsMatcher::Data expand_views_data(query_context);
            ExpandParameterizedViewsVisitor(expand_views_data).visit(query);
            const bool expanded_parameterized_view = expand_views_data.expanded_parameterized_view;
            const bool referenced_parameterized_view = expand_views_data.referenced_parameterized_view;

            /// Set when the analyzer pre-check below was supposed to run but skipped itself. The legacy
            /// fallback must then not dump the expanded query either.
            bool analyzer_pre_check_skipped = false;

            if (query_context->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                /// Expanding a parameterized view into its inner subquery removes the view object from
                /// the dumped tree, so `explainQueryTree` would only check the base tables. Run the
                /// access check on the original query instead, where the view still resolves to its own
                /// `TableNode`, and skip the (now incomplete) check on the expanded tree. This enforces
                /// the view's `SELECT` grant exactly as a real `SELECT ... FROM pv(...)` does.
                ///
                /// This pre-check is deliberately analyzer-only. The old interpreter never requires a
                /// `SELECT` grant on the parameterized view object: `Context::executeTableFunction`
                /// resolves `pv(...)` without any access check and `InterpreterSelectQuery` skips
                /// `checkAccessRightsForSelect` for table functions, so a real
                /// `SELECT ... FROM pv(...)` with `allow_experimental_analyzer = 0` succeeds with only
                /// the base-table grants (which `ExplainAnalyzedSyntaxVisitor` below does enforce, via
                /// `InterpreterSelectQuery` analysis of the expanded subquery). Running the view-object
                /// check in the legacy fallback would make `EXPLAIN SYNTAX` deny a query whose real
                /// execution succeeds.
                ///
                /// As on the `EXPLAIN AST optimize = 1` path, the check must not be tied to the expansion
                /// above having happened. Expansion is deliberately skipped for `FINAL` / `SAMPLE` and for
                /// `SQL SECURITY DEFINER` / `NONE` parameterized views, and `explainQueryTree` below only
                /// enforces the view-object grant itself when it accepts the explained statement - it
                /// declines any root that merely wraps a `SELECT` (`INSERT INTO dst SELECT ... FROM
                /// pv(...)`). The legacy visitor reached afterwards then inlines the view body through
                /// `StorageView::replaceWithSubquery` with no check on the view object at all. Every
                /// referenced parameterized view therefore needs the check here.
                bool access_check_performed = true;
                if (referenced_parameterized_view)
                    access_check_performed = checkAccessForExplainedQuery(
                        explained_query_before_expansion, query_context, settings.run_query_tree_passes, settings.query_tree_passes);

                /// The pre-check deliberately skips itself when the original query cannot be resolved (a
                /// "format but do not resolve" shape, see `resolveThenCheckAccessRights`). Dumping the
                /// expanded query then would reveal the parameter-substituted view body without any
                /// `SELECT` check ever having run on the view or its base tables, so fall back to
                /// formatting the original, unexpanded query instead. That dump carries no view metadata:
                /// it is the user's own query text, unresolved (the skip can only happen when the query
                /// tree passes do not run, so `explainQueryTree` dumps the unresolved tree).
                analyzer_pre_check_skipped = referenced_parameterized_view && !access_check_performed;

                const ASTPtr & query_to_explain
                    = analyzer_pre_check_skipped ? explained_query_before_expansion : ast.getExplainedQuery();

                bool explain_ok = explainQueryTree(query_to_explain, query_context, QueryTreeSettings{
                    .run_passes = settings.run_query_tree_passes,
                    .dump_tree = false,
                    .dump_passes = false,
                    .dump_ast = true,
                    .passes = settings.query_tree_passes,
                    .ast_one_line = settings.oneline,
                }, buf, /*format_ast_as_syntax=*/ true, /*check_access=*/ !expanded_parameterized_view);

                if (explain_ok)
                    break;
                auto query_context_mutable = Context::createCopy(query_context);
                query_context_mutable->setSetting("allow_experimental_analyzer", false);
                query_context = std::move(query_context_mutable);
            }

            ExplainAnalyzedSyntaxVisitor::Data data(query_context);

            /// If a parameterized view was expanded above, the resulting query may be unresolvable (e.g. an
            /// unknown identifier in the outer projection over the expanded body). The `analyze()`-mode
            /// `InterpreterSelectQuery` this visitor runs then throws while resolving the expanded query. As on
            /// the analyzer path, fall back to formatting the original, unexpanded query in that case: it is the
            /// user's own text and carries no parameter-substituted view body, so nothing the (now skipped)
            /// access check was supposed to protect is revealed. An `ACCESS_DENIED` is still propagated, so a
            /// user without access to the view's base tables is denied instead of getting the fallback dump.
            ///
            /// The same fallback applies when the analyzer pre-check above skipped itself and
            /// `explainQueryTree` then declined to dump the query - as it does for a statement that only
            /// wraps a `SELECT` (`EXPLAIN SYNTAX INSERT INTO dst SELECT ... FROM pv(...)`). Reaching the
            /// legacy visitor with the analyzer enabled must not turn into a dump of the expanded body
            /// that the (skipped) view-object check was supposed to protect.
            ASTPtr query_to_format;
            if (analyzer_pre_check_skipped)
            {
                query_to_format = explained_query_before_expansion;
            }
            else
            {
                try
                {
                    ExplainAnalyzedSyntaxVisitor(data).visit(query);
                    query_to_format = ast.getExplainedQuery();
                }
                catch (const Exception & e)
                {
                    if (e.code() == ErrorCodes::ACCESS_DENIED || !expanded_parameterized_view)
                        throw;
                    query_to_format = explained_query_before_expansion;
                }
            }

            IAST::FormatSettings format_settings(settings.oneline);
            IAST::FormatState format_state;
            IAST::FormatStateStacked format_frame;
            format_frame.allow_operators = false;
            query_to_format->format(buf, format_settings, format_state, format_frame);
            break;
        }
        case ASTExplainQuery::QueryTree:
        {
            if (!query_context->getSettingsRef()[Setting::allow_experimental_analyzer])
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "EXPLAIN QUERY TREE is only supported with the analyzer. SET enable_analyzer = 1.");

            auto settings = checkAndGetSettings<QueryTreeSettings>(ast.getSettings());
            if (!settings.dump_tree && !settings.dump_ast)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Either 'dump_tree' or 'dump_ast' must be set for EXPLAIN QUERY TREE query");

            if (!explainQueryTree(ast.getExplainedQuery(), query_context, settings, buf, /*format_ast_as_syntax=*/ false))
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT is supported for EXPLAIN QUERY TREE query");

            break;
        }
        case ASTExplainQuery::QueryPlan:
        {
            if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT is supported for EXPLAIN query");

            bool pretty_version = query_context->getSettingsRef()[Setting::explain_query_plan_default] == ExplainQueryPlanDefault::PRETTY;

            auto ast_settings = ast.getSettings();

            if (ast_settings)
                for (const auto & change : ast_settings->as<ASTSetQuery &>().changes)
                {
                    if (change.name != "json" && change.name != "distributed")
                        continue;
                    if (change.value.getType() == Field::Types::UInt64 && change.value.safeGet<UInt64>() != 0)
                        pretty_version = false;
                }

            auto settings = checkAndGetSettings<QueryPlanSettings>(ast_settings, pretty_version);

            QueryPlan plan;

            ContextPtr context;

            if (query_context->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                InterpreterSelectQueryAnalyzer interpreter(ast.getExplainedQuery(), query_context, options);
                context = interpreter.getContext();
                plan = std::move(interpreter).extractQueryPlan();
            }
            else
            {
                InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), query_context, options);
                interpreter.buildQueryPlan(plan);
                context = interpreter.getContext();
            }

            if (settings.optimize)
            {
                auto optimization_settings = QueryPlanOptimizationSettings(context);
                optimization_settings.keep_logical_steps = settings.keep_logical_steps;
                optimization_settings.is_explain = true;
                optimization_settings.max_step_description_length = query_context->getSettingsRef()[Setting::query_plan_max_step_description_length];
                plan.optimize(optimization_settings);
            }

            if (settings.json)
            {
                if (settings.query_plan_options.distributed)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Option 'distributed' is not supported with option 'json'");

                /// Add extra layers to make plan look more like from postgres.
                auto plan_map = std::make_unique<JSONBuilder::JSONMap>();
                plan_map->add("Plan", plan.explainPlan(settings.query_plan_options));
                auto plan_array = std::make_unique<JSONBuilder::JSONArray>();
                plan_array->add(std::move(plan_map));

                auto format_settings = getFormatSettings(query_context);
                format_settings.json.quote_64bit_integers = false;

                JSONBuilder::FormatSettings json_format_settings{.settings = format_settings};
                JSONBuilder::FormatContext format_context{.out = buf};

                plan_array->format(json_format_settings, format_context);

                single_line = true;
            }
            else
                plan.explainPlan(buf, settings.query_plan_options, 0, query_context->getSettingsRef()[Setting::query_plan_max_step_description_length]);
            break;
        }
        case ASTExplainQuery::QueryPipeline:
        {
            if (dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
            {
                auto settings = checkAndGetSettings<QueryPipelineSettings>(ast.getSettings());
                QueryPlan plan;
                ContextPtr context;

                if (query_context->getSettingsRef()[Setting::allow_experimental_analyzer])
                {
                    InterpreterSelectQueryAnalyzer interpreter(ast.getExplainedQuery(), query_context, options);
                    context = interpreter.getContext();
                    plan = std::move(interpreter).extractQueryPlan();
                }
                else
                {
                    InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), query_context, options);
                    interpreter.buildQueryPlan(plan);
                    context = interpreter.getContext();
                }

                auto optimization_settings = QueryPlanOptimizationSettings(context);
                optimization_settings.is_explain = true;
                optimization_settings.max_step_description_length = query_context->getSettingsRef()[Setting::query_plan_max_step_description_length];
                auto pipeline = plan.buildQueryPipeline(optimization_settings, BuildQueryPipelineSettings(context));

                if (settings.graph)
                {
                    if (settings.query_pipeline_options.distributed)
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Option 'distributed' is not supported with option 'graph'");

                    /// Pipe holds QueryPlan, should not go out-of-scope
                    QueryPlanResourceHolder resources;
                    auto pipe = QueryPipelineBuilder::getPipe(std::move(*pipeline), resources);
                    const auto & processors = pipe.getProcessors();

                    if (settings.compact)
                        printPipelineCompact(processors, buf, settings.query_pipeline_options.header);
                    else
                        printPipeline(processors, buf);
                }
                else
                {
                    plan.explainPipeline(buf, settings.query_pipeline_options);
                }
            }
            else if (dynamic_cast<const ASTInsertQuery *>(ast.getExplainedQuery().get()))
            {
                auto insert_context = Context::createCopy(getContext());
                InterpreterInsertQuery insert(
                    ast.getExplainedQuery(),
                    insert_context,
                    /* allow_materialized */ false,
                    /* no_squash */ false,
                    /* no_destination */ false,
                    /* async_insert */ false);
                auto io = insert.execute();
                printPipeline(io.pipeline.getProcessors(), buf);
                // we do not need it anymore, it would not be executed
                io.pipeline.cancel();
            }
            else
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT and INSERT is supported for EXPLAIN PIPELINE query");
            break;
        }
        case ASTExplainQuery::QueryEstimates:
        {
            if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT is supported for EXPLAIN ESTIMATE query");

            auto settings = checkAndGetSettings<QueryPlanSettings>(ast.getSettings());
            QueryPlan plan;
            ContextPtr context = query_context;

            if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                InterpreterSelectQueryAnalyzer interpreter(ast.getExplainedQuery(), query_context, SelectQueryOptions());
                context = interpreter.getContext();
                plan = std::move(interpreter).extractQueryPlan();
            }
            else
            {
                InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), query_context, SelectQueryOptions());
                context = interpreter.getContext();
                interpreter.buildQueryPlan(plan);
            }

            // Collect the selected marks, rows, parts during build query pipeline.
            // Hold on to the returned QueryPipelineBuilderPtr because `plan` may have pointers into
            // it (through QueryPlanResourceHolder).
            auto builder = plan.buildQueryPipeline(QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));

            plan.explainEstimate(res_columns);
            insert_buf = false;
            break;
        }
        case ASTExplainQuery::TableOverride:
        {
            if (auto * table_function = ast.getTableFunction()->as<ASTFunction>(); !table_function || table_function->name != "mysql")
            {
                throw Exception(ErrorCodes::INCORRECT_QUERY, "EXPLAIN TABLE OVERRIDE is not supported for the {}() table function", table_function->name);
            }
            auto storage = query_context->getQueryContext()->executeTableFunction(ast.getTableFunction());
            auto metadata = storage->getInMemoryMetadataPtr(query_context, false);
            const StorageInMemoryMetadata & metadata_snapshot = *metadata;
            TableOverrideAnalyzer::Result override_info;
            TableOverrideAnalyzer override_analyzer(ast.getTableOverride());
            override_analyzer.analyze(metadata_snapshot, override_info);
            override_info.appendTo(buf);
            break;
        }
        case ASTExplainQuery::CurrentTransaction:
        {
            if (ast.getSettings())
                throw Exception(ErrorCodes::UNKNOWN_SETTING, "Settings are not supported for EXPLAIN CURRENT TRANSACTION query.");

            if (auto txn = query_context->getCurrentTransaction())
            {
                String dump = txn->dumpDescription();
                buf.write(dump.data(), dump.size());
            }
            else
            {
                writeCString("<no current transaction>", buf);
            }

            break;
        }
        case ASTExplainQuery::WhatIf:
        {
            const auto & query_ast = ast.getExplainedQuery();
            if (!dynamic_cast<const ASTSelectWithUnionQuery *>(query_ast.get()))
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT is supported for EXPLAIN WHATIF query");

            auto whatif_result = WhatIfIndexEstimator::run(query_ast, query_context, ast.getSettings());
            whatif_result.format(buf);
            break;
        }
        case DB::ASTExplainQuery::Analyze:
        {
            if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only SELECT is currently supported for EXPLAIN ANALYZE query");

            /// Distributed query planning rewrites the plan into exchange/remote steps, which EXPLAIN ANALYZE cannot execute here.
            if (query_context->getSettingsRef()[Setting::make_distributed_plan])
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "EXPLAIN ANALYZE doesn't support queries executed in distributed mode");

            /// Plan the inner SELECT. This is cached when ignoreQuota / ignoreLimits already triggered
            /// it during quota charging in executeQuery, so the inner query is never planned twice.
            /// getAnalyzedInnerQuery also validates the EXPLAIN ANALYZE settings (the same check that was
            /// previously done here), so invalid settings are still rejected, just without re-parsing.
            /// EXPLAIN ANALYZE executes the inner SELECT, so quota and result limits must follow the same
            /// rules as running that SELECT directly; the inner interpreter resolves the effective
            /// ignore_quota / ignore_limits during planning (e.g. exempt system tables such as `system.one`).
            auto & analyzed = getAnalyzedInnerQuery();
            QueryPlan plan = std::move(analyzed.plan);
            ContextPtr context = analyzed.context;
            auto parallel_replicas_builder = analyzed.parallel_replicas_builder;
            const bool inner_ignore_quota = analyzed.ignore_quota;
            const bool inner_ignore_limits = analyzed.ignore_limits;
            UInt64 planning_ns = analyzed.planning_ns;
            Stopwatch watch;

            auto optimization_settings = QueryPlanOptimizationSettings(context);

            optimization_settings.max_step_description_length = query_context->getSettingsRef()[Setting::query_plan_max_step_description_length];
            optimization_settings.query_plan_with_parallel_replicas_builder = parallel_replicas_builder;

            watch.restart();
            plan.optimize(optimization_settings);
            planning_ns += watch.elapsed();

            /// Build the per-plan pretty-names registry now: buildQueryPipeline below moves the ActionsDAGs
            /// out of the plan steps, so the names must be snapshotted before the pipeline consumes the plan.
            /// EXPLAIN ANALYZE rejects distributed plans above, so this covers the whole plan tree.
            PrettyNamesPerPlan precomputed_pretty_names = QueryPlanFormat::buildPrettyNamesPerPlan(plan);

            plan.setConcurrencyControl(context->getSettingsRef()[Setting::use_concurrency_control]);

            watch.restart();
            auto pipeline_builder = plan.buildQueryPipeline(optimization_settings, BuildQueryPipelineSettings(context), false);
            planning_ns += watch.elapsed();

            watch.restart();
            auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));

            pipeline.setNormalizedQueryHash(query_context->getNormalizedQueryHash());
            auto to_complete = options.to_stage == QueryProcessingStage::Complete;
            auto quota = (!inner_ignore_quota && to_complete) ? context->getQuota() : nullptr;

            /// setLimitsAndQuota attaches a transform, so it must run before the pipeline is completed below.
            if (!inner_ignore_limits && to_complete)
            {
                auto limits = StreamLocalLimits::forQueryResult(context->getSettingsRef());
                pipeline.setLimitsAndQuota(limits, quota);
            }

            if (quota)
                pipeline.setQuota(quota);

            pipeline.complete(std::make_shared<EmptySink>(pipeline.getSharedHeader()));

            /// Inspect the materialized pipeline rather than the plan: remote execution always shows up as one of
            /// these sources, including when it comes from nested sub-plans the plan walk would miss.
            for (const auto & processor : pipeline.getProcessors())
            {
                const auto * proc_ptr = processor.get();
                if (dynamic_cast<const RemoteSource *>(proc_ptr)
                    || dynamic_cast<const RemoteTotalsSource *>(proc_ptr)
                    || dynamic_cast<const RemoteExtremesSource *>(proc_ptr)
                    || dynamic_cast<const DelayedSource *>(proc_ptr))
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "EXPLAIN ANALYZE doesn't support queries executed in distributed mode");
            }

            planning_ns += watch.elapsed();

            auto step_wall_clock_registry = std::make_unique<StepWallClockRegistry>();
            step_wall_clock_registry->populateFromPlan(plan);
            pipeline.setStepWallClockRegistry(std::move(step_wall_clock_registry));

            CompletedPipelineExecutor executor(pipeline);

            if (auto cancel_callback = getContext()->getInteractiveCancelCallback())
                executor.setCancelCallback(
                    std::move(cancel_callback),
                    query_context->getSettingsRef()[Setting::interactive_delay] / 1000);

            auto outer_thread_group = CurrentThread::getGroup();
            if (!outer_thread_group)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "EXPLAIN ANALYZE: current thread is not attached to a thread group");

            auto analyze_thread_group = std::make_shared<ThreadGroup>(outer_thread_group);
            analyze_thread_group->memory_tracker.setDescription("EXPLAIN ANALYZE");

            watch.restart();
            {
                ThreadGroupSwitcher switcher(analyze_thread_group, ThreadName::COMPLETED_PIPELINE_EXECUTOR, /*allow_existing_group=*/true);
                executor.execute();
            }
            UInt64 execute_ns = watch.elapsed();

            UInt64 total_time_ns = planning_ns + execute_ns;

            UInt64 read_rows   = analyze_thread_group->performance_counters[ProfileEvents::SelectedRows];
            UInt64 read_bytes  = analyze_thread_group->performance_counters[ProfileEvents::SelectedBytes];
            Int64  peak_memory = analyze_thread_group->memory_tracker.getPeak();

            AnalyzeStepsStats steps_to_stats(pipeline, execute_ns);

            formatHeaderExplainAnalyze(total_time_ns, planning_ns, execute_ns, read_rows, read_bytes, peak_memory, buf);

            plan.explainPlan(buf,
            analyzed.query_plan_options,
            0,
            query_context->getSettingsRef()[Setting::query_plan_max_step_description_length],
            &precomputed_pretty_names,
            "",
            false,
            &steps_to_stats);
        }
    }
    buf.finalize();
    if (insert_buf)
    {
        if (single_line)
            res_columns[0]->insertData(buf.str().data(), buf.str().size());
        else
            fillColumn(*res_columns[0], buf.str());
    }

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(sample_block.cloneWithColumns(std::move(res_columns)))));
}

void registerInterpreterExplainQuery(InterpreterFactory & factory);
void registerInterpreterExplainQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    { return std::make_unique<InterpreterExplainQuery>(args.query, args.context, args.options); };
    factory.registerInterpreter("InterpreterExplainQuery", create_fn);
}

}
