#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

class QueryTreePassManager;

/// The `SELECT` access checks a resolved query tree needs when it is never planned and read.
///
/// The planner checks `SELECT` on every table expression it plans (`prepareBuildQueryPlanForTableExpression`),
/// and the storages perform the rest when the plan is actually built: `StorageView::readImpl` resolves the
/// view's inner query and checks the base tables, `StorageAlias::read` checks the target table. Two callers
/// resolve a query tree without ever reaching those points and would otherwise hand out table metadata (column
/// names and types) the user may not read:
///  - `EXPLAIN QUERY TREE` / `EXPLAIN SYNTAX` under the analyzer (`InterpreterExplainQuery`), which only
///    resolve the query (https://github.com/ClickHouse/ClickHouse/issues/78938);
///  - the `only_analyze` planner used by `InterpreterCreateQuery::getSampleBlock` for `CREATE ... AS SELECT`,
///    which neither plans subqueries recursively nor reads any storage.
/// These helpers reproduce the full read-time contract for both, so a statement is denied exactly when the
/// real `SELECT` of the same query is.

/// Reproduce the `SELECT` check for every table referenced anywhere in `query_tree`, including subqueries in
/// expressions, each with the context of the scope it appears in, followed by the read-time checks of
/// `checkReadTimeAccessForTableExpression` for views and `Alias` tables. Denials throw `ACCESS_DENIED`.
///
/// Returns whether the check ran in full. With `skip_if_unresolvable`, the recursive base-table pass for a
/// non-inlined view skips itself (see `checkViewBaseTableAccess`) when the view's inner query cannot be
/// resolved by the analyzer, and `false` reports that skip so the caller can fail close (e.g. dump a
/// non-resolved tree instead). Without it the resolution error propagates, exactly as a real read of the view
/// would fail, and the function only ever returns `true`.
bool checkAccessRightsForQueryTree(QueryTreeNodePtr & query_tree, const ContextPtr & query_context, bool skip_if_unresolvable = true);

/// The checks a real read of one table expression performs only when the plan is built, after the planner's
/// own `SELECT` check on the object has passed: `SELECT` on the same `column_names` of an `Alias` table's
/// target (`StorageAlias::read`), and the recursive base-table pass for a view (`StorageView::readImpl`, see
/// `checkViewBaseTableAccess`). An empty `column_names` means a trivial read that asks for no specific column
/// (`SELECT count() FROM t`); the `Alias` case of it is already covered by the planner's any-column fallback.
/// Any other storage passes. Returns whether the check ran in full, see `checkAccessRightsForQueryTree`.
bool checkReadTimeAccessForTableExpression(
    const StoragePtr & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const Names & column_names,
    const ContextPtr & scope_context,
    bool skip_if_unresolvable = true);

/// Reproduce the base-table access check `StorageView::readImpl` performs when actually reading through a
/// regular (non-parameterized) view, by resolving the view's inner query under its own security context and
/// running `checkAccessRightsForQueryTree` on it (covering nested views too). `column_names` are the columns
/// requested from the view itself, matching the `column_names` real execution passes into
/// `InterpreterSelectQueryAnalyzer` / `InterpreterSelectWithUnionQuery` for the view's inner query - so a user
/// who can read only some of the view's output columns is not also required to have access to base columns
/// the view happens to select but this particular read never asked for. When `column_names` is empty (a
/// trivial read such as `SELECT count() FROM v`), the same single cheapest readable view column the planner
/// would pick (`chooseSmallestColumnToReadFromStorage`) is used, mirroring `prepareBuildQueryPlanForTableExpression`.
///
/// Returns whether the base-table access check was actually performed. With `skip_if_unresolvable` it is
/// skipped (returning `false`) when the view's inner query cannot be resolved by the analyzer - the same
/// "format but do not resolve" shapes (`NOT_IMPLEMENTED`, `BAD_ARGUMENTS`, remote table-function connection
/// errors, ...) that `resolveThenCheckAccessRights` handles for a top-level explained query. A caller about
/// to expand and dump the view body must then fall back to the unexpanded view reference, since no `SELECT`
/// check on the base tables ever ran.
bool checkViewBaseTableAccess(
    const StoragePtr & view_storage,
    const StorageSnapshotPtr & view_snapshot,
    const ContextPtr & scope_context,
    const Names & column_names,
    bool skip_if_unresolvable = true);

/// Resolve a throwaway `query_tree` (which the caller owns) and run `checkAccessRightsForQueryTree` on it.
///
/// With `skip_if_unresolvable`, a query that cannot be resolved (an invalid or fuzzed query, or a table
/// function whose arguments the analyzer intentionally does not evaluate for `EXPLAIN SYNTAX`) has no
/// resolved metadata to protect, and a real query would fail with the same resolution error before the
/// planner's access check, so the check is skipped rather than turning a formatting request into a
/// resolution error. This also covers a remote table function (e.g. `paimonAzure`, `url`) that throws a
/// non-`DB::Exception` while connecting during resolution: `EXPLAIN QUERY TREE run_passes = 0` dumps the
/// unresolved tree and must not turn into a connection error. An `ACCESS_DENIED` raised during resolution
/// is always propagated. Returns whether the access check was actually performed in full, so a caller about
/// to dump something the check was supposed to protect can tell that it never ran (the query did not
/// resolve) or ran only partially (a non-inlined view's base-table pass skipped itself).
bool resolveThenCheckAccessRights(
    QueryTreeNodePtr query_tree, QueryTreePassManager & pass_manager, const ContextPtr & query_context, bool skip_if_unresolvable = true);

}
