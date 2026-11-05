#pragma once

#include <Core/Names.h>
#include <Interpreters/Cache/QueryPlanCache.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Planner/Planner.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/StorageSnapshot.h>

#include <optional>

namespace DB
{

std::optional<QueryPlanCacheLookupContext>
tryBuildPreAnalysisQueryPlanCacheLookup(const ASTPtr & ast, const ContextPtr & context, UInt64 semantic_settings_hash);

bool astContainsInTableExpressionForQueryPlanCache(ASTPtr ast);

/// Checks the analyzed query tree for nondeterministic functions that are not visible in the AST:
/// identifiers resolved to functions and expanded `ALIAS` columns. Such plans must not be cached.
bool queryTreeContainsNonDeterministicFunctionsForQueryPlanCache(const QueryTreeNodePtr & query_tree);

/// Row policies are outside the cache contract: any applicable policy, including an always-true
/// one, bypasses lookup and insertion. Policies can be created or dropped while a query runs, so
/// this is checked again on the hit path and before inserting a freshly built plan.
bool hasRowPolicyForQueryPlanCache(const ContextPtr & context, const StorageID & storage_id);

/// The target of a refreshable materialized view is resolved by `RefreshTask::getAndLockTargetTable`,
/// which synchronizes a newly observed target table (e.g. one refreshed on another replica) before
/// reading it. A cache hit would bypass that, so such tables are outside the cache contract.
bool isRefreshTargetForQueryPlanCache(const ContextPtr & context, const StorageID & storage_id);

Names getSelectedColumnsForQueryPlanCacheEntry(const PlannerContextPtr & planner_context);

Names getReadColumnsForQueryPlanCacheEntry(const PlannerContextPtr & planner_context);

Names getAccessCheckedColumnsForQueryPlanCacheEntry(const PlannerContextPtr & planner_context);

std::vector<QueryPlanCacheStorageDependency> buildQueryPlanCacheDependencies(
    const QueryPlanCacheLookupContext & lookup_context,
    const QueryPlan & plan,
    const PlannerContextPtr & planner_context,
    const Names & selected_columns,
    const Names & read_columns,
    const Names & access_checked_columns);

struct ValidatedQueryPlanCacheEntry
{
    StorageID storage_id = StorageID::createEmpty();
    String table_name;
    Names selected_columns;
    Names read_columns;
    StorageMetadataPtr metadata_snapshot;
    StoragePtr storage;
    StorageSnapshotPtr storage_snapshot;
    TableLockHolder table_lock;
};

std::optional<ValidatedQueryPlanCacheEntry> validateQueryPlanCacheEntryAndBuildSnapshot(
    const QueryPlanCacheLookupContext & lookup_context, const ContextPtr & context, const QueryPlanCacheEntry & entry);

/// Rechecks the SELECT privilege for the current user: for `selected_columns` in the same way as the miss path
/// does (a trivial query needs a grant on any one column), and for every column of `access_checked_columns`.
void checkAccessForQueryPlanCacheHit(
    const ContextPtr & context,
    const StorageID & storage_id,
    const StorageMetadataPtr & metadata_snapshot,
    const Names & selected_columns,
    const Names & access_checked_columns);

void checkStorageSupportsTransactionsForQueryPlanCacheHit(
    const ContextPtr & context, const StoragePtr & storage);
}
