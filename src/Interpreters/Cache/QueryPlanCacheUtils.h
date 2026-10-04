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

/// Row policies are outside the cache contract: any applicable policy, including an always-true
/// one, bypasses lookup and insertion. Policies can be created or dropped while a query runs, so
/// this is checked again on the hit path and before inserting a freshly built plan.
bool hasRowPolicyForQueryPlanCache(const ContextPtr & context, const StorageID & storage_id);

Names getSelectedColumnsForQueryPlanCacheEntry(const PlannerContextPtr & planner_context);

Names getReadColumnsForQueryPlanCacheEntry(const PlannerContextPtr & planner_context);

std::vector<QueryPlanCacheStorageDependency> buildQueryPlanCacheDependencies(
    const QueryPlanCacheLookupContext & lookup_context,
    const QueryPlan & plan,
    const PlannerContextPtr & planner_context,
    const Names & selected_columns,
    const Names & read_columns);

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

void checkAccessForQueryPlanCacheHit(
    const ContextPtr & context, const StorageID & storage_id, const StorageMetadataPtr & metadata_snapshot, const Names & selected_columns);

void checkStorageSupportsTransactionsForQueryPlanCacheHit(
    const ContextPtr & context, const StoragePtr & storage);
}
