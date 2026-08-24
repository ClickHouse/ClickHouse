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

Names getSelectedColumnsForQueryPlanCacheEntry(const PlannerContextPtr & planner_context);

Names getReadColumnsForQueryPlanCacheEntry(const QueryPlan & plan);

std::vector<QueryPlanCacheStorageDependency> buildQueryPlanCacheDependencies(
    const QueryPlanCacheLookupContext & lookup_context,
    const QueryPlan & plan,
    const PlannerContextPtr & planner_context,
    const Names & selected_columns);

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
