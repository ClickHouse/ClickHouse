#pragma once

#include <Core/Names.h>
#include <Interpreters/Cache/QueryPlanCache.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Planner/Planner.h>
#include <Storages/StorageSnapshot.h>

#include <optional>

namespace DB
{

std::optional<QueryPlanCacheLookupContext> tryBuildPreAnalysisQueryPlanCacheLookup(
    const ASTPtr & ast,
    const ContextPtr & context,
    UInt64 semantic_settings_hash);

bool astContainsInTableExpressionForQueryPlanCache(ASTPtr ast);

Names getSelectedColumnsForQueryPlanCacheEntry(const PlannerContextPtr & planner_context);

QueryPlanCacheDependencyFingerprint buildQueryPlanCacheDependencyFingerprint(
    const QueryPlanCacheLookupContext & lookup_context,
    const ContextPtr & context,
    const Names & selected_columns);

struct ValidatedQueryPlanCacheEntry
{
    StorageID storage_id = StorageID::createEmpty();
    Names selected_columns;
    StorageMetadataPtr metadata_snapshot;
    std::vector<QueryPlanStorageBinding> storage_bindings;
};

std::optional<ValidatedQueryPlanCacheEntry> validateQueryPlanCacheEntryAndBuildSnapshot(
    const QueryPlanCacheLookupContext & lookup_context,
    const ContextPtr & context,
    const QueryPlanCacheEntry & entry);

void checkAccessForQueryPlanCacheHit(
    const ContextPtr & context,
    const StorageID & storage_id,
    const StorageMetadataPtr & metadata_snapshot,
    const Names & selected_columns);

void checkStoragesSupportTransactionsForQueryPlanCacheHit(
    const ContextPtr & context,
    const std::vector<QueryPlanStorageBinding> & storage_bindings);

}
