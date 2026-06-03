#pragma once

#include <Core/Names.h>
#include <Interpreters/Cache/QueryPlanCache.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Planner/Planner.h>

#include <optional>

namespace DB
{

std::optional<QueryPlanCacheLookupContext> tryBuildPreAnalysisQueryPlanCacheLookup(
    const ASTPtr & ast,
    const ContextPtr & context,
    UInt64 semantic_settings_hash);

Names getSelectedColumnsForQueryPlanCacheEntry(const PlannerContextPtr & planner_context);

QueryPlanCacheDependencyFingerprint buildQueryPlanCacheDependencyFingerprint(
    const QueryPlanCacheLookupContext & lookup_context,
    const ContextPtr & context,
    const Names & selected_columns);

bool validateQueryPlanCacheEntry(
    const QueryPlanCacheLookupContext & lookup_context,
    const ContextPtr & context,
    const QueryPlanCacheEntry & entry);

void checkAccessForQueryPlanCacheHit(const ContextPtr & context, const StorageID & storage_id, const Names & selected_columns);

}
