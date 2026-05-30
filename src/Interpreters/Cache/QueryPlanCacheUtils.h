#pragma once

#include <Core/Names.h>
#include <Interpreters/Cache/QueryPlanCache.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Planner/Planner.h>

#include <optional>

namespace DB
{

std::optional<QueryPlanCacheKey> tryBuildQueryPlanCacheKey(
    const ASTPtr & ast,
    const ContextPtr & context,
    UInt64 semantic_settings_hash);

Names getSelectedColumnsForQueryPlanCacheEntry(const PlannerContextPtr & planner_context);

void checkAccessForQueryPlanCacheHit(const ContextPtr & context, const StorageID & storage_id, const Names & selected_columns);

}
