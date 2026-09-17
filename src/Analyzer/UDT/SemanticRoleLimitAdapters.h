#pragma once

#include <Analyzer/UDT/QueryTreeSemanticRoleGraph.h>
#include <Analyzer/UDT/SemanticRolePlanner.h>

#include <DataTypes/UDT/ResourceLimits.h>

namespace DB::UDT
{

/// Maps the common query resource contract onto the semantic analyzer's
/// component limits. Every mapped counter has the same charge identity; the
/// planner retains its narrower finite implementation limits for counters
/// without a common contract entry. In particular, graph DFS
/// depth is not logical type-path depth, and an individual shape is not a
/// diagnostic payload. Owned descriptor handles are also a local lifetime
/// guard: only the shared distinct-descriptor ledger owns the query counter.
/// Those component ceilings therefore remain intentionally local.
SemanticRolePlannerLimits makeSemanticRolePlannerLimits(const EffectiveResourceLimits & limits) noexcept;

/// Keeps finite adapter-local node/boundary guards and maps the authoritative
/// sealed sink enumeration to its query-wide counter. Demanded node/path
/// states remain planner-owned.
QueryTreeSemanticRoleGraphLimits makeQueryTreeSemanticRoleGraphLimits(const EffectiveResourceLimits & limits) noexcept;

}
