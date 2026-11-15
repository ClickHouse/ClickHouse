#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>

namespace DB
{

/// Query-parameter names the distributed Cascades planner and its tests use. Kept in one place
/// so the planner, the executor and the hint parser cannot drift on the spelling.
namespace CascadesParams
{
    constexpr auto STAT_HINTS = "_internal_join_table_stat_hints";
    constexpr auto CLUSTER_NODE_COUNT = "_internal_cascades_cluster_node_count";
    constexpr auto TASK_LIMIT = "_internal_cascades_task_limit";
    constexpr auto COST_CONFIG = "_internal_cascades_cost_config";
}

/// Returns the value of `_internal_cascades_cluster_node_count`, or 0 if not set. Used by the
/// optimizer to determine cluster size and by the executor to cap the host list.
size_t getCascadesClusterNodeCountParam(ContextPtr context);

/// Task budget for the Cascades optimizer, overridable via `_internal_cascades_task_limit`
/// (0 or absent -> default_limit).
size_t getCascadesTaskLimitParam(ContextPtr context, size_t default_limit);

}
