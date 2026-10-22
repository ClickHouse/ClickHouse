#pragma once

#include <vector>
#include <base/types.h>

namespace DB
{

/// One node of the plan, reduced to what the passes that run across steps need: who is below whom.
/// Nodes are identified by the step's uniq id rather than its address, so the shape outlives the
/// plan. A node's children are its own children together with the roots of the child plans its
/// step holds, because both are below it for every purpose here.
struct PlanShapeNode
{
    String id;
    std::vector<String> children;

    bool is_join = false;
    /// Joins of one cluster share their cost. Zero means the join belongs to no cluster.
    UInt64 cluster_id = 0;
};

/// Every step of a query, in no particular order. Walk it from `PlanAnalysisSnapshot::root_id`.
using PlanShape = std::vector<PlanShapeNode>;

}
