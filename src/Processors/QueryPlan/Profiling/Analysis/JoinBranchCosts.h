#pragma once

#include <optional>
#include <unordered_map>
#include <vector>
#include <base/types.h>

namespace DB
{

/// One node of the plan, as the branch costs see it. Nodes are identified by the step's uniq id
/// rather than its address, so the costs can be computed once the plan itself is gone.
struct JoinBranchNode
{
    String id;
    std::vector<String> children;

    bool is_join = false;
    /// Joins of one cluster share their cost. Zero means the join belongs to no cluster.
    UInt64 cluster_id = 0;
};

using JoinBranchNodes = std::vector<JoinBranchNode>;

using CardinalityByJoinStep = std::unordered_map<String, std::optional<UInt64>>;

/// The cost of a join is its own cardinality plus the cardinality of every join of the same cluster
/// below it. A cost is absent as soon as any contributor's is.
class JoinBranchCosts
{
public:
    JoinBranchCosts(const JoinBranchNodes & nodes, const String & root_id, const CardinalityByJoinStep & cardinality_by_join_step);

    std::optional<UInt64> getBranchCost(const String & step_id) const;

private:
    std::unordered_map<String, std::optional<UInt64>> cost_by_join_step;
};

}
