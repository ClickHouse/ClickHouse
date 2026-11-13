#pragma once

#include <optional>
#include <unordered_map>
#include <Processors/QueryPlan/Profiling/Analysis/PlanAnalysisSnapshot.h>
#include <base/types.h>

namespace DB
{

using CardinalityByJoinStep = std::unordered_map<String, std::optional<UInt64>>;

/// The cost of a join is its own cardinality plus the cardinality of every join of the same cluster
/// below it. A cost is absent as soon as any contributor's is.
class JoinBranchCosts
{
public:
    JoinBranchCosts(const PlanAnalysisSnapshot & plan_snapshot, const CardinalityByJoinStep & cardinality_by_join_step);

    std::optional<UInt64> getBranchCost(const String & step_id) const;

private:
    std::unordered_map<String, std::optional<UInt64>> cost_by_join_step;
};

}
