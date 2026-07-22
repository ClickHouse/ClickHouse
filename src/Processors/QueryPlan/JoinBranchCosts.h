#pragma once

#include <optional>
#include <unordered_map>
#include <vector>
#include <Processors/QueryPlan/QueryPlan.h>
#include <base/types.h>

namespace DB
{

class JoinStep;

using CardinalityByJoinStep = std::unordered_map<const JoinStep *, UInt64>;

class JoinBranchCosts
{
public:
    JoinBranchCosts() = default;
    JoinBranchCosts(const QueryPlan & plan, const CardinalityByJoinStep & cardinality_by_join_step);

    std::optional<double> getBranchCost(const JoinStep * join_step) const;

private:
    std::vector<const JoinStep *> accumulate(QueryPlan::Node * node, const CardinalityByJoinStep & cardinality_by_join_step);

    std::unordered_map<const JoinStep *, double> cost_by_join_step;
};

}
