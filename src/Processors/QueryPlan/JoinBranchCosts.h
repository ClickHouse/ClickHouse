#pragma once

#include <optional>
#include <unordered_map>
#include <Processors/QueryPlan/QueryPlan.h>
#include <base/types.h>

namespace DB
{

class JoinStep;

using CardinalityByJoinStep = std::unordered_map<const JoinStep *, std::optional<UInt64>>;

class JoinBranchCosts
{
public:
    JoinBranchCosts(const QueryPlan & plan, const CardinalityByJoinStep & cardinality_by_join_step);

    std::optional<UInt64> getBranchCost(const JoinStep * join_step) const;

private:
    void accumulate(const QueryPlan::Node * root, const CardinalityByJoinStep & cardinality_by_join_step);

    std::unordered_map<const JoinStep *, std::optional<UInt64>> cost_by_join_step;
};

}
