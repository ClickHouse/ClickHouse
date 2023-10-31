#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryCoordination/Optimizer/Cost/Cost.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>

namespace DB
{

class PlanFragment;
using PlanFragmentPtr = std::shared_ptr<PlanFragment>;

struct PlanNode
{
    std::shared_ptr<IQueryPlanStep> step;
    std::vector<PlanNode *> children = {};

    UInt32 plan_id;

    /// Just for explain
    Cost cost;
    Statistics statistics;
};

}
