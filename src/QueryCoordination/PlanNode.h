#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class PlanFragment;
using PlanFragmentPtr = std::shared_ptr<PlanFragment>;

struct PlanNode
{
    std::shared_ptr<IQueryPlanStep> step;
    std::vector<PlanNode *> children = {};

    PlanFragmentPtr fragment;
    UInt32 plan_id;
};

using PlanNodePtr = std::shared_ptr<PlanNode>;

}
