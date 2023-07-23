#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

enum StepType
{
    Source,
    Agg,
};

class PlanFragment;
using PlanFragmentPtr = std::shared_ptr<PlanFragment>;

struct PlanNode
{
    std::shared_ptr<IQueryPlanStep> step;
    std::vector<PlanNode *> children = {};

    UInt32 plan_id;

    StepType type;
};

}
