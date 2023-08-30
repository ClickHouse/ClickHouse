#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryCoordination/Optimizer/StepTree.h>

namespace DB
{

class Optimizer
{
public:
    StepTree optimize(QueryPlan && plan, ContextPtr query_context);
};

}
