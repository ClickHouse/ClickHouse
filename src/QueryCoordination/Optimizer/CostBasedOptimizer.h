#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryCoordination/Optimizer/SubQueryPlan.h>

namespace DB
{

class CostBasedOptimizer
{
public:
    QueryPlan optimize(QueryPlan && plan, ContextPtr query_context);
};

}
