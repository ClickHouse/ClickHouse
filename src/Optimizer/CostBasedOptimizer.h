#pragma once

#include <Optimizer/SubQueryPlan.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class CostBasedOptimizer
{
public:
    QueryPlan optimize(QueryPlan && plan, ContextPtr query_context);
};

}
