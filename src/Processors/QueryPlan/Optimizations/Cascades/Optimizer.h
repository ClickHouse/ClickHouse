#pragma once

#include <Processors/QueryPlan/QueryPlan.h>


namespace DB
{

class CascadesOptimizer
{
public:
    void optimize(QueryPlan & query_plan);
};

}
