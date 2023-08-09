#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class Optimizer
{
public:
    QueryPlan optimize(QueryPlan && plan);
};

}
