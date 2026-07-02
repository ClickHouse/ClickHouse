#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <utility>


namespace DB
{

class OptimizerContext;

class CascadesOptimizer
{
public:
    explicit CascadesOptimizer(QueryPlan & query_plan_);

    void optimize();

private:
    QueryPlanPtr buildBestPlan(GroupId subtree_root_group_id, ExpressionProperties required_properties, const Memo & memo);

    QueryPlan & query_plan;
};

}
