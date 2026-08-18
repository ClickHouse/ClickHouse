#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <utility>


namespace DB
{

class OptimizerContext;

struct QueryPlanOptimizationSettings;

class CascadesOptimizer
{
public:
    CascadesOptimizer(QueryPlan & query_plan_, const QueryPlanOptimizationSettings & optimization_settings_);

    void optimize();

private:
    QueryPlanPtr buildBestPlan(GroupId subtree_root_group_id, ExpressionProperties required_properties, const Memo & memo);

    QueryPlan & query_plan;
    const QueryPlanOptimizationSettings & optimization_settings;
};

}
