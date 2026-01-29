#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>


namespace DB
{

class OptimizerContext;
class JoinGraph;

class CascadesOptimizer
{
public:
    explicit CascadesOptimizer(QueryPlan & query_plan_);

    void optimize();

private:
    GroupId fillMemoFromQueryPlan(OptimizerContext & optimizer_context);
    GroupId populateMemoFromJoinGraph(const JoinGraph & join_graph, OptimizerContext & optimizer_context);
    QueryPlanPtr buildBestPlan(GroupId subtree_root_group_id, ExpressionProperties required_properties, const Memo & memo);

    QueryPlan & query_plan;
};

}
