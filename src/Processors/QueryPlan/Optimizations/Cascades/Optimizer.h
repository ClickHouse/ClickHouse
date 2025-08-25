#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>


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
    JoinGraph buildJoinGraph();
    GroupId populateMemo(const JoinGraph & join_graph, OptimizerContext & optimizer_context);

    QueryPlan & query_plan;
};

}
