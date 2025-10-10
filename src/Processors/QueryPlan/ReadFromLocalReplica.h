#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

namespace DB
{

class ReadFromLocalParallelReplicaStep : public SourceStepWithFilterBase
{
public:
    explicit ReadFromLocalParallelReplicaStep(QueryPlanPtr query_plan_);

    String getName() const override { return "ReadFromLocalReplica"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    QueryPlanPtr extractQueryPlan();

    void addFilter(ActionsDAG filter_dag, std::string column_name) override;

private:
    QueryPlanPtr query_plan;
};

}
