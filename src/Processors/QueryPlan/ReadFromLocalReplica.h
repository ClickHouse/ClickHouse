#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class ReadFromLocalParallelReplicaStep : public ISourceStep
{
public:
    explicit ReadFromLocalParallelReplicaStep(QueryPlanPtr query_plan_);

    String getName() const override { return "ReadFromLocalReplica"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    QueryPlanPtr extractQueryPlan();

    /// Non-destructive access to the inner plan (for optimization passes that
    /// need to inspect or modify the local plan before it is extracted).
    QueryPlan * getQueryPlan() { return query_plan.get(); }

    void addFilter(FilterDAGInfo filter);

private:
    QueryPlanPtr query_plan;
};

}
