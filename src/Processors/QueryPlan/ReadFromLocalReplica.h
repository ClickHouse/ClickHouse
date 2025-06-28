#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{
class ReadFromLocalParallelReplicaStep : public ISourceStep
{
public:
    explicit ReadFromLocalParallelReplicaStep(QueryPlanPtr query_plan_);

    String getName() const override { return "ReadFromLocalReplica"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    QueryPlanPtr extractQueryPlan();

    bool hasOptimized() const { return has_optimized; }
    void setOptimized(bool optimized) { has_optimized = optimized; }

private:
    QueryPlanPtr query_plan;
    bool has_optimized = false;
};

}
