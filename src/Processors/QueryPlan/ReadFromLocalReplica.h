#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class ReadFromLocalParallelReplicaStep : public ISourceStep
{
public:
    explicit ReadFromLocalParallelReplicaStep(QueryPlanPtr query_plan_, ContextPtr context_);

    String getName() const override { return "ReadFromLocalReplica"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    QueryPlanPtr extractQueryPlan();

    /// The fragment this step stands for, before it is spliced into the outer plan. Whoever adds a
    /// condition to it has to know what the fragment reads: see `fixedColumnMayChangeReadMode` in
    /// `filterPushDown.cpp`.
    const QueryPlan * getQueryPlan() const { return query_plan.get(); }

    /// Context of the subquery this local plan reads, carrying the same per-subquery
    /// SETTINGS that are shipped to remote replicas.
    ContextPtr getContext() const { return context; }

    void addFilter(FilterDAGInfo filter);

private:
    QueryPlanPtr query_plan;
    ContextPtr context;
};

}
