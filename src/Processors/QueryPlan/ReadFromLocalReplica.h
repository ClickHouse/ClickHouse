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

    /// The local plan is held inside this step rather than as a child node, so plan-wide walks must
    /// descend through here explicitly to reach it.
    const QueryPlan * getQueryPlan() const { return query_plan.get(); }

    /// Context of the subquery this local plan reads, carrying the same per-subquery
    /// SETTINGS that are shipped to remote replicas.
    ContextPtr getContext() const { return context; }

    void addFilter(FilterDAGInfo filter);

    /// Take the snapshot of what this fragment fixes on its own - what the replicas will fix too -
    /// before a condition from outside is pushed in, and hold every read in it to that. One snapshot
    /// for the whole fragment: it is taken along the chain read-in-order itself walks, so a second read
    /// off to the side of a join is held to the first one's columns and may lose an ordering it could
    /// have had. Ordering is what has to be withheld here, so erring towards less of it is the safe way
    /// to be imprecise.
    void restrictFixedColumnsToOwnFilters();

private:
    QueryPlanPtr query_plan;
    ContextPtr context;
};

}
