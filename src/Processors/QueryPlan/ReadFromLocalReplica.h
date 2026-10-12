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
    ReadFromLocalParallelReplicaStep(QueryPlanPtr query_plan_, ContextPtr context_, bool replicas_get_pushed_conditions_);

    String getName() const override { return "ReadFromLocalReplica"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    QueryPlanPtr extractQueryPlan();

    /// The local plan is held inside this step rather than as a child node, so plan-wide walks must
    /// descend through here explicitly to reach it.
    const QueryPlan * getQueryPlan() const { return query_plan.get(); }

    /// Context of the subquery this local plan reads, carrying the same per-subquery
    /// SETTINGS that are shipped to remote replicas.
    ContextPtr getContext() const { return context; }

    /// Whether a condition pushed into this fragment also reaches the replicas running it. It does when
    /// they were given a query to run and the rewrite that splices the condition into that query accepts
    /// it (`canSpliceFiltersIntoRemoteQuery`); it does not when they were given a plan, which is built
    /// and shipped whole. Answered where the fragment is shipped, and carried here rather than guessed
    /// at from the shape of the local copy.
    bool replicasGetPushedConditions() const { return replicas_get_pushed_conditions; }

    void addFilter(FilterDAGInfo filter);

    /// Take the snapshot of what this fragment fixes on its own - what the replicas will fix too -
    /// before a condition from outside is pushed in, and hold each of its reads to that. One snapshot
    /// per read: a fragment can hold several coordinated reads, and they do not fix the same columns.
    void restrictFixedColumnsToOwnFilters();

private:
    QueryPlanPtr query_plan;
    ContextPtr context;
    bool replicas_get_pushed_conditions;
};

}
