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
    /// before a condition from outside is pushed in, and hold each of its reads to that. One snapshot
    /// per read: a fragment can hold several coordinated reads, and they do not fix the same columns.
    void restrictFixedColumnsToOwnFilters();

    /// Whether this fragment joins. `ReadFromRemote::addFilters` refuses to splice a condition into a
    /// shipped query whose join tree holds more than one table expression, so for such a fragment the
    /// replicas keep the query as it was however the settings are set.
    bool hasJoin() const;

private:
    QueryPlanPtr query_plan;
    ContextPtr context;
};

}
