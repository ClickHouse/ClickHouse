#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class ReadFromLocalParallelReplicaStep : public ISourceStep
{
public:
    /// `can_ship_condition_` answers, for a concrete condition, whether the query shipped to the replicas
    /// would really carry it; empty means it never would.
    ReadFromLocalParallelReplicaStep(
        QueryPlanPtr query_plan_, ContextPtr context_, std::function<bool(const ActionsDAG &)> can_ship_condition_);

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

    /// Whether `condition` would really end up in the query shipped to the replicas, see
    /// `canAddFiltersToShippedQuery`. `parallel_replicas_filter_pushdown` being on is not on its own a
    /// promise that the replicas get it: the query may not take a predicate at all, and this one may not
    /// be expressible against what that query selects.
    bool shippedQueryCanCarry(const ActionsDAG & condition) const
    {
        return can_ship_condition && can_ship_condition(condition);
    }

    void addFilter(FilterDAGInfo filter);

private:
    QueryPlanPtr query_plan;
    ContextPtr context;
    std::function<bool(const ActionsDAG &)> can_ship_condition;
};

}
