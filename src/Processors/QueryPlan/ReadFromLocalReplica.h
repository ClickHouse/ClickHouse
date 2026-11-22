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
    /// The conjuncts of a condition that the replicas would not get. See `conjunctsTheReplicasMiss`.
    using LeftOutConjuncts = std::function<ActionsDAG::NodeRawConstPtrs(const ActionsDAG::Node & condition)>;

    explicit ReadFromLocalParallelReplicaStep(QueryPlanPtr query_plan_, ContextPtr context_, LeftOutConjuncts left_out_conjuncts_ = {});

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

    /// The conjuncts (`ActionsDAG::extractConjunctionAtoms`) of a condition pushed down onto this step that
    /// the replicas would not get: those the splice into the query they are sent leaves out, or all of
    /// them when nothing is spliced.
    ActionsDAG::NodeRawConstPtrs conjunctsTheReplicasMiss(const ActionsDAG::Node & condition) const;

private:
    QueryPlanPtr query_plan;
    ContextPtr context;
    LeftOutConjuncts left_out_conjuncts;
};

}
