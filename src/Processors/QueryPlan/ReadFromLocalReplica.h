#pragma once

#include <Core/Names.h>
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

    /// Context of the subquery this local plan reads, carrying the same per-subquery
    /// SETTINGS that are shipped to remote replicas.
    ContextPtr getContext() const { return context; }

    void addFilter(FilterDAGInfo filter);

    /// Conditions already moved into the plan. A partial push-down leaves the original `Filter` in
    /// place, so the pass has to recognize what it has already taken and stop.
    bool hasPushedCondition(const String & name) const { return pushed_conditions.contains(name); }
    void notePushedCondition(const String & name) { pushed_conditions.insert(name); }

private:
    QueryPlanPtr query_plan;
    ContextPtr context;
    NameSet pushed_conditions;
};

}
