#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Marks the parallel replicas local/remote boundary in a query plan.
///
/// Inserted above the reading step and (later) pushed up the plan; everything below it becomes the
/// fragment that runs on the replicas. A dedicated phase replaces this step with a UNION of a local
/// read and a remote read of that fragment (see ClusterProxy::applyParallelReplicasSplit). It is a
/// transient marker: it never reaches pipeline generation in a parallel-replicas query. If a plan that
/// still contains it is executed directly, the step is a pass-through, so the query runs single-node.
class ParallelReplicasSplitStep final : public ITransformingStep
{
public:
    explicit ParallelReplicasSplitStep(SharedHeader input_header_, ContextPtr context_)
        : ITransformingStep(input_header_, input_header_, {})
        , context(context_)
    {
    }

    String getName() const override { return "ParallelReplicasSplit"; }

    /// Pass-through when executed directly (no split was applied). Out-of-line to anchor the vtable.
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    const ContextPtr & getContext() const { return context; }

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    ContextPtr context;
};

}
