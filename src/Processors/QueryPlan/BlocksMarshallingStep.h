#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// The part of the decision to pre-serialize result blocks that the connection context answers:
/// the setting, and the two cases marshalling is not supported in. Callers add the structural
/// conditions of their own path - that the blocks really do leave this process over the network,
/// and that the plan is the one a peer executes. See `Planner::buildPlanForQueryNode` and
/// `createRemotePlanFragmentForParallelReplicas`, the two places a `BlocksMarshallingStep` is added.
bool contextAllowsBlocksMarshalling(const Context & context);

/// Serializes and compresses input blocks to avoid doing it on TCPHandler.
class BlocksMarshallingStep : public ITransformingStep
{
public:
    explicit BlocksMarshallingStep(const SharedHeader & input_header_);

    String getName() const override { return "BlocksMarshalling"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization &) const override { }
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override;
};

}
