#pragma once

#include <Processors/IProcessor.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>

#include <functional>

namespace DB
{

class QueryPipelineBuilder;

/// Builds a pipeline with the given callback when its output is needed for the first time,
/// then forwards the data of that pipeline. If the output is never needed, the callback is never called.
///
/// Unlike `DelayedSource`, it is not treated as a remote source when the rows before `LIMIT` are counted,
/// so it is suitable for local reads.
class OnDemandPipelineSource final : public IProcessor
{
public:
    using Creator = std::function<QueryPipelineBuilder()>;

    OnDemandPipelineSource(SharedHeader header, Creator creator_);

    String getName() const override { return "OnDemandPipelineSource"; }

    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;

private:
    Creator creator;

    QueryPlanResourceHolder resources;
    Processors processors;
    OutputPort * pipeline_output = nullptr;
};

}
