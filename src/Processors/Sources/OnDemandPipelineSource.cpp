#include <Processors/Port.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/OnDemandPipelineSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

OnDemandPipelineSource::OnDemandPipelineSource(SharedHeader header, Creator creator_)
    : IProcessor({}, {std::move(header)})
    , creator(std::move(creator_))
{
}

IProcessor::Status OnDemandPipelineSource::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    /// Build the pipeline only when the output is needed.
    if (inputs.empty())
    {
        if (!output.isNeeded())
            return Status::PortFull;

        return pipeline_output ? Status::UpdatePipeline : Status::Ready;
    }

    if (!output.canPush())
        return Status::PortFull;

    auto & input = inputs.front();
    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    output.push(input.pull());
    return Status::PortFull;
}

void OnDemandPipelineSource::work()
{
    auto pipe = QueryPipelineBuilder::getPipe(creator(), resources);
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(outputs.front().getSharedHeader()));

    pipe.resize(1);
    pipeline_output = pipe.getOutputPort(0);
    processors = Pipe::detachProcessors(std::move(pipe));
}

IProcessor::PipelineUpdate OnDemandPipelineSource::updatePipeline()
{
    inputs.emplace_back(outputs.front().getHeader(), this);
    /// `connect` checks that the headers of the ports are the same.
    connect(*pipeline_output, inputs.back());
    inputs.back().setNeeded();

    /// Attribute the new processors to the step of this one, for `EXPLAIN ANALYZE`.
    for (auto & processor : processors)
        processor->inheritQueryPlanStepFromParent(*this, getQueryPlanStepGroup());

    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

}
