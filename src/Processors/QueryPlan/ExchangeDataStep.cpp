#include <Processors/QueryPlan/ExchangeDataStep.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryCoordination/ExchangeDataReceiver.h>
#include <QueryPipeline/StreamLocalLimits.h>

namespace DB
{

void ExchangeDataStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/)
{
    Pipes pipes;

    auto receiver = std::make_shared<ExchangeDataReceiver>(output_stream.value());

    pipes.emplace_back(receiver);

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    pipeline.init(std::move(pipe));
}

}
