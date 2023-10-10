#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryCoordination/Exchange/ExchangeDataSource.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Core/SortCursor.h>

namespace DB
{

void ExchangeDataStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/)
{
    Pipes pipes;

    for (const auto & source : sources)
    {
        LOG_DEBUG(&Poco::Logger::get("ExchangeDataStep"), "Create ExchangeDataSource for fragment {} exchange {} data from {} ", fragment_id, plan_id, source);
        auto receiver = std::make_shared<ExchangeDataSource>(output_stream.value(), fragment_id, plan_id, source);
        pipes.emplace_back(receiver);
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    pipeline.init(std::move(pipe));
}

}
