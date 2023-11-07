#include <Core/SortCursor.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <QueryCoordination/Exchange/ExchangeDataSource.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/StreamLocalLimits.h>

namespace DB
{

void ExchangeDataStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/)
{
    Pipes pipes;

    for (const auto & source : sources)
    {
        LOG_DEBUG(
            &Poco::Logger::get("ExchangeDataStep"),
            "Create ExchangeDataSource for fragment {} exchange {} data from {} ",
            fragment_id,
            plan_id,
            source);
        auto receiver = std::make_shared<ExchangeDataSource>(output_stream.value(), fragment_id, plan_id, source);
        pipes.emplace_back(receiver);
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    if (source_merge)
    {
        pipeline.init(std::move(pipe));
        mergingSorted(pipeline, sort_description, 0);
    }

    pipeline.init(std::move(pipe));
}

void ExchangeDataStep::mergingSorted(QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, UInt64 limit_)
{
    /// If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1)
    {
        auto transform = std::make_shared<MergingSortedTransform>(
            pipeline.getHeader(),
            pipeline.getNumStreams(),
            result_sort_desc,
            max_block_size,
            /*max_block_size_bytes=*/0,
            SortingQueueStrategy::Batch,
            limit_,
            true);

        pipeline.addTransform(std::move(transform));
    }
}

}
