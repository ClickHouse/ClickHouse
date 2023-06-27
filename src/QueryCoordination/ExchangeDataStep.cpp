#include <QueryCoordination/ExchangeDataStep.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryCoordination/ExchangeDataReceiver.h>
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
        LOG_DEBUG(&Poco::Logger::get("ExchangeDataStep"), "Create ExchangeDataReceiver for {} fragment_id {} exchange_id {}", source, fragment_id, plan_id);
        auto receiver = std::make_shared<ExchangeDataReceiver>(output_stream.value(), fragment_id, plan_id, source);
        pipes.emplace_back(receiver);
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    /// TODO if has merge sort info, add MergeSortingTransform
    if (has_sort_info)
    {
        pipeline.init(std::move(pipe));
        mergingSorted(pipeline, sort_info.result_description, sort_info.limit);
    }
    else
    {
        pipe.resize(1);
        pipeline.init(std::move(pipe));
    }
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
            sort_info.max_block_size,
            /*max_block_size_bytes=*/0,
            SortingQueueStrategy::Batch,
            limit_,
            sort_info.always_read_till_end);

        pipeline.addTransform(std::move(transform));
    }
}

}
