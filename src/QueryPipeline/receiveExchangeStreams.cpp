#include <QueryPipeline/receiveExchangeStreams.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

QueryPipelineBuilder receiveExchangeStreams(
    const SharedHeader & output_header,
    const String & exchange_id,
    const VectorWithMemoryTracking<ExchangeStreamId> & stream_ids,
    const BuildQueryPipelineSettings & settings,
    bool spread_over_max_threads)
{
    /// Ask for one deserializer up front: whether there is one decides what the sources output.
    auto first_deserializer = settings.exchange_lookup->createDeserializer(output_header, exchange_id);
    const bool sources_output_packets = first_deserializer != nullptr;

    Pipes pipes;
    for (const auto & stream_id : stream_ids)
        pipes.emplace_back(Pipe(settings.exchange_lookup->createSource(output_header, stream_id, sources_output_packets)));

    QueryPipelineBuilder pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));
    if (!sources_output_packets)
        return pipeline;

    if (spread_over_max_threads && settings.max_threads > pipeline.getNumStreams())
        pipeline.resize(settings.max_threads);

    pipeline.addSimpleTransform([&](const SharedHeader &) -> std::shared_ptr<IProcessor>
    {
        if (first_deserializer)
            return std::exchange(first_deserializer, nullptr);
        return settings.exchange_lookup->createDeserializer(output_header, exchange_id);
    });
    return pipeline;
}

}
