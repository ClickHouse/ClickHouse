#include <Processors/QueryPlan/BroadcastSendStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/ISink.h>
#include <Processors/Port.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

QueryPipelineBuilderPtr BroadcastSendStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    /// Send copies of data to num_buckets outputs
    auto & pipeline = *pipelines.front();

    /// Serialize once on every stream; the copies for the destinations share the packets instead of
    /// serializing them again. The sinks are told whether they get packets.
    bool input_is_serialized = false;
    pipeline.addSimpleTransform([&](const SharedHeader & header) -> ProcessorPtr
    {
        auto transform = settings.exchange_lookup->createSerializer(header, exchange_id);
        input_is_serialized |= transform != nullptr;
        return transform;
    });
    pipeline.resize(1);
    if (num_buckets > 1)
        pipeline.addTransform(std::make_shared<CopyTransform>(pipeline.getSharedHeader(), num_buckets));

    const String shard_id = settings.parameter_lookup->getParameter("bucket_id").safeGet<String>();

    /// Add sink for each bucket
    size_t bucket = 0;
    pipeline.setSinks([&](const SharedHeader & header, Pipe::StreamType stream_type)
    {
        chassert(stream_type == Pipe::StreamType::Main);
        String destination_bucket_id = toString(bucket);
        ++bucket;
        return settings.exchange_lookup->createSink(header, ExchangeStreamId(exchange_id, shard_id, destination_bucket_id), input_is_serialized);
    });

    if (bucket != num_buckets)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BroadcastSendStep: expected {} buckets, but created only {}", num_buckets, bucket);

    return std::move(pipelines.front());
}

void BroadcastSendStep::serialize(Serialization & ctx) const
{
    writeStringBinary(exchange_id, ctx.out);
    writeVarUInt(num_buckets, ctx.out);
}

std::unique_ptr<IQueryPlanStep> BroadcastSendStep::deserialize(Deserialization & ctx)
{
    String exchange_id;
    readStringBinary(exchange_id, ctx.in);

    size_t num_buckets = 0;
    readVarUInt(num_buckets, ctx.in);

    return std::make_unique<BroadcastSendStep>(ctx.input_headers.front(), exchange_id, num_buckets);
}

void registerBroadcastSendStep(QueryPlanStepRegistry & registry);
void registerBroadcastSendStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("BroadcastSend", BroadcastSendStep::deserialize);
}

}
