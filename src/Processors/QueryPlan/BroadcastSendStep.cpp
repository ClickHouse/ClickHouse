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
    /// Add calculation of hash of key columns and bucket id based on the hash
    /// Add fork processor to send data to num_buckets outputs
    auto & pipeline = *pipelines.front();
    auto stream_header = pipeline.getSharedHeader();
    {
        pipeline.resize(1);
        if (num_buckets > 1)
        {
            /// Copies the input block to num_buckets outputs
            auto copy = std::make_shared<CopyTransform>(stream_header, num_buckets);
            pipeline.addTransform(copy);
        }
    }

    const String shard_id = settings.parameter_lookup->getParameter("bucket_id").safeGet<String>();

    /// Add sink for each bucket
    size_t bucket = 0;
    pipeline.setSinks([&](const SharedHeader & header, Pipe::StreamType stream_type)
    {
        chassert(stream_type == Pipe::StreamType::Main);
        String destination_bucket_id = toString(bucket);
        ++bucket;   /// TODO: this is a hack. Find a better way to assigning bucket id to each sink.
        return settings.exchange_lookup->createSink(header, ExchangeStreamId(exchange_id, shard_id, destination_bucket_id));
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

    size_t num_buckets;
    readVarUInt(num_buckets, ctx.in);

    return std::make_unique<BroadcastSendStep>(ctx.input_headers.front(), exchange_id, num_buckets);
}

void registerBroadcastSendStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("BroadcastSend", BroadcastSendStep::deserialize);
}

}
