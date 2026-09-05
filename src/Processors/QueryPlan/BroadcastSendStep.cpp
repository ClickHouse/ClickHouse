#include <Processors/QueryPlan/BroadcastSendStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/StepManifest.h>
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
        ++bucket;
        return settings.exchange_lookup->createSink(header, ExchangeStreamId(exchange_id, shard_id, destination_bucket_id));
    });

    if (bucket != num_buckets)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BroadcastSendStep: expected {} buckets, but created only {}", num_buckets, bucket);

    return std::move(pipelines.front());
}

namespace
{

constexpr auto BROADCAST_SEND_MANIFEST = StepManifest<BroadcastSendStep, BroadcastSendWire>("BroadcastSend")
    .nameIntroducedIn(1)
    .inputs(1)
    .baseFormat(
        field("exchange_id", WireFieldClass::Logical, &BroadcastSendWire::exchange_id),
        field("num_buckets", WireFieldClass::Physical, &BroadcastSendWire::num_buckets));

}

BroadcastSendWire BroadcastSendStep::toWire() const
{
    return BroadcastSendWire{exchange_id, num_buckets};
}

QueryPlanStepPtr BroadcastSendStep::fromWire(BroadcastSendWire wire, Deserialization & ctx)
{
    return std::make_unique<BroadcastSendStep>(ctx.input_headers.front(), std::move(wire.exchange_id), wire.num_buckets);
}

void BroadcastSendStep::serialize(Serialization & ctx) const
{
    if (usesManifest(ctx.version))
        writeManifestPayload(BROADCAST_SEND_MANIFEST, toWire(), ctx);
    else
        serializeLegacy(ctx);
}

QueryPlanStepPtr BroadcastSendStep::deserialize(Deserialization & ctx)
{
    if (usesManifest(ctx.version))
        return fromWire(readManifestPayload(BROADCAST_SEND_MANIFEST, ctx), ctx);
    return deserializeLegacy(ctx);
}

void BroadcastSendStep::serializeLegacy(Serialization & ctx) const
{
    writeStringBinary(exchange_id, ctx.out);
    writeVarUInt(num_buckets, ctx.out);
}

QueryPlanStepPtr BroadcastSendStep::deserializeLegacy(Deserialization & ctx)
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
    registerManifest<BROADCAST_SEND_MANIFEST>(registry, BroadcastSendStep::deserialize);
}

}
