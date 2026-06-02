#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Sinks/NativeCompressedSink.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/Transforms/ScatterByPartitionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

QueryPipelineBuilderPtr ShuffleSendStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    /// Add calculation of hash of key columns and bucket id based on the hash
    /// Add fork processor to send data to num_buckets outputs
    auto & pipeline = *pipelines.front();
    auto stream_header = pipeline.getSharedHeader();
    {
        ColumnNumbers key_columns;
        for (const auto & key_name : key_names)
            key_columns.push_back(stream_header->getPositionByName(key_name));

        pipeline.resize(1);
        auto scatter = std::make_shared<ScatterByPartitionTransform>(stream_header, num_buckets, key_columns);
        pipeline.addTransform(scatter);
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ShuffleSendStep: expected {} buckets, but created only {}", num_buckets, bucket);

    return std::move(pipelines.front());
}

namespace
{

void serializeNames(const Names & names, WriteBuffer & out)
{
    writeVarUInt(names.size(), out);
    for (const String & name : names)
        writeStringBinary(name, out);
}

void deserializeNames(Names & names, ReadBuffer & in)
{
    size_t size;
    readVarUInt(size, in);
    names.resize(size);
    for (size_t i = 0; i < size; ++i)
        readStringBinary(names[i], in);
}

}

void ShuffleSendStep::serialize(Serialization & ctx) const
{
    writeStringBinary(exchange_id, ctx.out);
    serializeNames(key_names, ctx.out);
    writeVarUInt(num_buckets, ctx.out);
}

std::unique_ptr<IQueryPlanStep> ShuffleSendStep::deserialize(Deserialization & ctx)
{
    String exchange_id;
    readStringBinary(exchange_id, ctx.in);

    Names key_names;
    deserializeNames(key_names, ctx.in);

    size_t num_buckets;
    readVarUInt(num_buckets, ctx.in);

    return std::make_unique<ShuffleSendStep>(ctx.input_headers.front(), exchange_id, std::move(key_names), num_buckets);
}

void registerShuffleSendStep(QueryPlanStepRegistry & registry);
void registerShuffleSendStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ShuffleSend", ShuffleSendStep::deserialize);
}

}
