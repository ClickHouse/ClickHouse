#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Sinks/NativeCompressedSink.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/scatterByPartition.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

QueryPipelineBuilderPtr ShuffleSendStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    auto & pipeline = *pipelines.front();
    auto stream_header = pipeline.getSharedHeader();

    /// Exchanges carry only the main data stream; throw instead of silently dropping totals/extremes.
    if (pipeline.hasTotals() || pipeline.hasExtremes())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ShuffleSendStep does not support pipelines with totals or extremes");

    ColumnNumbers key_columns;
    key_columns.reserve(key_names.size());
    for (const auto & key_name : key_names)
        key_columns.push_back(stream_header->getPositionByName(key_name));

    /// Repartitioning creates num_streams * num_buckets connections in the pipeline.
    /// Cap the number of streams to keep that number small.
    const size_t max_scatter_streams = 16;
    if (pipeline.getNumStreams() > max_scatter_streams)
        pipeline.resize(max_scatter_streams);

    /// Repartition the data so that stream i carries exactly the rows of bucket i.
    scatterByPartition(pipeline, num_buckets, key_columns, hash_cast_types);

    const String shard_id = settings.parameter_lookup->getParameter("bucket_id").safeGet<String>();

    size_t bucket = 0;
    pipeline.setSinks([&](const SharedHeader & header, Pipe::StreamType stream_type)
    {
        chassert(stream_type == Pipe::StreamType::Main);
        String destination_bucket_id = toString(bucket);
        ++bucket;
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
    size_t size = 0;
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

    writeVarUInt(hash_cast_types.size(), ctx.out);
    for (const auto & type : hash_cast_types)
        writeStringBinary(type ? type->getName() : "", ctx.out);
}

std::unique_ptr<IQueryPlanStep> ShuffleSendStep::deserialize(Deserialization & ctx)
{
    String exchange_id;
    readStringBinary(exchange_id, ctx.in);

    Names key_names;
    deserializeNames(key_names, ctx.in);

    size_t num_buckets = 0;
    readVarUInt(num_buckets, ctx.in);

    size_t hash_cast_count = 0;
    readVarUInt(hash_cast_count, ctx.in);
    DataTypes hash_cast_types;
    hash_cast_types.reserve(hash_cast_count);
    for (size_t i = 0; i < hash_cast_count; ++i)
    {
        String type_name;
        readStringBinary(type_name, ctx.in);
        hash_cast_types.push_back(type_name.empty() ? nullptr : DataTypeFactory::instance().get(type_name));
    }

    return std::make_unique<ShuffleSendStep>(ctx.input_headers.front(), exchange_id, std::move(key_names), num_buckets, std::move(hash_cast_types));
}

void registerShuffleSendStep(QueryPlanStepRegistry & registry);
void registerShuffleSendStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ShuffleSend", ShuffleSendStep::deserialize);
}

}
