#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/MergeRuntimeFiltersStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Transforms/MergeRuntimeFiltersTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}

MergeRuntimeFiltersStep::MergeRuntimeFiltersStep(
    String filter_name_,
    const DataTypePtr & filter_column_type_,
    const RuntimeFilterGeometry & geometry_,
    String input_exchange_id_,
    Strings source_buckets_,
    size_t fan_in_,
    std::vector<Output> outputs_)
    : filter_name(std::move(filter_name_))
    , filter_column_type(filter_column_type_)
    , geometry(geometry_)
    , input_exchange_id(std::move(input_exchange_id_))
    , source_buckets(std::move(source_buckets_))
    , fan_in(fan_in_)
    , outputs(std::move(outputs_))
{
    if (input_exchange_id.empty() || source_buckets.empty() || fan_in == 0 || outputs.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeRuntimeFiltersStep requires an input exchange, source buckets and outputs");
}

QueryPipelineBuilderPtr
MergeRuntimeFiltersStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    if (!pipelines.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeRuntimeFiltersStep expects no input pipelines");

    const String bucket_id = settings.parameter_lookup->getParameter("bucket_id").safeGet<String>();
    const size_t bucket_index = parse<size_t>(bucket_id);

    /// Task `i` consumes child buckets `[i * fan_in, (i + 1) * fan_in)`; the wiring enumerated the
    /// exchange streams with the same rule.
    const size_t children_begin = bucket_index * fan_in;
    const size_t children_end = std::min(children_begin + fan_in, source_buckets.size());
    if (children_begin >= children_end)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "MergeRuntimeFiltersStep task {} has no child buckets: {} sources with fan-in {}",
            bucket_id,
            source_buckets.size(),
            fan_in);

    auto partials_header = runtimeFilterPartialsHeader();

    Pipes pipes;
    for (size_t child = children_begin; child < children_end; ++child)
        pipes.emplace_back(
            settings.exchange_lookup->createSource(partials_header, ExchangeStreamId(input_exchange_id, source_buckets[child], bucket_id)));

    /// Destination streams, in the order the sinks are attached below.
    std::vector<ExchangeStreamId> destination_streams;
    for (const auto & output : outputs)
    {
        if (output.destination_buckets.empty())
            destination_streams.emplace_back(output.exchange_id, bucket_id, toString(bucket_index / fan_in));
        else
            for (const String & destination_bucket : output.destination_buckets)
                destination_streams.emplace_back(output.exchange_id, bucket_id, destination_bucket);
    }

    auto pipeline = std::make_unique<QueryPipelineBuilder>();
    pipeline->init(Pipe::unitePipes(std::move(pipes)));

    pipeline->addTransform(
        std::make_shared<MergeRuntimeFiltersTransform>(
            partials_header,
            children_end - children_begin,
            MergeRuntimeFiltersTransform::Mode::ForwardUnion,
            filter_name,
            /*filter_key_=*/String{},
            filter_column_type,
            geometry,
            /*filter_lookup_=*/nullptr,
            /*num_forward_destinations_=*/destination_streams.size()));

    if (destination_streams.size() > 1)
        pipeline->addTransform(std::make_shared<CopyTransform>(partials_header, destination_streams.size()));

    size_t next_sink = 0;
    pipeline->setSinks(
        [&](const SharedHeader & header, Pipe::StreamType stream_type) -> ProcessorPtr
        {
            chassert(stream_type == Pipe::StreamType::Main);
            return settings.exchange_lookup->createSink(header, destination_streams[next_sink++], /*advisory*/ true);
        });
    if (next_sink != destination_streams.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "MergeRuntimeFiltersStep: expected {} destination streams, but created {}",
            destination_streams.size(),
            next_sink);

    return pipeline;
}

void MergeRuntimeFiltersStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const
{
    geometry.serializeSettings(settings, version);
}

void MergeRuntimeFiltersStep::serialize(Serialization & ctx) const
{
    writeStringBinary(filter_name, ctx.out);
    encodeDataType(filter_column_type, ctx.out);
    writeStringBinary(input_exchange_id, ctx.out);
    writeVectorBinary(source_buckets, ctx.out);
    writeVarUInt(fan_in, ctx.out);
    writeVarUInt(outputs.size(), ctx.out);
    for (const auto & output : outputs)
    {
        writeStringBinary(output.exchange_id, ctx.out);
        writeVectorBinary(output.destination_buckets, ctx.out);
    }
}

QueryPlanStepPtr MergeRuntimeFiltersStep::deserialize(Deserialization & ctx)
{
    if (!ctx.input_headers.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "MergeRuntimeFiltersStep must have no input streams");

    String filter_name;
    readStringBinary(filter_name, ctx.in);
    DataTypePtr filter_column_type = decodeDataType(ctx.in, ctx.max_type_complexity);
    String input_exchange_id;
    readStringBinary(input_exchange_id, ctx.in);
    Strings source_buckets;
    readVectorBinary(source_buckets, ctx.in);
    size_t fan_in = 0;
    readVarUInt(fan_in, ctx.in);
    size_t num_outputs = 0;
    readVarUInt(num_outputs, ctx.in);
    std::vector<Output> outputs(num_outputs);
    size_t parent_outputs = 0;
    for (auto & output : outputs)
    {
        readStringBinary(output.exchange_id, ctx.in);
        readVectorBinary(output.destination_buckets, ctx.in);
        if (output.exchange_id.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "MergeRuntimeFiltersStep has an output without an exchange id");
        parent_outputs += output.destination_buckets.empty();
    }
    /// A task has at most one parent at the next level; two parent outputs would write the same
    /// destination stream twice.
    if (parent_outputs > 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "MergeRuntimeFiltersStep has more than one parent output");

    if (input_exchange_id.empty() || source_buckets.empty() || fan_in == 0 || outputs.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "MergeRuntimeFiltersStep is missing its exchange topology");

    auto geometry = RuntimeFilterGeometry::fromSettings(ctx.settings);
    geometry.validateTransported();

    return std::make_unique<MergeRuntimeFiltersStep>(
        std::move(filter_name),
        filter_column_type,
        geometry,
        std::move(input_exchange_id),
        std::move(source_buckets),
        fan_in,
        std::move(outputs));
}

QueryPlanStepPtr MergeRuntimeFiltersStep::clone() const
{
    return std::make_unique<MergeRuntimeFiltersStep>(*this);
}

void MergeRuntimeFiltersStep::describeActions(FormatSettings & format_settings) const
{
    std::string_view filter_id_view = filter_name;
    if (format_settings.pretty)
    {
        if (auto it = format_settings.runtime_filter_names.find(filter_name); it != format_settings.runtime_filter_names.end())
            filter_id_view = it->second.pretty_name;
    }
    format_settings.out << format_settings.detail_prefix << "Filter id: " << filter_id_view << '\n';
    format_settings.out << format_settings.detail_prefix << "Sources: " << source_buckets.size() << '\n';
}

void registerMergeRuntimeFiltersStep(QueryPlanStepRegistry & registry);
void registerMergeRuntimeFiltersStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("MergeRuntimeFilters", MergeRuntimeFiltersStep::deserialize);
}

}
