#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/ReceiveRuntimeFilterStep.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/MergeRuntimeFiltersTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}

ReceiveRuntimeFilterStep::ReceiveRuntimeFilterStep(
    const SharedHeader & input_header_,
    String filter_name_,
    String filter_key_,
    const DataTypePtr & filter_column_type_,
    const RuntimeFilterGeometry & geometry_)
    : filter_name(std::move(filter_name_))
    , filter_key(std::move(filter_key_))
    , filter_column_type(filter_column_type_)
    , geometry(geometry_)
{
    updateInputHeaders({input_header_});
}

void ReceiveRuntimeFilterStep::setExchange(const String & exchange_id_, Strings source_buckets_)
{
    exchange_id = exchange_id_;
    source_buckets = std::move(source_buckets_);
}

QueryPipelineBuilderPtr
ReceiveRuntimeFilterStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    auto pipeline = std::move(pipelines.front());
    if (exchange_id.empty())
        return pipeline;

    auto query_context = CurrentThread::get().tryGetQueryContext();
    if (!query_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query context is not available for ReceiveRuntimeFilterStep");

    const String bucket_id = settings.parameter_lookup->getParameter("bucket_id").safeGet<String>();
    auto partials_header = runtimeFilterPartialsHeader();

    pipeline->transform(
        [&](OutputPortRawPtrs ports)
        {
            Processors sources;
            for (const String & source_bucket : source_buckets)
                sources.emplace_back(
                    settings.exchange_lookup->createSource(partials_header, ExchangeStreamId(exchange_id, source_bucket, bucket_id)));

            auto merge = std::make_shared<MergeRuntimeFiltersTransform>(
                partials_header,
                source_buckets.size(),
                MergeRuntimeFiltersTransform::Mode::RegisterUnion,
                filter_name,
                filter_key,
                filter_column_type,
                geometry,
                query_context->getRuntimeFilterLookup());

            return wireRuntimeFilterMergeBranch(ports, std::move(sources), std::move(merge));
        },
        /*check_ports=*/false);

    return pipeline;
}

void ReceiveRuntimeFilterStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

void ReceiveRuntimeFilterStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 /*version*/) const
{
    geometry.serializeSettings(settings);
}

void ReceiveRuntimeFilterStep::serialize(Serialization & ctx) const
{
    writeStringBinary(filter_name, ctx.out);
    /// Unlike `BuildRuntimeFilterStep`, the rendezvous key IS serialized: the worker-side merge must
    /// register the union under the same key the shipped `__applyFilter` const carries.
    writeStringBinary(filter_key, ctx.out);
    encodeDataType(filter_column_type, ctx.out);
    writeStringBinary(exchange_id, ctx.out);
    writeVectorBinary(source_buckets, ctx.out);
}

QueryPlanStepPtr ReceiveRuntimeFilterStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "ReceiveRuntimeFilterStep must have one input stream");

    String filter_name;
    readStringBinary(filter_name, ctx.in);
    String filter_key;
    readStringBinary(filter_key, ctx.in);
    DataTypePtr filter_column_type = decodeDataType(ctx.in, ctx.max_type_complexity);
    String exchange_id;
    readStringBinary(exchange_id, ctx.in);
    Strings source_buckets;
    readVectorBinary(source_buckets, ctx.in);
    if (!exchange_id.empty() && source_buckets.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "ReceiveRuntimeFilterStep has an exchange but no sources");

    auto geometry = RuntimeFilterGeometry::fromSettings(ctx.settings);
    geometry.validateTransported();

    auto step = std::make_unique<ReceiveRuntimeFilterStep>(
        ctx.input_headers.front(), std::move(filter_name), std::move(filter_key), filter_column_type, geometry);
    step->setExchange(exchange_id, std::move(source_buckets));
    return step;
}

QueryPlanStepPtr ReceiveRuntimeFilterStep::clone() const
{
    return std::make_unique<ReceiveRuntimeFilterStep>(*this);
}

void ReceiveRuntimeFilterStep::describeActions(FormatSettings & format_settings) const
{
    std::string_view filter_id_view = filter_name;
    if (format_settings.pretty)
    {
        if (auto it = format_settings.runtime_filter_names.find(filter_name); it != format_settings.runtime_filter_names.end())
            filter_id_view = it->second.pretty_name;
    }
    format_settings.out << format_settings.detail_prefix << "Filter id: " << filter_id_view << '\n';
}

void registerReceiveRuntimeFilterStep(QueryPlanStepRegistry & registry);
void registerReceiveRuntimeFilterStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ReceiveRuntimeFilter", ReceiveRuntimeFilterStep::deserialize);
}

}
