#include <string_view>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int PARAMETER_OUT_OF_BOUND;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

BuildRuntimeFilterStep::BuildRuntimeFilterStep(
    const SharedHeader & input_header_,
    String filter_column_name_,
    const DataTypePtr & filter_column_type_,
    String filter_name_,
    String filter_key_,
    RuntimeFilterGeometry geometry_,
    bool allow_to_use_not_exact_filter_,
    bool track_key_range_,
    std::optional<UInt64> distinct_keys_hint_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , filter_column_name(std::move(filter_column_name_))
    , filter_column_type(filter_column_type_)
    , filter_name(filter_name_)
    , filter_key(std::move(filter_key_))
    , geometry(geometry_)
    , allow_to_use_not_exact_filter(allow_to_use_not_exact_filter_)
    , track_key_range(track_key_range_)
    , distinct_keys_hint(distinct_keys_hint_)
{
    if (!geometry.bloom_filter_bytes)
        geometry.bloom_filter_bytes = DEFAULT_RUNTIME_BLOOM_FILTER_BYTES;
    if (geometry.bloom_filter_bytes > MAX_RUNTIME_BLOOM_FILTER_BYTES)
        throw Exception(
            ErrorCodes::PARAMETER_OUT_OF_BOUND,
            "Specified runtime bloom filter size {} is too big, maximum: {}",
            geometry.bloom_filter_bytes,
            MAX_RUNTIME_BLOOM_FILTER_BYTES);

    if (!geometry.bloom_filter_hash_functions)
        geometry.bloom_filter_hash_functions = DEFAULT_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS;
    if (geometry.bloom_filter_hash_functions > MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS)
        throw Exception(
            ErrorCodes::PARAMETER_OUT_OF_BOUND,
            "Specified runtime bloom filter hash function count {} is too big, maximum: {}",
            geometry.bloom_filter_hash_functions,
            MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS);

    /// The exact phase is byte-bounded by the bloom size unless the plan raised it explicitly
    /// (runtime-filter transport does, from cardinality estimates).
    if (!geometry.exact_bytes_limit)
        geometry.exact_bytes_limit = geometry.bloom_filter_bytes;
}

void BuildRuntimeFilterStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// A step with no rendezvous key (e.g. a deserialized/placeholder plan — the random key is never
    /// serialized) can never register a filter that any `__applyFilter` looks up. Skip the build
    /// transform entirely so the step is a true passthrough: no key casting/insertion, no wasted
    /// build work for a filter that would never be applied.
    if (filter_key.empty())
        return;

    auto streams = pipeline.getNumStreams();
    auto query_context = CurrentThread::get().tryGetQueryContext();
    pipeline.addSimpleTransform([&, query_context](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type)-> ProcessorPtr
    {
        /// Build the filter only from the main stream
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;

        return std::make_shared<BuildRuntimeFilterTransform>(
            header,
            filter_column_name,
            filter_column_type,
            filter_name,
            filter_key,
            /*filters_to_merge_=*/streams - 1,
            geometry,
            allow_to_use_not_exact_filter,
            track_key_range,
            distinct_keys_hint,
            query_context);
    });
}

void BuildRuntimeFilterStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

void BuildRuntimeFilterStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 /*version*/) const
{
    geometry.serializeSettings(settings);
}

void BuildRuntimeFilterStep::serialize(Serialization & ctx) const
{
    writeStringBinary(filter_column_name, ctx.out);
    encodeDataType(filter_column_type, ctx.out);
    writeStringBinary(filter_name, ctx.out);
    writeBinary(allow_to_use_not_exact_filter, ctx.out);
}

QueryPlanStepPtr BuildRuntimeFilterStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "BuildRuntimeFilterStep must have one input stream");

    String filter_column_name;
    readStringBinary(filter_column_name, ctx.in);

    DataTypePtr filter_column_type = decodeDataType(ctx.in, ctx.max_type_complexity);

    String filter_name;
    readStringBinary(filter_name, ctx.in);

    bool allow_to_use_not_exact_filter = false;
    readBinary(allow_to_use_not_exact_filter, ctx.in);

    /// A deserialized step carries no random lookup key (it is never serialized); runtime filters are
    /// re-derived per plan build. If such a step is ever executed, `finish()` no-ops on the empty key.
    return std::make_unique<BuildRuntimeFilterStep>(
        ctx.input_headers.front(),
        std::move(filter_column_name),
        filter_column_type,
        std::move(filter_name),
        /*filter_key_=*/String{},
        RuntimeFilterGeometry::fromSettings(ctx.settings),
        allow_to_use_not_exact_filter,
        /*track_key_range_=*/false); /// deserialized step is inert (no rendezvous key), so it never builds
}

QueryPlanStepPtr BuildRuntimeFilterStep::clone() const
{
    return std::make_unique<BuildRuntimeFilterStep>(*this);
}

void BuildRuntimeFilterStep::describeActions(FormatSettings & format_settings) const
{
    const std::string & prefix = format_settings.detail_prefix;

    std::string_view filter_id_view = filter_name;
    if (format_settings.pretty)
    {
        if (auto it = format_settings.runtime_filter_names.find(filter_name); it != format_settings.runtime_filter_names.end())
            filter_id_view = it->second.pretty_name;
    }

    format_settings.out << prefix << "Filter id: " << filter_id_view << '\n';

    if (format_settings.pretty)
    {
        if (auto it = format_settings.runtime_filter_names.find(filter_name); it != format_settings.runtime_filter_names.end())
        {
            if (!it->second.build_table_name.empty())
                format_settings.out << prefix << "Source table: " << it->second.build_table_name << '\n';
        }
    }
    else
    {
        format_settings.out << prefix << "Allow not exact filter: " << allow_to_use_not_exact_filter << '\n';
    }
}

void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry);
void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("BuildRuntimeFilter", BuildRuntimeFilterStep::deserialize);
}

}
