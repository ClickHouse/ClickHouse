#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Common/Exception.h>

namespace DB
{

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 join_runtime_filter_exact_values_limit;
    extern const QueryPlanSerializationSettingsUInt64 join_runtime_bloom_filter_bytes;
    extern const QueryPlanSerializationSettingsUInt64 join_runtime_bloom_filter_hash_functions;
}


namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int PARAMETER_OUT_OF_BOUND;
}

/// Runtime bloom filter should be small and fast otherwise it is pointless
static constexpr UInt64 MAX_RUNTIME_BLOOM_FILTER_BYTES = 16 * 1024 * 1024;
static constexpr UInt64 MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS = 10;
static constexpr UInt64 DEFAULT_RUNTIME_BLOOM_FILTER_BYTES = 512 * 1024;
static constexpr UInt64 DEFAULT_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS = 3;


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
    UInt64 exact_values_limit_,
    UInt64 bloom_filter_bytes_,
    UInt64 bloom_filter_hash_functions_)
    : ITransformingStep(
        input_header_,
        input_header_,
        getTraits())
    , filter_column_name(std::move(filter_column_name_))
    , filter_column_type(filter_column_type_)
    , filter_name(filter_name_)
    , exact_values_limit(exact_values_limit_)
    , bloom_filter_bytes(bloom_filter_bytes_)
    , bloom_filter_hash_functions(bloom_filter_hash_functions_)
{
    if (!bloom_filter_bytes)
        bloom_filter_bytes = DEFAULT_RUNTIME_BLOOM_FILTER_BYTES;
    if (bloom_filter_bytes > MAX_RUNTIME_BLOOM_FILTER_BYTES)
        throw Exception(
            ErrorCodes::PARAMETER_OUT_OF_BOUND,
            "Specified runtime bloom filter size {} is too big, maximum: {}",
            bloom_filter_bytes, MAX_RUNTIME_BLOOM_FILTER_BYTES);

    if (!bloom_filter_hash_functions)
        bloom_filter_hash_functions = DEFAULT_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS;
    if (bloom_filter_hash_functions > MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS)
        throw Exception(
            ErrorCodes::PARAMETER_OUT_OF_BOUND,
            "Specified runtime bloom filter hash function count {} is too big, maximum: {}",
            bloom_filter_hash_functions, MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS);
}

void BuildRuntimeFilterStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType)
    {
        return std::make_shared<BuildRuntimeFilterTransform>(
            header, filter_column_name, filter_column_type, filter_name, exact_values_limit, bloom_filter_bytes, bloom_filter_hash_functions);
    });
}

void BuildRuntimeFilterStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

void BuildRuntimeFilterStep::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    settings[QueryPlanSerializationSetting::join_runtime_filter_exact_values_limit] = exact_values_limit;
    settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_bytes] = bloom_filter_bytes;
    settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_hash_functions] = bloom_filter_hash_functions;
}

void BuildRuntimeFilterStep::serialize(Serialization & ctx) const
{
    writeStringBinary(filter_column_name, ctx.out);
    encodeDataType(filter_column_type, ctx.out);
    writeStringBinary(filter_name, ctx.out);
}

QueryPlanStepPtr BuildRuntimeFilterStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "BuildRuntimeFilterStep must have one input stream");

    String filter_column_name;
    readStringBinary(filter_column_name, ctx.in);

    DataTypePtr filter_column_type = decodeDataType(ctx.in);

    String filter_name;
    readStringBinary(filter_name, ctx.in);

    const UInt64 exact_values_limit = ctx.settings[QueryPlanSerializationSetting::join_runtime_filter_exact_values_limit];
    const UInt64 bloom_filter_bytes = ctx.settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_bytes];
    const UInt64 bloom_filter_hash_functions = ctx.settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_hash_functions];

    return std::make_unique<BuildRuntimeFilterStep>(
        ctx.input_headers.front(),
        std::move(filter_column_name),
        filter_column_type,
        std::move(filter_name),
        exact_values_limit,
        bloom_filter_bytes,
        bloom_filter_hash_functions);
}

QueryPlanStepPtr BuildRuntimeFilterStep::clone() const
{
    return std::make_unique<BuildRuntimeFilterStep>(*this);
}

void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("BuildRuntimeFilter", BuildRuntimeFilterStep::deserialize);
}

}
