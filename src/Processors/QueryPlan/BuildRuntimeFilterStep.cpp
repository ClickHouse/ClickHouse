#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Common/Exception.h>

namespace DB
{

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 join_runtime_filter_exact_values_limit;
    extern const QueryPlanSerializationSettingsUInt64 join_runtime_bloom_filter_bytes;
    extern const QueryPlanSerializationSettingsUInt64 join_runtime_bloom_filter_hash_functions;
    extern const QueryPlanSerializationSettingsDouble join_runtime_filter_pass_ratio_threshold_for_disabling;
    extern const QueryPlanSerializationSettingsUInt64 join_runtime_filter_blocks_to_skip_before_reenabling;
    extern const QueryPlanSerializationSettingsDouble join_runtime_bloom_filter_max_ratio_of_set_bits;
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
    UInt64 bloom_filter_hash_functions_,
    Float64 pass_ratio_threshold_for_disabling_,
    UInt64 blocks_to_skip_before_reenabling_,
    Float64 max_ratio_of_set_bits_in_bloom_filter_,
    bool allow_to_use_not_exact_filter_)
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
    , pass_ratio_threshold_for_disabling(pass_ratio_threshold_for_disabling_)
    , blocks_to_skip_before_reenabling(blocks_to_skip_before_reenabling_)
    , max_ratio_of_set_bits_in_bloom_filter(max_ratio_of_set_bits_in_bloom_filter_)
    , allow_to_use_not_exact_filter(allow_to_use_not_exact_filter_)
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
    auto streams = pipeline.getNumStreams();
    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type)-> ProcessorPtr
    {
        /// Build the filter only from the main stream
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;

        return std::make_shared<BuildRuntimeFilterTransform>(
            header,
            filter_column_name,
            filter_column_type,
            filter_name,
            /*filters_to_merge_=*/streams - 1,
            exact_values_limit,
            bloom_filter_bytes,
            bloom_filter_hash_functions,
            pass_ratio_threshold_for_disabling,
            blocks_to_skip_before_reenabling,
            max_ratio_of_set_bits_in_bloom_filter,
            allow_to_use_not_exact_filter);
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
    settings[QueryPlanSerializationSetting::join_runtime_filter_pass_ratio_threshold_for_disabling] = pass_ratio_threshold_for_disabling;
    settings[QueryPlanSerializationSetting::join_runtime_filter_blocks_to_skip_before_reenabling] = blocks_to_skip_before_reenabling;
    settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_max_ratio_of_set_bits] = max_ratio_of_set_bits_in_bloom_filter;
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

    DataTypePtr filter_column_type = decodeDataType(ctx.in);

    String filter_name;
    readStringBinary(filter_name, ctx.in);

    bool allow_to_use_not_exact_filter;
    readBinary(allow_to_use_not_exact_filter, ctx.in);

    const UInt64 exact_values_limit = ctx.settings[QueryPlanSerializationSetting::join_runtime_filter_exact_values_limit];
    const UInt64 bloom_filter_bytes = ctx.settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_bytes];
    const UInt64 bloom_filter_hash_functions = ctx.settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_hash_functions];
    const Float64 pass_ratio_threshold_for_disabling = ctx.settings[QueryPlanSerializationSetting::join_runtime_filter_pass_ratio_threshold_for_disabling];
    const Float64 blocks_to_skip_before_reenabling = static_cast<Float64>(ctx.settings[QueryPlanSerializationSetting::join_runtime_filter_blocks_to_skip_before_reenabling]);
    const Float64 max_ratio_of_set_bits_in_bloom_filter = ctx.settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_max_ratio_of_set_bits];

    return std::make_unique<BuildRuntimeFilterStep>(
        ctx.input_headers.front(),
        std::move(filter_column_name),
        filter_column_type,
        std::move(filter_name),
        exact_values_limit,
        bloom_filter_bytes,
        bloom_filter_hash_functions,
        pass_ratio_threshold_for_disabling,
        blocks_to_skip_before_reenabling,
        max_ratio_of_set_bits_in_bloom_filter,
        allow_to_use_not_exact_filter);
}

QueryPlanStepPtr BuildRuntimeFilterStep::clone() const
{
    return std::make_unique<BuildRuntimeFilterStep>(*this);
}

void BuildRuntimeFilterStep::describeActions(FormatSettings & format_settings) const
{
    std::string prefix(format_settings.offset, format_settings.indent_char);
    format_settings.out
        << prefix << "Filter id: " << filter_name << '\n'
        << prefix << "Allow not exact filter: " << allow_to_use_not_exact_filter << '\n';

}

void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("BuildRuntimeFilter", BuildRuntimeFilterStep::deserialize);
}

}
