#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/DistinctSortedStreamTransform.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/SortDescription.h>

namespace DB
{

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsOverflowMode distinct_overflow_mode;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_in_distinct;
    extern const QueryPlanSerializationSettingsUInt64 max_rows_in_distinct;
    extern const QueryPlanSerializationSettingsUInt64 distinct_set_limit_for_enabling_bloom_filter;
    extern const QueryPlanSerializationSettingsUInt64 distinct_bloom_filter_bytes;
    extern const QueryPlanSerializationSettingsDouble distinct_pass_ratio_threshold_for_disabling_bloom_filter;
    extern const QueryPlanSerializationSettingsDouble distinct_bloom_filter_max_ratio_of_set_bits;
}

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int PARAMETER_OUT_OF_BOUND;
}

/// Distinct bloom filter should be small and fast otherwise it is pointless
static constexpr UInt64 MAX_DISTINCT_BLOOM_FILTER_BYTES = 16 * 1024 * 1024;
static constexpr UInt64 DEFAULT_DISTINCT_BLOOM_FILTER_BYTES = 512 * 1024;

static ITransformingStep::Traits getTraits(bool pre_distinct)
{
    const bool preserves_number_of_streams = pre_distinct;
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = !pre_distinct,
            .preserves_number_of_streams = preserves_number_of_streams,
            .preserves_sorting = preserves_number_of_streams,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

DistinctStep::DistinctStep(
    const SharedHeader & input_header_,
    const SizeLimits & set_size_limits_,
    UInt64 limit_hint_,
    const Names & columns_,
    bool pre_distinct_,
    UInt64 set_limit_for_enabling_bloom_filter_,
    UInt64 bloom_filter_bytes_,
    Float64 pass_ratio_threshold_for_disabling_bloom_filter_,
    Float64 max_ratio_of_set_bits_in_bloom_filter_)
    : ITransformingStep(
            input_header_,
            input_header_,
            getTraits(pre_distinct_))
    , set_size_limits(set_size_limits_)
    , limit_hint(limit_hint_)
    , columns(columns_)
    , pre_distinct(pre_distinct_)
    , set_limit_for_enabling_bloom_filter(set_limit_for_enabling_bloom_filter_)
    , bloom_filter_bytes(bloom_filter_bytes_)
    , pass_ratio_threshold_for_disabling_bloom_filter(pass_ratio_threshold_for_disabling_bloom_filter_)
    , max_ratio_of_set_bits_in_bloom_filter(max_ratio_of_set_bits_in_bloom_filter_)
{
    if (!bloom_filter_bytes)
        bloom_filter_bytes = DEFAULT_DISTINCT_BLOOM_FILTER_BYTES;
    if (bloom_filter_bytes > MAX_DISTINCT_BLOOM_FILTER_BYTES)
        throw Exception(
            ErrorCodes::PARAMETER_OUT_OF_BOUND,
            "Specified distinct bloom filter size {} is too big, maximum: {}",
            bloom_filter_bytes, MAX_DISTINCT_BLOOM_FILTER_BYTES);
}

void DistinctStep::updateLimitHint(UInt64 hint)
{
    if (hint && limit_hint)
        /// Both limits are set - take the min
        limit_hint = std::min(hint, limit_hint);
    else
        /// Some limit is not set - take the other one
        limit_hint = std::max(hint, limit_hint);
}

void DistinctStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// The final distinct deduplicates across the whole input, so it needs all data in a single
    /// stream; the pre-distinct only reduces the data, deduplicating each stream independently.
    /// However, when the input streams carry disjoint sets of the DISTINCT key values, each stream
    /// can be deduplicated independently, so we keep the streams and skip merging them into one.
    if (!pre_distinct && !skip_stream_merging)
        pipeline.resize(1);

    size_t threads = pipeline.getNumThreads();

    pipeline.addSimpleTransform(
        [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            /// When the stream is sorted by a prefix of the distinct columns, deduplicate by
            /// ranges of equal prefix values, hashing only the remaining columns within a range
            /// (and with no remaining columns, keeping one row per range without hashing at all).
            if (!distinct_sort_desc.empty())
                return std::make_shared<DistinctSortedStreamTransform>(header, set_size_limits, limit_hint, distinct_sort_desc, columns);

            return std::make_shared<DistinctTransform>(
                header,
                set_size_limits,
                limit_hint,
                columns,
                pre_distinct,
                set_limit_for_enabling_bloom_filter,
                bloom_filter_bytes,
                pass_ratio_threshold_for_disabling_bloom_filter,
                max_ratio_of_set_bits_in_bloom_filter,
                threads
            );
        });
}

void DistinctStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix << "Columns: ";

    if (columns.empty())
        settings.out << "none";
    else
    {
        bool first = true;
        for (const auto & column : columns)
        {
            if (!first)
                settings.out << ", ";
            first = false;

            settings.out << (settings.pretty ? QueryPlanFormat::formatColumnPretty(column, settings.pretty_names) : column);
        }
    }

    settings.out << '\n';

    if (skip_stream_merging)
        settings.out << prefix << "Skip stream merging: 1\n";
}

void DistinctStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
    if (skip_stream_merging)
        map.add("Skip stream merging", true);
}

void DistinctStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

void DistinctStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 /*version*/) const
{
    settings[QueryPlanSerializationSetting::max_rows_in_distinct] = set_size_limits.max_rows;
    settings[QueryPlanSerializationSetting::max_bytes_in_distinct] = set_size_limits.max_bytes;
    settings[QueryPlanSerializationSetting::distinct_overflow_mode] = set_size_limits.overflow_mode;

    settings[QueryPlanSerializationSetting::distinct_set_limit_for_enabling_bloom_filter] = set_limit_for_enabling_bloom_filter;
    settings[QueryPlanSerializationSetting::distinct_bloom_filter_bytes] = bloom_filter_bytes;
    settings[QueryPlanSerializationSetting::distinct_pass_ratio_threshold_for_disabling_bloom_filter] = pass_ratio_threshold_for_disabling_bloom_filter;
    settings[QueryPlanSerializationSetting::distinct_bloom_filter_max_ratio_of_set_bits] = max_ratio_of_set_bits_in_bloom_filter;
}

void DistinctStep::serialize(Serialization & ctx) const
{
    /// Let's not serialize limit_hint.
    /// Ideally, we can get if from a query plan optimization on the follower.

    writeVarUInt(columns.size(), ctx.out);
    for (const auto & column : columns)
        writeStringBinary(column, ctx.out);
}

QueryPlanStepPtr DistinctStep::deserialize(Deserialization & ctx, bool pre_distinct_)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "DistinctStep must have one input stream");

    size_t columns_size = 0;
    readVarUInt(columns_size, ctx.in);
    Names column_names(columns_size);
    for (size_t i = 0; i < columns_size; ++i)
        readStringBinary(column_names[i], ctx.in);

    SizeLimits size_limits;
    size_limits.max_rows = ctx.settings[QueryPlanSerializationSetting::max_rows_in_distinct];
    size_limits.max_bytes = ctx.settings[QueryPlanSerializationSetting::max_bytes_in_distinct];
    size_limits.overflow_mode = ctx.settings[QueryPlanSerializationSetting::distinct_overflow_mode];

    const UInt64 set_limit_for_enabling_bloom_filter = ctx.settings[QueryPlanSerializationSetting::distinct_set_limit_for_enabling_bloom_filter];
    const UInt64 bloom_filter_bytes = ctx.settings[QueryPlanSerializationSetting::distinct_bloom_filter_bytes];
    const Float64 pass_ratio_threshold_for_disabling = ctx.settings[QueryPlanSerializationSetting::distinct_pass_ratio_threshold_for_disabling_bloom_filter];
    const Float64 max_ratio_of_set_bits_in_bloom_filter = ctx.settings[QueryPlanSerializationSetting::distinct_bloom_filter_max_ratio_of_set_bits];

    return std::make_unique<DistinctStep>(
        ctx.input_headers.front(),
        size_limits,
        0,
        column_names,
        pre_distinct_,
        set_limit_for_enabling_bloom_filter,
        bloom_filter_bytes,
        pass_ratio_threshold_for_disabling,
        max_ratio_of_set_bits_in_bloom_filter
    );
}

QueryPlanStepPtr DistinctStep::deserializeNormal(Deserialization & ctx)
{
    return DistinctStep::deserialize(ctx, false);
}
QueryPlanStepPtr DistinctStep::deserializePre(Deserialization & ctx)
{
    return DistinctStep::deserialize(ctx, true);
}

QueryPlanStepPtr DistinctStep::clone() const
{
    return std::make_unique<DistinctStep>(*this);
}

void registerDistinctStep(QueryPlanStepRegistry & registry);
void registerDistinctStep(QueryPlanStepRegistry & registry)
{
    /// Preliminary distinct probably can be a query plan optimization.
    /// It's easier to serialize it using different names, so that pre-distinct can be potentially removed later.
    registry.registerStep("Distinct", DistinctStep::deserializeNormal);
    registry.registerStep("PreDistinct", DistinctStep::deserializePre);
}

}
