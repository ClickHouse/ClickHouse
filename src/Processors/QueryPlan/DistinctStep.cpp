#include <Core/SortDescription.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/DistinctSortedStreamTransform.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/scatterByPartition.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsOverflowMode distinct_overflow_mode;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_in_distinct;
    extern const QueryPlanSerializationSettingsUInt64 max_rows_in_distinct;
}

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

bool preliminaryDistinctIsUseful(size_t max_threads)
{
    return max_threads > 1;
}

static ITransformingStep::Traits getTraits(bool pre_distinct)
{
    const bool preserves_number_of_streams = pre_distinct;
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
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
    bool pre_distinct_)
    : ITransformingStep(
            input_header_,
            input_header_,
            getTraits(pre_distinct_))
    , set_size_limits(set_size_limits_)
    , limit_hint(limit_hint_)
    , columns(columns_)
    , pre_distinct(pre_distinct_)
{
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

bool DistinctStep::tryScatterStreams(QueryPipelineBuilder & pipeline) const
{
    /// Each input chunk is split across all partitions. Bound both dimensions of the scatter mesh
    /// to limit hashing, copying, and scheduling overhead at high thread counts.
    static constexpr size_t max_partitions = 16;
    static constexpr size_t max_scatter_streams = 16;

    const size_t num_partitions = std::min(pipeline.getNumThreads(), max_partitions);
    if (pipeline.getNumStreams() <= 1 || num_partitions <= 1)
        return false;

    const auto key_column_positions = DistinctTransform::getNonConstantKeyColumnPositions(*pipeline.getSharedHeader(), columns);
    if (key_column_positions.empty())
        return false;

    if (pipeline.getNumStreams() > max_scatter_streams)
        pipeline.resize(max_scatter_streams);
    scatterByPartition(pipeline, num_partitions, key_column_positions);
    return true;
}

void DistinctStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    /// Final deduplication can keep disjoint streams separate unless a consumer requires their original
    /// order. Preliminary deduplication always processes each stream independently.
    if (preserve_input_order && pipeline.getNumStreams() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Order-preserving DISTINCT requires a single input stream");

    bool scattered = false;
    if (!pre_distinct && !skip_stream_merging)
    {
        /// Hash partitioning makes the streams disjoint, but changes their order. Sorted deduplication
        /// needs equal prefix values to remain contiguous so it can deduplicate one range at a time.
        scattered = parallel_distinct && distinct_sort_desc.empty() && tryScatterStreams(pipeline);
        if (!scattered)
            pipeline.resize(1);
    }

    /// Size limits apply to the combined set across all hash partitions. Each partition reports its new
    /// keys and retained set bytes to one limit processor, so local size checks are disabled in this case.
    const bool global_limits = scattered && set_size_limits.hasLimits();
    const SizeLimits local_limits = global_limits ? SizeLimits{} : set_size_limits;

    pipeline.addSimpleTransform(
        [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            /// When the stream is sorted by a prefix of the distinct columns, deduplicate by ranges of
            /// equal prefix values, hashing only the remaining columns within each range. If no columns
            /// remain, keep one row per range without hashing.
            if (!distinct_sort_desc.empty())
                return std::make_shared<DistinctSortedStreamTransform>(header, set_size_limits, limit_hint, distinct_sort_desc, columns);

            /// The preliminary deduplication is best-effort (a deduplicating consumer follows), so on
            /// mostly-unique input the transform may abandon it and free its hash table. A limit hint
            /// forbids this: an abandoned transform cannot count distinct rows to stop its input early.
            const bool allow_abandoning = pre_distinct && settings.allow_preliminary_distinct_abandoning && limit_hint == 0;
            return std::make_shared<DistinctTransform>(
                header, local_limits, limit_hint, columns, allow_abandoning, /*skip_null_keys=*/false, /*report_set_size=*/global_limits);
        });

    /// The scattered outputs are already disjoint, so a later merge needs no further deduplication.
    /// Global limit accounting keeps their stream assignments intact for downstream steps to reuse.
    if (global_limits)
        pipeline.addTransform(std::make_shared<DistinctLimitTransform>(pipeline.getSharedHeader(), set_size_limits, pipeline.getNumStreams()));
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
}

void DistinctStep::serialize(Serialization & ctx) const
{
    /// Limit hints and ordering requirements are derived again during plan optimization.

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

    return std::make_unique<DistinctStep>(ctx.input_headers.front(), size_limits, 0, column_names, pre_distinct_);
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
