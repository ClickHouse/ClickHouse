#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/DistinctSortedStreamTransform.h>
#include <Processors/Transforms/DistinctSortedTransform.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/SortDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

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
    const Header & input_header_,
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

void DistinctStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (!pre_distinct)
        pipeline.resize(1);

    {
        if (!distinct_sort_desc.empty())
        {
            /// pre-distinct for sorted chunks
            if (pre_distinct)
            {
                pipeline.addSimpleTransform(
                    [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
                    {
                        if (stream_type != QueryPipelineBuilder::StreamType::Main)
                            return nullptr;

                        return std::make_shared<DistinctSortedStreamTransform>(
                            header,
                            set_size_limits,
                            limit_hint,
                            distinct_sort_desc,
                            columns);
                    });
                return;
            }

            /// final distinct for sorted stream (sorting inside and among chunks)
            if (pipeline.getNumStreams() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "DistinctStep with in-order expects single input");

            if (distinct_sort_desc.size() < columns.size())
            {
                if (DistinctSortedTransform::isApplicable(pipeline.getHeader(), distinct_sort_desc, columns))
                {
                    pipeline.addSimpleTransform(
                        [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
                        {
                            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                                return nullptr;

                            return std::make_shared<DistinctSortedTransform>(
                                header, distinct_sort_desc, set_size_limits, limit_hint, columns);
                        });
                    return;
                }
            }
            else
            {
                pipeline.addSimpleTransform(
                    [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
                    {
                        if (stream_type != QueryPipelineBuilder::StreamType::Main)
                            return nullptr;

                        return std::make_shared<DistinctSortedStreamTransform>(
                            header, set_size_limits, limit_hint, distinct_sort_desc, columns);
                    });
                return;
            }
        }
    }

    pipeline.addSimpleTransform(
        [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            return std::make_shared<DistinctTransform>(header, set_size_limits, limit_hint, columns);
        });
}

void DistinctStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
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

            settings.out << column;
        }
    }

    settings.out << '\n';
}

void DistinctStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
}

void DistinctStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

void DistinctStep::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    settings.max_rows_in_distinct = set_size_limits.max_rows;
    settings.max_bytes_in_distinct = set_size_limits.max_bytes;
    settings.distinct_overflow_mode = set_size_limits.overflow_mode;
}

void DistinctStep::serialize(Serialization & ctx) const
{
    /// Let's not serialize limit_hint.
    /// Ideally, we can get if from a query plan optimization on the follower.

    writeVarUInt(columns.size(), ctx.out);
    for (const auto & column : columns)
        writeStringBinary(column, ctx.out);
}

std::unique_ptr<IQueryPlanStep> DistinctStep::deserialize(Deserialization & ctx, bool pre_distinct_)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "DistinctStep must have one input stream");

    size_t columns_size;
    readVarUInt(columns_size, ctx.in);
    Names column_names(columns_size);
    for (size_t i = 0; i < columns_size; ++i)
        readStringBinary(column_names[i], ctx.in);

    SizeLimits size_limits;
    size_limits.max_rows = ctx.settings.max_rows_in_distinct;
    size_limits.max_bytes = ctx.settings.max_bytes_in_distinct;
    size_limits.overflow_mode = ctx.settings.distinct_overflow_mode;

    return std::make_unique<DistinctStep>(
        ctx.input_headers.front(), size_limits, 0, column_names, pre_distinct_);
}

std::unique_ptr<IQueryPlanStep> DistinctStep::deserializeNormal(Deserialization & ctx)
{
    return DistinctStep::deserialize(ctx, false);
}
std::unique_ptr<IQueryPlanStep> DistinctStep::deserializePre(Deserialization & ctx)
{
    return DistinctStep::deserialize(ctx, true);
}

void registerDistinctStep(QueryPlanStepRegistry & registry)
{
    /// Preliminary distinct probably can be a query plan optimization.
    /// It's easier to serialize it using different names, so that pre-distinct can be potentially removed later.
    registry.registerStep("Distinct", DistinctStep::deserializeNormal);
    registry.registerStep("PreDistinct", DistinctStep::deserializePre);
}

}
