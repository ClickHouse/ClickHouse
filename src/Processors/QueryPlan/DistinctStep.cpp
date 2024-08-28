#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/Transforms/DistinctSortedChunkTransform.h>
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

static SortDescription getSortDescription(const SortDescription & input_sort_desc, const Names& columns)
{
    SortDescription distinct_sort_desc;
    for (const auto & sort_column_desc : input_sort_desc)
    {
        if (std::find(begin(columns), end(columns), sort_column_desc.column_name) == columns.end())
            break;
        distinct_sort_desc.emplace_back(sort_column_desc);
    }
    return distinct_sort_desc;
}

DistinctStep::DistinctStep(
    const DataStream & input_stream_,
    const SizeLimits & set_size_limits_,
    UInt64 limit_hint_,
    const Names & columns_,
    bool pre_distinct_,
    bool optimize_distinct_in_order_)
    : ITransformingStep(
            input_stream_,
            input_stream_.header,
            getTraits(pre_distinct_))
    , set_size_limits(set_size_limits_)
    , limit_hint(limit_hint_)
    , columns(columns_)
    , pre_distinct(pre_distinct_)
    , optimize_distinct_in_order(optimize_distinct_in_order_)
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

    if (optimize_distinct_in_order)
    {
        const auto & input_stream = input_streams.back();
        const SortDescription distinct_sort_desc = getSortDescription(input_stream.sort_description, columns);
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

                        return std::make_shared<DistinctSortedChunkTransform>(
                            header,
                            set_size_limits,
                            limit_hint,
                            distinct_sort_desc,
                            columns,
                            input_stream.sort_scope == DataStream::SortScope::Stream);
                    });
                return;
            }
            /// final distinct for sorted stream (sorting inside and among chunks)
            if (input_stream.sort_scope == DataStream::SortScope::Global)
            {
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

                            return std::make_shared<DistinctSortedChunkTransform>(
                                header, set_size_limits, limit_hint, distinct_sort_desc, columns, true);
                        });
                    return;
                }
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

void DistinctStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        input_streams.front().header,
        getTraits(pre_distinct).data_stream_traits);
}

void DistinctStep::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    settings.max_rows_in_distinct = set_size_limits.max_rows;
    settings.max_bytes_in_distinct = set_size_limits.max_bytes;
    settings.distinct_overflow_mode = set_size_limits.overflow_mode;
}

void DistinctStep::serialize(WriteBuffer & out) const
{
    /// Let's not serialzie limit_hint.
    /// Ideally, we can get if from a query plan optimization on the follower.

    writeVarUInt(columns.size(), out);
    for (const auto & column : columns)
        writeStringBinary(column, out);
}

std::unique_ptr<IQueryPlanStep> DistinctStep::deserialize(
    ReadBuffer & in, const DataStreams & input_streams_, QueryPlanSerializationSettings & settings, bool pre_distinct_)
{
    if (input_streams_.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "DistinctStep must have one input stream");

    size_t columns_size;
    readVarUInt(columns_size, in);
    Names column_names(columns_size);
    for (size_t i = 0; i < columns_size; ++i)
        readStringBinary(column_names[i], in);

    SizeLimits size_limits;
    size_limits.max_rows = settings.max_rows_in_distinct;
    size_limits.max_bytes = settings.max_bytes_in_distinct;
    size_limits.overflow_mode = settings.distinct_overflow_mode;

    return std::make_unique<DistinctStep>(
        input_streams_.front(), size_limits, 0, column_names, pre_distinct_, false);
}

std::unique_ptr<IQueryPlanStep> DistinctStep::deserializeNormal(
    ReadBuffer & in, const DataStreams & input_streams_, QueryPlanSerializationSettings & settings)
{
    return DistinctStep::deserialize(in, input_streams_, settings, false);
}
std::unique_ptr<IQueryPlanStep> DistinctStep::deserializePre(
    ReadBuffer & in, const DataStreams & input_streams_, QueryPlanSerializationSettings & settings)
{
    return DistinctStep::deserialize(in, input_streams_, settings, true);
}

void registerDistinctStep(QueryPlanStepRegistry & registry)
{
    /// Preliminary distinct probably can be a query plan optimization.
    /// It's easier to serialzie it using different names, so that pre-distinct can be potentially removed later.
    registry.registerStep("Distinct", DistinctStep::deserializeNormal);
    registry.registerStep("PreDistinct", DistinctStep::deserializePre);
}

}
