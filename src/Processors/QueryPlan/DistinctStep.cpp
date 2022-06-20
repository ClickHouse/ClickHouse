#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/Transforms/DistinctPrimaryKeyTransform.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static bool checkColumnsAlreadyDistinct(const Names & columns, const NameSet & distinct_names)
{
    if (distinct_names.empty())
        return false;

    /// Now we need to check that distinct_names is a subset of columns.
    std::unordered_set<std::string_view> columns_set(columns.begin(), columns.end());
    for (const auto & name : distinct_names)
        if (!columns_set.contains(name))
            return false;

    return true;
}

static ITransformingStep::Traits getTraits(bool pre_distinct, bool already_distinct_columns)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = already_distinct_columns, /// Will be calculated separately otherwise
            .returns_single_stream = !pre_distinct && !already_distinct_columns,
            .preserves_number_of_streams = pre_distinct || already_distinct_columns,
            .preserves_sorting = true, /// Sorting is preserved indeed because of implementation.
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}


DistinctStep::DistinctStep(
    const DataStream & input_stream_,
    const SizeLimits & set_size_limits_,
    UInt64 limit_hint_,
    const Names & columns_,
    bool pre_distinct_,
    const InputOrderInfoPtr & distinct_info_)
    : ITransformingStep(
            input_stream_,
            input_stream_.header,
            getTraits(pre_distinct_, checkColumnsAlreadyDistinct(columns_, input_stream_.distinct_columns)))
    , set_size_limits(set_size_limits_)
    , limit_hint(limit_hint_)
    , columns(columns_)
    , distinct_info(distinct_info_)
    , pre_distinct(pre_distinct_)
{
    if (!output_stream->distinct_columns.empty() /// Columns already distinct, do nothing
        && (!pre_distinct /// Main distinct
            || input_stream_.has_single_port)) /// pre_distinct for single port works as usual one
    {
        /// Build distinct set.
        for (const auto & name : columns)
            output_stream->distinct_columns.insert(name);
    }
}

void DistinctStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (checkColumnsAlreadyDistinct(columns, input_streams.front().distinct_columns))
        return;

    if (!pre_distinct)
        pipeline.resize(1);

    if (pre_distinct && distinct_info)
    {
        pipeline.addSimpleTransform(
            [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<DistinctPrimaryKeyTransform>(
                    header, set_size_limits, limit_hint, distinct_info->order_key_prefix_descr, columns);
            });
    }
    else
    {
        pipeline.addSimpleTransform(
            [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<DistinctTransform>(header, set_size_limits, limit_hint, columns);
            });
    }
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

}
