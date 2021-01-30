#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Processors/QueryPipeline.h>
#include <IO/Operators.h>

namespace DB
{

static bool checkColumnsAlreadyDistinct(const Names & columns, const NameSet & distinct_names)
{
    if (distinct_names.empty())
        return false;

    /// Now we need to check that distinct_names is a subset of columns.
    std::unordered_set<std::string_view> columns_set(columns.begin(), columns.end());
    for (const auto & name : distinct_names)
        if (columns_set.count(name) == 0)
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
    bool pre_distinct_)
    : ITransformingStep(
            input_stream_,
            input_stream_.header,
            getTraits(pre_distinct_, checkColumnsAlreadyDistinct(columns_, input_stream_.distinct_columns)))
    , set_size_limits(set_size_limits_)
    , limit_hint(limit_hint_)
    , columns(columns_)
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

void DistinctStep::transformPipeline(QueryPipeline & pipeline)
{
    if (checkColumnsAlreadyDistinct(columns, input_streams.front().distinct_columns))
        return;

    if (!pre_distinct)
        pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipeline::StreamType::Main)
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

}
