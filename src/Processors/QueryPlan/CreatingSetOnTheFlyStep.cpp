#include <Processors/QueryPlan/CreatingSetOnTheFlyStep.h>
#include <Processors/Transforms/CreatingSetsOnTheFlyTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Processors/IProcessor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static ITransformingStep::Traits getTraits(bool is_filter)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = true,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = !is_filter,
        }
    };
}

CreatingSetOnTheFlyStep::CreatingSetOnTheFlyStep(const DataStream & input_stream_, const Names & column_names_, const SizeLimits & size_limits)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(false))
    , column_names(column_names_)
{
    if (input_streams.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} requires exactly one input stream, got {}", getName(), input_streams.size());

    set = std::make_shared<Set>(size_limits, false, true);

    {
        ColumnsWithTypeAndName header;
        for (const auto & name : column_names)
        {
            ColumnWithTypeAndName column = input_streams[0].header.getByName(name);
            header.emplace_back(column);
        }
        set->setHeader(header);
    }
}

void CreatingSetOnTheFlyStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    UNUSED(settings);
    size_t num_streams = pipeline.getNumStreams();

    pipeline.resize(1);
    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;
        return std::make_shared<CreatingSetsOnTheFlyTransform>(header, column_names, set);
    });
    pipeline.resize(num_streams);
}

void CreatingSetOnTheFlyStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add(getName(), true);
}

void CreatingSetOnTheFlyStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << getName();

    settings.out << '\n';
}

void CreatingSetOnTheFlyStep::updateOutputStream()
{
    if (input_streams.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} requires exactly one input stream, got {}", getName(), input_streams.size());

    output_stream = input_streams[0];
}


FilterBySetOnTheFlyStep::FilterBySetOnTheFlyStep(const DataStream & input_stream_, const Names & column_names_, SetPtr set_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(true))
    , column_names(column_names_)
    , set(set_)
{
    if (input_streams.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} requires exactly one input stream, got {}", getName(), input_streams.size());
}

void FilterBySetOnTheFlyStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    UNUSED(settings);
    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;
        return std::make_shared<FilterBySetOnTheFlyTransform>(header, column_names, set);
    });
}

void FilterBySetOnTheFlyStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add(getName(), true);
}

void FilterBySetOnTheFlyStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << getName();

    settings.out << '\n';
}

void FilterBySetOnTheFlyStep::updateOutputStream()
{
    if (input_streams.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} requires exactly one input stream, got {}", getName(), input_streams.size());

    output_stream = input_streams[0];
}


}
