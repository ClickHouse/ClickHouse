#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/Transforms/FillingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false, /// TODO: it seem to actually be true. Check it later.
            .returns_single_stream = true,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FillingStep::FillingStep(const DataStream & input_stream_, SortDescription sort_description_)
    : ITransformingStep(input_stream_, FillingTransform::transformHeader(input_stream_.header, sort_description_), getTraits())
    , sort_description(std::move(sort_description_))
{
    if (!input_stream_.has_single_port)
        throw Exception("FillingStep expects single input", ErrorCodes::LOGICAL_ERROR);
}

void FillingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        return std::make_shared<FillingTransform>(header, sort_description, on_totals);
    });
}

void FillingStep::describeActions(FormatSettings & settings) const
{
    settings.out << String(settings.offset, ' ');
    dumpSortDescription(sort_description, input_streams.front().header, settings.out);
    settings.out << '\n';
}

void FillingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Sort Description", explainSortDescription(sort_description, input_streams.front().header));
}

}
