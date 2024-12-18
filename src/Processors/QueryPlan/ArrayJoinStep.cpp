#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/Transforms/ArrayJoinTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

ArrayJoinStep::ArrayJoinStep(const DataStream & input_stream_, ArrayJoinActionPtr array_join_)
    : ITransformingStep(
        input_stream_,
        ArrayJoinTransform::transformHeader(input_stream_.header, array_join_),
        getTraits())
    , array_join(std::move(array_join_))
{
}

void ArrayJoinStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(), ArrayJoinTransform::transformHeader(input_streams.front().header, array_join), getDataStreamTraits());
}

void ArrayJoinStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        return std::make_shared<ArrayJoinTransform>(header, array_join, on_totals);
    });
}

void ArrayJoinStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    bool first = true;

    settings.out << prefix << (array_join->is_left ? "LEFT " : "") << "ARRAY JOIN ";
    for (const auto & column : array_join->columns)
    {
        if (!first)
            settings.out << ", ";
        first = false;


        settings.out << column;
    }
    settings.out << '\n';
}

void ArrayJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Left", array_join->is_left);

    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : array_join->columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
}

}
