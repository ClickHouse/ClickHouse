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

ArrayJoinStep::ArrayJoinStep(const Header & input_header_, ArrayJoin array_join_, bool is_unaligned_, size_t max_block_size_)
    : ITransformingStep(
        input_header_,
        ArrayJoinTransform::transformHeader(input_header_, array_join_.columns),
        getTraits())
    , array_join(std::move(array_join_))
    , is_unaligned(is_unaligned_)
    , max_block_size(max_block_size_)
{
}

void ArrayJoinStep::updateOutputHeader()
{
    output_header = ArrayJoinTransform::transformHeader(input_headers.front(), array_join.columns);
}

void ArrayJoinStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto array_join_actions = std::make_shared<ArrayJoinAction>(array_join.columns, array_join.is_left, is_unaligned, max_block_size);
    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        return std::make_shared<ArrayJoinTransform>(header, array_join_actions, on_totals);
    });
}

void ArrayJoinStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    bool first = true;

    settings.out << prefix << (array_join.is_left ? "LEFT " : "") << "ARRAY JOIN ";
    for (const auto & column : array_join.columns)
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
    map.add("Left", array_join.is_left);

    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : array_join.columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
}

}
