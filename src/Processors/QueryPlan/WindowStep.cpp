#include <Processors/QueryPlan/WindowStep.h>

#include <Processors/Transforms/WindowTransform.h>
#include <Processors/QueryPipeline.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>

namespace DB
{

static ITransformingStep::Traits getTraits(const ActionsDAGPtr & actions)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = !actions->hasArrayJoin(),
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = !actions->hasArrayJoin(),
        },
        {
            .preserves_number_of_rows = !actions->hasArrayJoin(),
        }
    };
}

static ITransformingStep::Traits getJoinTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

WindowStep::WindowStep(const DataStream & input_stream_,
        ActionsDAGPtr actions_dag_)
    : ITransformingStep(
        input_stream_,
        Transform::transformHeader(input_stream_.header,
            std::make_shared<ExpressionActions>(actions_dag_)),
        getTraits(actions_dag_))
    , actions_dag(std::move(actions_dag_))
{
    /// Some columns may be removed by expression.
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

void WindowStep::updateInputStream(DataStream input_stream, bool keep_header)
{
    Block out_header = keep_header
        ? std::move(output_stream->header)
        : Transform::transformHeader(input_stream.header,
            std::make_shared<ExpressionActions>(actions_dag));

    output_stream = createOutputStream(
            input_stream,
            std::move(out_header),
            getDataStreamTraits());

    input_streams.clear();
    input_streams.emplace_back(std::move(input_stream));
}

void WindowStep::transformPipeline(QueryPipeline & pipeline)
{
    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<Transform>(header, expression);
    });

    if (!blocksHaveEqualStructure(pipeline.getHeader(), output_stream->header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                pipeline.getHeader().getColumnsWithTypeAndName(),
                output_stream->header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);
        auto convert_actions = std::make_shared<ExpressionActions>(convert_actions_dag);

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, convert_actions);
        });
    }
}

void WindowStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    bool first = true;

    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    for (const auto & action : expression->getActions())
    {
        settings.out << prefix << (first ? "Actions: "
                                         : "         ");
        first = false;
        settings.out << action.toString() << '\n';
    }
}

}
