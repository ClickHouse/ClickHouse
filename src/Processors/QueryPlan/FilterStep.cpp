#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>

namespace DB
{

static ITransformingStep::Traits getTraits(const ActionsDAGPtr & expression)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = !expression->hasArrayJoin(), /// I suppose it actually never happens
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FilterStep::FilterStep(
    const DataStream & input_stream_,
    ActionsDAGPtr actions_dag_,
    String filter_column_name_,
    bool remove_filter_column_)
    : ITransformingStep(
        input_stream_,
        FilterTransform::transformHeader(input_stream_.header, std::make_shared<ExpressionActions>(actions_dag_), filter_column_name_, remove_filter_column_),
        getTraits(actions_dag_))
    , actions_dag(std::move(actions_dag_))
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
{
    /// TODO: it would be easier to remove all expressions from filter step. It should only filter by column name.
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

void FilterStep::updateInputStream(DataStream input_stream, bool keep_header)
{
    Block out_header = std::move(output_stream->header);
    if (keep_header)
        out_header = FilterTransform::transformHeader(input_stream.header, std::make_shared<ExpressionActions>(actions_dag), filter_column_name, remove_filter_column);

    output_stream = createOutputStream(
            input_stream,
            std::move(out_header),
            getDataStreamTraits());

    input_streams.clear();
    input_streams.emplace_back(std::move(input_stream));
}

void FilterStep::transformPipeline(QueryPipeline & pipeline)
{
    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<FilterTransform>(header, expression, filter_column_name, remove_filter_column, on_totals);
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

void FilterStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Filter column: " << filter_column_name << '\n';

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
