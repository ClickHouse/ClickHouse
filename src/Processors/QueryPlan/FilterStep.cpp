#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/QueryPipeline.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>

namespace DB
{

static ITransformingStep::Traits getTraits(const ExpressionActionsPtr & expression)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = !expression->hasJoinOrArrayJoin(), /// I suppose it actually never happens
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
    ExpressionActionsPtr expression_,
    String filter_column_name_,
    bool remove_filter_column_)
    : ITransformingStep(
        input_stream_,
        FilterTransform::transformHeader(input_stream_.header, expression_, filter_column_name_, remove_filter_column_),
        getTraits(expression_))
    , expression(std::move(expression_))
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
{
    /// TODO: it would be easier to remove all expressions from filter step. It should only filter by column name.
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

void FilterStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<FilterTransform>(header, expression, filter_column_name, remove_filter_column, on_totals);
    });
}

void FilterStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Filter column: " << filter_column_name << '\n';

    bool first = true;
    for (const auto & action : expression->getActions())
    {
        settings.out << prefix << (first ? "Actions: "
                                         : "         ");
        first = false;
        settings.out << action.toString() << '\n';
    }
}

}
