#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/InflatingExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>

namespace DB
{

static ITransformingStep::Traits getTraits(const ExpressionActionsPtr & expression)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = !expression->hasJoinOrArrayJoin(),
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = !expression->hasJoinOrArrayJoin(),
        },
        {
            .preserves_number_of_rows = !expression->hasJoinOrArrayJoin(),
        }
    };
}

ExpressionStep::ExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_)
    : ITransformingStep(
        input_stream_,
        Transform::transformHeader(input_stream_.header, expression_),
        getTraits(expression_))
    , expression(std::move(expression_))
{
    /// Some columns may be removed by expression.
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

void ExpressionStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<Transform>(header, expression, on_totals);
    });
}

static void doDescribeActions(const ExpressionActionsPtr & expression, IQueryPlanStep::FormatSettings & settings)
{
    String prefix(settings.offset, ' ');
    bool first = true;

    for (const auto & action : expression->getActions())
    {
        settings.out << prefix << (first ? "Actions: "
                                         : "         ");
        first = false;
        settings.out << action.toString() << '\n';
    }
}

void ExpressionStep::describeActions(FormatSettings & settings) const
{
    doDescribeActions(expression, settings);
}

InflatingExpressionStep::InflatingExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_)
    : ITransformingStep(
        input_stream_,
        Transform::transformHeader(input_stream_.header, expression_),
        getTraits(expression_))
    , expression(std::move(expression_))
{
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

void InflatingExpressionStep::transformPipeline(QueryPipeline & pipeline)
{
    /// In case joined subquery has totals, and we don't, add default chunk to totals.
    bool add_default_totals = false;
    if (!pipeline.hasTotals())
    {
        pipeline.addDefaultTotals();
        add_default_totals = true;
    }

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<Transform>(header, expression, on_totals, add_default_totals);
    });
}

void InflatingExpressionStep::describeActions(FormatSettings & settings) const
{
    doDescribeActions(expression, settings);
}

}
