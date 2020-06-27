#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/InflatingExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits(const ExpressionActionsPtr & expression)
{
    return ITransformingStep::DataStreamTraits
    {
            .preserves_distinct_columns = !expression->hasJoinOrArrayJoin()
    };
}

static void filterDistinctColumns(const Block & res_header, NameSet & distinct_columns)
{
    if (distinct_columns.empty())
        return;

    NameSet new_distinct_columns;
    for (const auto & column : res_header)
        if (distinct_columns.count(column.name))
            new_distinct_columns.insert(column.name);

    distinct_columns.swap(new_distinct_columns);
}

ExpressionStep::ExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_)
    : ITransformingStep(
        input_stream_,
        Transform::transformHeader(input_stream_.header, expression_),
        getTraits(expression_))
    , expression(std::move(expression_))
{
    /// Some columns may be removed by expression.
    /// TODO: also check aliases, functions and some types of join
    filterDistinctColumns(output_stream->header, output_stream->distinct_columns);
    filterDistinctColumns(output_stream->header, output_stream->local_distinct_columns);
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
    filterDistinctColumns(output_stream->header, output_stream->distinct_columns);
    filterDistinctColumns(output_stream->header, output_stream->local_distinct_columns);
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
