#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/InflatingExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits(const ExpressionActionsPtr & expression)
{
    return ITransformingStep::DataStreamTraits{
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

ExpressionStep::ExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_, bool default_totals_)
    : ITransformingStep(
        input_stream_,
        ExpressionTransform::transformHeader(input_stream_.header, expression_),
        getTraits(expression_))
    , expression(std::move(expression_))
    , default_totals(default_totals_)
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
        return std::make_shared<ExpressionTransform>(header, expression, on_totals, default_totals);
    });
}

InflatingExpressionStep::InflatingExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_, bool default_totals_)
    : ITransformingStep(
        input_stream_,
        DataStream{.header = ExpressionTransform::transformHeader(input_stream_.header, expression_)})
    , expression(std::move(expression_))
    , default_totals(default_totals_)
{
}

void InflatingExpressionStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<InflatingExpressionTransform>(header, expression, on_totals, default_totals);
    });
}

}
