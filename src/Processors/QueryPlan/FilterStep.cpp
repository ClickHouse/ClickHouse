#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/QueryPipeline.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits(const ExpressionActionsPtr & expression)
{
    return ITransformingStep::DataStreamTraits{
            .preserves_distinct_columns = !expression->hasJoinOrArrayJoin() /// I suppose it actually never happens
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
    filterDistinctColumns(output_stream->header, output_stream->distinct_columns);
    filterDistinctColumns(output_stream->header, output_stream->local_distinct_columns);
}

void FilterStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<FilterTransform>(header, expression, filter_column_name, remove_filter_column, on_totals);
    });
}

}
