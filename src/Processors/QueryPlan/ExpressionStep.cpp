#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

ExpressionStep::ExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_)
    : ITransformingStep(
        input_stream_,
        DataStream{.header = ExpressionTransform::transformHeader(input_stream_.header, expression_)})
    , expression(std::move(expression_))
{
}

void ExpressionStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<ExpressionTransform>(header, expression, on_totals);
    });
}

}
