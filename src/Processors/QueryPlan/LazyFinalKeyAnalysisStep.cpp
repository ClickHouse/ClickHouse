#include <Processors/Port.h>
#include <Processors/QueryPlan/LazyFinalKeyAnalysisStep.h>
#include <Processors/Transforms/LazyFinalKeyAnalysisTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

LazyFinalKeyAnalysisStep::LazyFinalKeyAnalysisStep(SharedHeader input_header_, FutureSetPtr future_set_)
    : ITransformingStep(
        input_header_,
        input_header_,
        {
            .data_stream_traits = {.returns_single_stream = true, .preserves_number_of_streams = true, .preserves_sorting = false},
            .transform_traits = {.preserves_number_of_rows = false},
        })
    , future_set(std::move(future_set_))
{
}

void LazyFinalKeyAnalysisStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const SharedHeader &)
    {
        return std::make_shared<LazyFinalKeyAnalysisTransform>(future_set);
    });
}

}
