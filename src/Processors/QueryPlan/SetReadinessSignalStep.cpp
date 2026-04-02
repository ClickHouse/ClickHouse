#include <Processors/Port.h>
#include <Processors/QueryPlan/SetReadinessSignalStep.h>
#include <Processors/Transforms/SetReadinessSignalTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

SetReadinessSignalStep::SetReadinessSignalStep(SharedHeader input_header_, FutureSetPtr future_set_)
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

void SetReadinessSignalStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const SharedHeader &)
    {
        return std::make_shared<SetReadinessSignalTransform>(future_set);
    });
}

}
