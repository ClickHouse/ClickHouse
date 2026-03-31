#include <Processors/Port.h>
#include <Processors/QueryPlan/LazyFinalKeyAnalysisStep.h>
#include <Processors/Transforms/LazyFinalKeyAnalysisTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

LazyFinalKeyAnalysisStep::LazyFinalKeyAnalysisStep(
    SharedHeader input_header_,
    FutureSetPtr future_set_,
    ContextPtr query_context_,
    StorageMetadataPtr metadata_snapshot_,
    RangesInDataParts ranges_)
    : ITransformingStep(
        input_header_,
        input_header_,
        {
            .data_stream_traits = {.returns_single_stream = true, .preserves_number_of_streams = true, .preserves_sorting = false},
            .transform_traits = {.preserves_number_of_rows = false},
        })
    , future_set(std::move(future_set_))
    , query_context(std::move(query_context_))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , ranges(std::move(ranges_))
{
}

void LazyFinalKeyAnalysisStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const SharedHeader &)
    {
        return std::make_shared<LazyFinalKeyAnalysisTransform>(
            future_set,
            query_context,
            metadata_snapshot,
            std::move(ranges));
    });
}

}
