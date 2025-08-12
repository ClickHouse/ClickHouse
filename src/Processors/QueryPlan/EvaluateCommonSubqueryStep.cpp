#include <Processors/QueryPlan/EvaluateCommonSubqueryStep.h>

#include <Processors/Transforms/EvaluateCommonSubqueryTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

EvaluateCommonSubqueryStep::EvaluateCommonSubqueryStep(
    const SharedHeader & header_,
    StoragePtr storage_,
    ContextPtr context_
) : ITransformingStep(header_, header_, Traits{
        .data_stream_traits = {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true
        },
        .transform_traits = {
            .preserves_number_of_rows = true
        }
    })
    , storage(std::move(storage_))
    , context(std::move(context_))
{
    if (!storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Storage is not set for EvaluateCommonSubqueryStep");
}

void EvaluateCommonSubqueryStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &  /*settings*/)
{
    pipeline.addSimpleTransform(
        [this](const SharedHeader & in_header)
        {
            return std::make_shared<EvaluateCommonSubqueryTransform>(in_header, storage, context);
        });
}

}
