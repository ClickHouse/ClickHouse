#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinLazyColumnsStep::JoinLazyColumnsStep(const SharedHeader & left_header_, const SharedHeader & right_header_, LazyMaterializingRowsPtr lazy_materializing_rows_)
    : lazy_materializing_rows(std::move(lazy_materializing_rows_))
{
    updateInputHeaders({left_header_, right_header_});
}

JoinLazyColumnsStep::~JoinLazyColumnsStep() = default;

QueryPipelineBuilderPtr JoinLazyColumnsStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinLazyColumnsStep must have two pipelines");

    auto transform = std::make_shared<LazyMaterializingTransform>(input_headers.front(), input_headers.back(), lazy_materializing_rows, dataflow_cache_updater);
    return QueryPipelineBuilder::mergePipelines(std::move(pipelines[0]), std::move(pipelines[1]), transform, &processors);
}

void JoinLazyColumnsStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(LazyMaterializingTransform::transformHeader(
        *input_headers.front(), *input_headers.back()));
}

void JoinLazyColumnsStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
