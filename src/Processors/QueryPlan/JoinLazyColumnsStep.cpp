#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinLazyColumnsStep::JoinLazyColumnsStep(
    const SharedHeader & left_header_,
    const SharedHeader & right_header_,
    ILazyMaterializingRowsPtr lazy_materializing_rows_,
    String index_column_name_)
    : lazy_materializing_rows(std::move(lazy_materializing_rows_))
    , index_column_name(std::move(index_column_name_))
{
    updateInputHeaders({left_header_, right_header_});
}

JoinLazyColumnsStep::~JoinLazyColumnsStep() = default;

QueryPipelineBuilderPtr JoinLazyColumnsStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinLazyColumnsStep must have two pipelines");

    /// The widest step does not count the readers the lazy read adds at run time. A single-threaded main branch runs in
    /// the calling thread; keep it there.
    const bool reserve_threads = lazy_read_expands && pipelines[0]->getNumThreads() > 1;

    /// The transform joins exactly one main and one lazy stream. The main branch is not
    /// necessarily single-stream (e.g. a limit without a sorting step above the reading).
    if (pipelines[0]->getNumStreams() > 1)
        pipelines[0]->resize(1);
    if (pipelines[1]->getNumStreams() > 1)
        pipelines[1]->resize(1);

    auto transform = std::make_shared<LazyMaterializingTransform>(
        input_headers.front(), input_headers.back(), lazy_materializing_rows, dataflow_cache_updater, index_column_name);
    transform->setPassThrough(pass_through);
    auto res = QueryPipelineBuilder::mergePipelines(std::move(pipelines[0]), std::move(pipelines[1]), transform, &processors);
    if (reserve_threads)
        res->reserveThreadsForExpansion(settings.max_threads);
    return res;
}

void JoinLazyColumnsStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(LazyMaterializingTransform::transformHeader(
        *input_headers.front(), *input_headers.back(), index_column_name));
}

void JoinLazyColumnsStep::setPassThrough(bool value)
{
    if (value && !blocksHaveEqualStructure(*input_headers.back(), *output_header))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "JoinLazyColumnsStep: pass-through requires lazy header to match output header, "
            "got lazy={} vs output={}",
            input_headers.back()->dumpStructure(), output_header->dumpStructure());
    pass_through = value;
}

void JoinLazyColumnsStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
