#include <DataTypes/DataTypesNumber.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/LazyFinalKeyAnalysisStep.h>
#include <Processors/QueryPlan/LazyReadReplacingFinalStep.h>
#include <Processors/Sources/LazyReadReplacingFinalSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

LazyReadReplacingFinalStep::LazyReadReplacingFinalStep(
    StorageMetadataPtr metadata_snapshot_,
    const MergeTreeData & data_,
    ContextPtr query_context_,
    LazyFinalSharedStatePtr shared_state_,
    LazyFinalKeyAnalysisStep * analysis_step_)
    : ISourceStep(std::make_shared<const Block>(Block({ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "__global_row_index"}})))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , data(data_)
    , query_context(std::move(query_context_))
    , shared_state(std::move(shared_state_))
    , analysis_step(analysis_step_)
{
}

void LazyReadReplacingFinalStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto source = std::make_shared<LazyReadReplacingFinalSource>(
        metadata_snapshot,
        data,
        query_context,
        shared_state);

    pipeline.init(Pipe(std::move(source)));
}

QueryPlanRawPtrs LazyReadReplacingFinalStep::getChildPlans()
{
    if (!explain_plan)
    {
        auto reading = analysis_step->buildReadingStep();
        explain_plan.emplace(LazyReadReplacingFinalSource::buildPlanFromReadingStep(
            std::move(reading), metadata_snapshot, data, query_context));
    }

    return {&*explain_plan};
}

}
