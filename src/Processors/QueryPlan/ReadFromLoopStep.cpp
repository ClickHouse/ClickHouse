#include <Processors/QueryPlan/ReadFromLoopStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/IStorage.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>

namespace DB
{

ReadFromLoopStep::ReadFromLoopStep(
    const Names & column_names_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    QueryProcessingStage::Enum processed_stage_,
    StoragePtr inner_storage_,
    size_t max_block_size_,
    size_t num_streams_)
    : SourceStepWithFilter(
          DataStream{.header = storage_snapshot_->getSampleBlockForColumns(column_names_)},
          column_names_,
          query_info_,
          storage_snapshot_,
          context_)
    , column_names(column_names_)
    , processed_stage(processed_stage_)
    , inner_storage(std::move(inner_storage_))
    , max_block_size(max_block_size_)
    , num_streams(num_streams_)
{
}

Pipe ReadFromLoopStep::makePipe()
{
    Pipes res_pipe;

    for (size_t i = 0; i < 10; ++i)
    {
        QueryPlan plan;
        inner_storage->read(
            plan,
            column_names,
            storage_snapshot,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams);
        auto builder = plan.buildQueryPipeline(
            QueryPlanOptimizationSettings::fromContext(context),
            BuildQueryPipelineSettings::fromContext(context));

        QueryPlanResourceHolder resources;
        auto pipe = QueryPipelineBuilder::getPipe(std::move(*builder), resources);

        res_pipe.emplace_back(std::move(pipe));
    }

    return Pipe::unitePipes(std::move(res_pipe));
}

void ReadFromLoopStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.init(makePipe());
}

}
