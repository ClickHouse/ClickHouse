#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

CompletedPipelineExecutor::CompletedPipelineExecutor(QueryPipeline & pipeline_) : pipeline(pipeline_) {}

void CompletedPipelineExecutor::execute()
{
    PipelineExecutor executor(pipeline.processors);
    executor.execute(pipeline.getNumThreads());
}

}
