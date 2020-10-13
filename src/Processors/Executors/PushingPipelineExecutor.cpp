#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Sources/PushingSource.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{
PushingPipelineExecutor::PushingPipelineExecutor(QueryPipeline & pipeline_, PushingSource & source_) : pipeline(pipeline_), source(source_)
{
}

PushingPipelineExecutor::~PushingPipelineExecutor()
{
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException("PushingPipelineExecutor");
    }
}

const Block & PushingPipelineExecutor::getHeader() const
{
    return pipeline.getHeader();
}

bool PushingPipelineExecutor::push(const Block & block)
{
    if (!executor)
       executor = pipeline.execute();

    source.push(block);
    return executor->executeStep(&source.no_input_flag);
}

void PushingPipelineExecutor::cancel()
{
    /// Cancel execution if it wasn't finished.
    if (executor)
        executor->cancel();
}

}
