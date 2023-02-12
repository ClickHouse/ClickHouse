#include <Processors/Executors/ManualPipelineExecutor.h>

#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/ReadProgressCallback.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

QueryPipeline & validatePipeline(QueryPipeline & query_pipeline)
{
    if (!query_pipeline.completed())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for ManualPipelineExecutor must be completed");
    return query_pipeline;
}

}

ManualPipelineExecutor::ManualPipelineExecutor(QueryPipeline & query_pipeline)
    : pipeline{&validatePipeline(query_pipeline)}
    , executor(pipeline->processors, pipeline->process_list_element)
{
    executor.setReadProgressCallback(pipeline->getReadProgressCallback());
}

ManualPipelineExecutor::~ManualPipelineExecutor()
{
    try
    {
        executor.cancel();
    }
    catch (...)
    {
        tryLogCurrentException("ManualPipelineExecutor");
    }
}

bool ManualPipelineExecutor::executeStep()
{
    return executor.executeStep();
}

bool ManualPipelineExecutor::executeStep(std::atomic_bool & yield_flag)
{
    return executor.executeStep(&yield_flag);
}

}
