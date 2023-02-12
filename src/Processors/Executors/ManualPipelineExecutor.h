#pragma once

#include <Processors/Executors/PipelineExecutor.h>

#include <atomic>

namespace DB
{

class QueryPipeline;

/// Simple executor for step by step execution of completed QueryPipeline
class ManualPipelineExecutor
{
public:
    explicit ManualPipelineExecutor(QueryPipeline & query_pipeline);
    ~ManualPipelineExecutor();

    bool executeStep();
    bool executeStep(std::atomic_bool & yield_flag);

private:
    QueryPipeline * pipeline;
    PipelineExecutor executor;
};

}
