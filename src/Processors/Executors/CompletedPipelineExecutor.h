#pragma once

namespace DB
{

class QueryPipeline;

class CompletedPipelineExecutor
{
public:
    explicit CompletedPipelineExecutor(QueryPipeline & pipeline_);

    void execute();

private:
    QueryPipeline & pipeline;
};

}
