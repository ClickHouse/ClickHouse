#pragma once

#include <memory>

namespace DB
{

class Block;
class QueryPipeline;
class PullingAsyncPipelineExecutor;
class PullingPipelineExecutor;

/// Wrapper for `Pulling(Async)PipelineExecutor` to dynamically dispatch calls to the right executor
class DictionaryPipelineExecutor
{
public:
    DictionaryPipelineExecutor(QueryPipeline & pipeline_, bool async);
    bool pull(Block & block);

    ~DictionaryPipelineExecutor();

private:
    bool pullImpl(Block & block);

    std::unique_ptr<PullingAsyncPipelineExecutor> async_executor;
    std::unique_ptr<PullingPipelineExecutor> executor;
};

}
