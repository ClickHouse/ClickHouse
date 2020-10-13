#pragma once
#include <atomic>
#include <memory>

namespace DB
{
class Block;
class Chunk;
class QueryPipeline;
class PushingSource;
class PipelineExecutor;

using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

/// Pushing executor for QueryPipeline. Always execute pipeline in single thread.
/// Typical usage is:
///
/// PushingPipelineExecutor executor(query_pipeline);
///     ... receive a chunk ...
/// executor.push(chunk);
class PushingPipelineExecutor
{
public:
    explicit PushingPipelineExecutor(QueryPipeline & pipeline_, PushingSource & source_);
    ~PushingPipelineExecutor();

    /// Get structure of returned block or chunk.
    const Block & getHeader() const;

    /// Methods return false if query is finished.
    bool push(const Block & block);

    /// Stop execution. It is not necessary, but helps to stop execution before executor is destroyed.
    void cancel();

private:
    std::atomic_bool has_data_flag = false;
    QueryPipeline & pipeline;
    PushingSource & source;
    PipelineExecutorPtr executor;
};

}
