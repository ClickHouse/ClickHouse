#pragma once
#include <memory>
#include <atomic>
#include <vector>

namespace DB
{

class Block;
class Chunk;
class QueryPipeline;
class PushingSource;

class PipelineExecutor;
using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::vector<ProcessorPtr>;

/// Pushing executor for Chain of processors. Always executed in single thread.
/// Typical usage is:
///
/// PushingPipelineExecutor executor(chain);
/// executor.start();
/// while (auto chunk = ...)
///     executor.push(std::move(chunk));
/// executor.finish();
class PushingPipelineExecutor
{
public:
    explicit PushingPipelineExecutor(QueryPipeline & pipeline_);
    ~PushingPipelineExecutor();

    /// Get structure of returned block or chunk.
    const Block & getHeader() const;

    void start();

    void push(Chunk chunk);
    void push(Block block);

    void finish();

    /// Stop execution. It is not necessary, but helps to stop execution before executor is destroyed.
    void cancel();

private:
    QueryPipeline & pipeline;
    std::atomic_bool input_wait_flag = false;
    std::shared_ptr<PushingSource> pushing_source;

    PipelineExecutorPtr executor;
    bool started = false;
    bool finished = false;
};

}
