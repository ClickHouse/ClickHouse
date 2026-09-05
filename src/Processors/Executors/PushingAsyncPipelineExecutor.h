#pragma once
#include <memory>
#include <vector>

namespace DB
{

class Block;
class Chunk;
class QueryPipeline;
class PushingAsyncSource;

class PipelineExecutor;
using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::vector<ProcessorPtr>;

/// Pushing executor for Chain of processors using several background threads.
/// Typical usage is:
///
/// PushingAsyncPipelineExecutor executor(chain);
/// executor.start();
/// while (auto chunk = ...)
///     executor.push(std::move(chunk));
/// executor.finish();
class PushingAsyncPipelineExecutor
{
public:
    explicit PushingAsyncPipelineExecutor(QueryPipeline & pipeline_);
    ~PushingAsyncPipelineExecutor();

    /// Get structure of returned block or chunk.
    const Block & getHeader() const;

    void start();

    void push(Chunk chunk);
    void push(Block block);

    void finish();

    /// Stop execution. It is not necessary, but helps to stop execution before executor is destroyed.
    void cancel();

    struct Data;

private:
    QueryPipeline & pipeline;
    std::shared_ptr<PushingAsyncSource> pushing_source;

    bool started = false;
    bool finished = false;

    std::unique_ptr<Data> data;
};

}
