#pragma once
#include <memory>
#include <atomic>

namespace DB
{

class Block;
class Chunk;
class Chain;
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
    explicit PushingPipelineExecutor(Chain & chain);
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
    Chain & chain;
    std::atomic_bool need_data_flag = false;
    std::shared_ptr<PushingSource> pushing_source;

    std::unique_ptr<Processors> processors;
    PipelineExecutorPtr executor;
    bool started = false;
    bool finished = false;
};

}
