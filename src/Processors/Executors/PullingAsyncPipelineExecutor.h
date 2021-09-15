#pragma once
#include <memory>

namespace DB
{

class QueryPipeline;
class Block;
class Chunk;
class LazyOutputFormat;
struct BlockStreamProfileInfo;

/// Asynchronous pulling executor for QueryPipeline.
/// Always creates extra thread. If query is executed in single thread, use PullingPipelineExecutor.
/// Typical usage is:
///
/// PullingAsyncPipelineExecutor executor(query_pipeline);
/// while (executor.pull(chunk, timeout))
///     ... process chunk ...
class PullingAsyncPipelineExecutor
{
public:
    explicit PullingAsyncPipelineExecutor(QueryPipeline & pipeline_);
    ~PullingAsyncPipelineExecutor();

    /// Get structure of returned block or chunk.
    const Block & getHeader() const;

    /// Methods return false if query is finished.
    /// If milliseconds > 0, returns empty object and `true` after timeout exceeded. Otherwise method is blocking.
    /// You can use any pull method.
    bool pull(Chunk & chunk, uint64_t milliseconds = 0);
    bool pull(Block & block, uint64_t milliseconds = 0);

    /// Stop execution. It is not necessary, but helps to stop execution before executor is destroyed.
    void cancel();

    /// Get totals and extremes. Returns empty chunk if doesn't have any.
    Chunk getTotals();
    Chunk getExtremes();

    /// Get totals and extremes. Returns empty chunk if doesn't have any.
    Block getTotalsBlock();
    Block getExtremesBlock();

    /// Get query profile info.
    BlockStreamProfileInfo & getProfileInfo();

    /// Internal executor data.
    struct Data;

private:
    QueryPipeline & pipeline;
    std::shared_ptr<LazyOutputFormat> lazy_format;
    std::unique_ptr<Data> data;
};

}
