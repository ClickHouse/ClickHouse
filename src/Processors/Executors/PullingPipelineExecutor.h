#pragma once
#include <memory>

namespace DB
{

class QueryPipeline;
class Block;
class Chunk;
class LazyOutputFormat;
struct BlockStreamProfileInfo;

/// Pulling executor for QueryPipeline.
/// Typical usage is:
///
/// PullingPipelineExecutor executor(query_pipeline);
/// while (executor.pull(chunk))
///     ... process chunk ...
class PullingPipelineExecutor
{
public:
    explicit PullingPipelineExecutor(QueryPipeline & pipeline_);
    ~PullingPipelineExecutor();

    /// Methods return false if query is finished.
    /// If milliseconds > 0, returns empty object and `true` after timeout exceeded.
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

    struct Data;

private:
    QueryPipeline & pipeline;
    std::shared_ptr<LazyOutputFormat> lazy_format;
    std::unique_ptr<Data> data;
};

}
