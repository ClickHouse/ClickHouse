#pragma once

#include <Core/Block_fwd.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <atomic>
#include <memory>

namespace DB
{

class Block;
class Chunk;
class QueryPipeline;
class PullingOutputFormat;
struct ProfileInfo;

/// Pulling executor for QueryPipeline. Always execute pipeline in single thread.
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

    /// Get structure of returned block or chunk.
    const Block & getHeader() const;
    const SharedHeader & getSharedHeader() const;

    /// Methods return false if query is finished.
    /// You can use any pull method.
    bool pull(Chunk & chunk);
    bool pull(Block & block);

    /// Stop execution. It is not necessary, but helps to stop execution before executor is destroyed.
    void cancel();

    /// Get totals and extremes. Returns empty chunk if doesn't have any.
    Chunk getTotals();
    Chunk getExtremes();

    /// Get totals and extremes. Returns empty chunk if doesn't have any.
    Block getTotalsBlock();
    Block getExtremesBlock();

    /// Get query profile info.
    ProfileInfo & getProfileInfo();

    /// Returns the final state of the internal `PipelineExecutor`. Use this after `pull` returned `false`
    /// to distinguish normal end-of-stream (`Executing` — the status is not switched to `Finished`)
    /// from cancellation (`CancelledByTimeout` / `CancelledByUser`).
    PipelineExecutor::ExecutionStatus getExecutionStatus() const;

private:
    std::atomic_bool has_data_flag = false;
    QueryPipeline & pipeline;
    std::shared_ptr<PullingOutputFormat> pulling_format;
    PipelineExecutorPtr executor;
};

}
