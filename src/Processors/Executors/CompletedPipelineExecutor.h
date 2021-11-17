#pragma once
#include <functional>
#include <memory>

namespace DB
{

class QueryPipeline;

/// Executor for completed QueryPipeline.
/// Allows to specify a callback which checks if execution should be cancelled.
/// If callback is specified, runs execution in a separate thread.
class CompletedPipelineExecutor
{
public:
    explicit CompletedPipelineExecutor(QueryPipeline & pipeline_);
    ~CompletedPipelineExecutor();

    /// This callback will be called each interactive_timeout_ms (if it is not 0).
    /// If returns true, query would be cancelled.
    void setCancelCallback(std::function<bool()> is_cancelled, size_t interactive_timeout_ms_);

    void execute();
    struct Data;

private:
    QueryPipeline & pipeline;
    std::function<bool()> is_cancelled_callback;
    size_t interactive_timeout_ms = 0;
    std::unique_ptr<Data> data;
};

}
