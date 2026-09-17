#pragma once
#include <Processors/Executors/ExecutorCancellation.h>

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

    /// Check before starting execution and each interactive_timeout_ms (if it is not 0).
    /// A true result cancels execution. Use an explicit policy to finish a partial result or cancel the query.
    void setCancelCallback(std::function<bool()> callback, size_t interactive_timeout_ms_);
    void setCancelCallback(ExecutorCancellation callback, size_t interactive_timeout_ms_);

    void initialize();
    void execute();
    void cancel();

    struct Data;

private:
    QueryPipeline & pipeline;
    ExecutorCancellation cancel_callback;
    size_t interactive_timeout_ms = 0;
    std::unique_ptr<Data> data;
};

}
