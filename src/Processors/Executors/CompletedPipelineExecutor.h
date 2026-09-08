#pragma once
#include <Processors/Executors/CancelCallbackMode.h>

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
    /// A true result cancels execution, or only reading in PartialResult mode.
    void setCancelCallback(std::function<bool()> is_cancelled, size_t interactive_timeout_ms_, CancelCallbackMode mode = CancelCallbackMode::Cancel);

    void initialize();
    void execute();
    void cancel();

    struct Data;

private:
    void checkCancelCallback();

    CancelCallbackMode cancel_callback_mode = CancelCallbackMode::Cancel;
    QueryPipeline & pipeline;
    std::function<bool()> is_cancelled_callback;
    size_t interactive_timeout_ms = 0;
    std::unique_ptr<Data> data;
};

}
