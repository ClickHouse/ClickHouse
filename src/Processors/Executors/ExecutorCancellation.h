#pragma once

#include <functional>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class PipelineExecutor;

/// Binds a polled cancellation callback to its execution policy.
/// Exceptions from the callback always propagate unchanged.
class ExecutorCancellation
{
public:
    ExecutorCancellation() = default;

    static ExecutorCancellation cancelExecution(std::function<bool()> callback_);
    static ExecutorCancellation finishPartialResult(std::function<bool()> callback_);
    static ExecutorCancellation cancelQuery(std::function<bool()> callback_, ContextPtr context_);

    explicit operator bool() const { return bool(callback); }
    void check(PipelineExecutor & executor) const;

private:
    enum class Policy
    {
        CancelExecution,
        FinishPartialResult,
        CancelQuery,
    };

    ExecutorCancellation(std::function<bool()> callback_, Policy policy_, ContextPtr context_);

    std::function<bool()> callback;
    Policy policy = Policy::CancelExecution;
    ContextPtr context;
};

}
