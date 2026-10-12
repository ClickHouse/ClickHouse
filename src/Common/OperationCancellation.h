#pragma once

#include <atomic>
#include <functional>

namespace DB
{

enum class CancellationRequest
{
    None,
    StopWork,
    StopAll,
};

enum class ExecutionPhase
{
    Work,
    Cleanup,
};

using CancellationRequestPredicate = std::function<CancellationRequest()>;

class OperationCancellationState
{
public:
    void request(CancellationRequest value) noexcept
    {
        auto previous = requested.load(std::memory_order_relaxed);
        while (previous < value && !requested.compare_exchange_weak(previous, value, std::memory_order_relaxed))
        {
        }
    }

    CancellationRequest getRequest() const noexcept { return requested.load(std::memory_order_relaxed); }

private:
    std::atomic<CancellationRequest> requested{CancellationRequest::None};
};

class ThreadStatus;
class ThreadGroup;

/// Changes only this execution's sensitivity; never clears the shared cancellation request.
class ExecutionPhaseScope
{
public:
    explicit ExecutionPhaseScope(ExecutionPhase phase) noexcept;
    ~ExecutionPhaseScope();
    ExecutionPhaseScope(const ExecutionPhaseScope &) = delete;
    ExecutionPhaseScope & operator=(const ExecutionPhaseScope &) = delete;

private:
    ThreadStatus * thread;
    const ThreadGroup * group;
    ExecutionPhase previous;
};

}
