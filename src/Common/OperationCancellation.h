#pragma once

#include <atomic>
#include <functional>

namespace DB
{

/// `StopWork` stops ordinary work at cancellation checks. Cleanup uses `QueryCancellationBlocker`
/// to preserve rollback safety, even when retries must continue indefinitely.
/// Cancellation does not interrupt a running syscall or guarantee bounded shutdown.
enum class CancellationRequest
{
    None,
    StopWork,
};

using CancellationRequestPredicate = std::function<CancellationRequest()>;

class OperationCancellationState
{
public:
    void request(CancellationRequest value) noexcept
    {
        if (value != CancellationRequest::None)
            requested.store(value, std::memory_order_relaxed);
    }

    CancellationRequest getRequest() const noexcept { return requested.load(std::memory_order_relaxed); }

private:
    std::atomic<CancellationRequest> requested{CancellationRequest::None};
};

}
