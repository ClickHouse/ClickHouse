#pragma once

#include <atomic>
#include <functional>

namespace DB
{

/// Note that `StopWork` requests cooperative cancellation: the operation stops at its next cancellation check.
/// It itself remains responsible for rollback and cleanup. If you want to protect a cleanup, use ThreadStatus::QueryCancellationBlocker
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
