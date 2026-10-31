#pragma once

#include <Common/CurrentThread.h>
#include <base/defines.h>

#include <algorithm>
#include <chrono>
#include <mutex>
#include <optional>

namespace DB
{

/// Retries `try_for(slice)` until it succeeds, the deadline passes, or the query is killed.
/// Cancellation is a no-op off-query, so background callers simply wait.
template <typename TryFor>
bool waitCancellable(
    TryFor && try_for,
    std::optional<std::chrono::steady_clock::time_point> deadline = {},
    std::chrono::milliseconds slice = std::chrono::milliseconds(100))
{
    while (true)
    {
        auto timeout = slice;
        if (deadline)
        {
            const auto now = std::chrono::steady_clock::now();
            if (now >= *deadline)
                return try_for(std::chrono::milliseconds::zero());
            timeout = std::min(slice, std::chrono::ceil<std::chrono::milliseconds>(*deadline - now));
        }
        const bool acquired = try_for(timeout);
        CurrentThread::checkIfNotCancelled();
        if (acquired)
            return true;
    }
}

template <typename Mutex>
class TSA_SCOPED_LOCKABLE CancellableLockGuard
{
public:
    explicit CancellableLockGuard(Mutex & mutex_) TSA_ACQUIRE(mutex_) : lock(mutex_, std::defer_lock)
    {
        waitCancellable([&](auto timeout) { return lock.try_lock_for(timeout); });
    }
    ~CancellableLockGuard() TSA_RELEASE() = default;

private:
    std::unique_lock<Mutex> lock;
};
_LIBCPP_CTAD_SUPPORTED_FOR_TYPE(CancellableLockGuard);

}
