#pragma once

#include <atomic>
#include <memory>

namespace DB
{

/// Copies share the local stop signal for one reading scope. An empty token does not allocate a signal.
class ReadCancellationToken
{
public:
    ReadCancellationToken() = default;

    static ReadCancellationToken create();

    bool isInitialized() const { return bool(cancelled); }
    bool isCancelled() const { return cancelled && cancelled->load(std::memory_order_relaxed); }

    /// Requires an initialized token. Returns true only for the first stop request.
    bool cancel() const noexcept;

    /// Full query cancellation takes precedence over the local stop signal.
    void checkIfNotCancelled() const;

private:
    std::shared_ptr<std::atomic_bool> cancelled;
};

}
