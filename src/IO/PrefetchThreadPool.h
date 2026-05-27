#pragma once

#include <IO/Rope.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <future>
#include <functional>
#include <memory>

namespace DB
{

/// Handle to a task submitted to the PrefetchThreadPool.
///
/// Lifecycle:
///   Queued  ── worker pulls the task and CAS'es to ─►  Running ──► Done
///       │
///       └── caller CAS'es to Cancelled (tryCancel) ──►  Cancelled
///                                                          (worker no-ops when it pulls)
///
/// `tryCancel` returns true only if the task hadn't started yet. In that
/// case the caller should do the work synchronously. If `tryCancel` returns
/// false, the worker either is running or has finished — `get()` is the
/// only safe way to obtain the result.
class PrefetchHandle
{
public:
    enum class State : uint8_t
    {
        Queued,
        Running,
        Cancelled,
        Done,
    };

    /// True iff the task hadn't started yet AND we successfully prevented it
    /// from running. False if the task is Running, Done, or already Cancelled.
    bool tryCancel();

    /// Block until the task completes; rethrow exceptions set by the worker.
    /// Only valid after `tryCancel` returned false.
    Rope get();

    /// For diagnostics/tests.
    State state() const;

private:
    friend class PrefetchThreadPool;

    struct SharedState
    {
        std::atomic<State> state{State::Queued};
        std::promise<Rope> promise;
    };

    PrefetchHandle(std::shared_ptr<SharedState> shared_, std::future<Rope> future_)
        : shared(std::move(shared_)), future(std::move(future_)) {}

    std::shared_ptr<SharedState> shared;
    std::future<Rope> future;
};

/// Shared bounded thread pool for prefetch tasks.
class PrefetchThreadPool
{
public:
    /// `queue_size` is the total scheduled-jobs cap (running + queued)
    /// enforced by the underlying ThreadPool. `submit` returns nullptr
    /// when the limit is reached; the caller falls back to a synchronous
    /// read. Defaults to `pool_size * 10`.
    explicit PrefetchThreadPool(size_t pool_size, size_t queue_size = 0);

    virtual ~PrefetchThreadPool() = default;

    /// Submit a task. Returns a handle to the scheduled task on success, or
    /// `nullptr` if the pool's queue is full / scheduling otherwise failed.
    /// The caller treats a nullptr return as "do it synchronously when you
    /// need the result" — no exception is propagated.
    ///
    /// Virtual so tests can install a mock that controls timing (e.g. drop
    /// every submission) without spinning up real workers.
    virtual std::unique_ptr<PrefetchHandle> submit(std::function<Rope()> task);

protected:
    /// Test-only constructor: skips ThreadPool initialization so a mock
    /// subclass can override `submit` without paying for real workers.
    struct NoWorkers {};
    explicit PrefetchThreadPool(NoWorkers);

private:
    ThreadPool pool;
};

}
