#pragma once

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
/// only safe way to join it. The handle carries no result payload: the
/// caller's own state (e.g. a `FetchMachine` context) carries the products;
/// the handle answers only "did it run / can I still revoke it / did it throw".
class JobHandle
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

    /// Block until the task resolves; rethrow an exception the TASK BODY
    /// threw. A revoked task resolves cleanly - cancellation is a correct
    /// outcome, not an error; read `state()` for the verdict.
    void get();

    /// Non-blocking probe: true once the worker has set the promise.
    bool isFinished() const noexcept;

    State state() const;

private:
    friend class PrefetchThreadPool;

    /// Passkey: lets the pool construct handles via `make_shared` while keeping
    /// the ctor closed to everyone else (a private ctor would block make_shared).
    struct ConstructTag
    {
        explicit ConstructTag() = default;
    };

public:
    explicit JobHandle(ConstructTag) : future(promise.get_future()) {}

private:
    std::atomic<State> current_state{State::Queued};
    std::promise<void> promise;
    std::future<void> future;
};

/// Shared bounded thread pool for the executor's background jobs (machine
/// fetch steps and deferred cache-write steps share it).
class PrefetchThreadPool
{
public:
    /// `queue_size` is the total scheduled-jobs cap (running + queued)
    /// enforced by the underlying ThreadPool. `submitJob` returns nullptr
    /// when the limit is reached; the caller falls back to doing the work
    /// synchronously. Defaults to `pool_size * 10`.
    explicit PrefetchThreadPool(size_t pool_size, size_t queue_size = 0);

    virtual ~PrefetchThreadPool() = default;

    /// Submit a task. Returns a handle to the scheduled task on success, or
    /// `nullptr` if the pool's queue is full / scheduling otherwise failed.
    /// The caller treats a nullptr return as "do it synchronously when you
    /// need the result" — no exception is propagated. No in-flight gauge is
    /// bumped here: the pool cannot know what kind of work the job carries —
    /// a caller that needs a gauge holds its own `CurrentMetrics::Increment`.
    ///
    /// Virtual so tests can install a mock that controls timing (e.g. drop
    /// every submission, or run it inline) without spinning up real workers.
    virtual std::shared_ptr<JobHandle> submitJob(std::function<void()> task);

    /// Test-only factory: a `Done`-state `JobHandle`. Lets tests exercise the
    /// consume path deterministically without real worker threads. Required
    /// because `JobHandle` can only be constructed via the pool's passkey —
    /// a subclass mock can't construct handles directly.
    static std::shared_ptr<JobHandle> makeCompletedJobHandleForTest();

protected:
    /// Test-only constructor: skips ThreadPool initialization so a mock
    /// subclass can override `submitJob` without paying for real workers.
    struct NoWorkers {};
    explicit PrefetchThreadPool(NoWorkers);

private:
    ThreadPool pool;
};

}
