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

    /// Non-blocking probe: true once the worker has set the promise.
    bool isFinished() const noexcept;

    /// For diagnostics/tests.
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
    explicit PrefetchHandle(ConstructTag) : future(promise.get_future()) {}

private:
    std::atomic<State> current_state{State::Queued};
    std::promise<Rope> promise;
    std::future<Rope> future;
};

/// Handle to a void task submitted via `submitJob`. Same lifecycle and CAS
/// protocol as `PrefetchHandle`, without a result payload: the caller's own
/// state carries the products; the handle answers only "did it run / can I
/// still revoke it / did it throw".
///
///   Queued ── worker CAS ─► Running ──► Done
///      │
///      └── caller CAS (tryCancel) ─► Cancelled (worker no-ops when it pulls)
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

    /// Block until the task completes; rethrow exceptions thrown by the task.
    /// Only valid after `tryCancel` returned false.
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
    virtual std::shared_ptr<PrefetchHandle> submit(std::function<Rope()> task);

    /// Submit a void task. Same null-on-full contract as `submit`. Unlike
    /// `submit`, no in-flight gauge is bumped here: the pool cannot know what
    /// kind of work the job carries — a caller that needs a gauge holds its
    /// own `CurrentMetrics::Increment` for the job's lifetime.
    virtual std::shared_ptr<JobHandle> submitJob(std::function<void()> task);

    /// Test-only factory: build a `Done`-state `PrefetchHandle` from a
    /// precomputed `Rope`. Lets tests exercise the prefetch-consume path
    /// deterministically without spinning up real worker threads. Required
    /// because `PrefetchHandle` can only be constructed via the pool's
    /// passkey — a subclass mock can't construct handles directly.
    static std::shared_ptr<PrefetchHandle> makeCompletedHandleForTest(Rope rope);

    /// Test-only factory: a `Done`-state `JobHandle`, for the same reason.
    static std::shared_ptr<JobHandle> makeCompletedJobHandleForTest();

protected:
    /// Test-only constructor: skips ThreadPool initialization so a mock
    /// subclass can override `submit` without paying for real workers.
    struct NoWorkers {};
    explicit PrefetchThreadPool(NoWorkers);

private:
    ThreadPool pool;
};

}
