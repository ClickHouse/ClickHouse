#pragma once

#include <Common/ThreadPool.h>

#include <atomic>
#include <future>
#include <functional>
#include <memory>

namespace DB
{

/// Handle to a task submitted to the `PrefetchThreadPool`:
///
///   Queued  ── worker pulls the task and CAS'es to ─►  Running ──► Done
///       │
///       └── caller CAS'es to Cancelled (tryCancel) ──►  Cancelled
///
/// The handle carries no result payload - the caller's own state carries the
/// products; the handle answers "did it run / can I still revoke it / did it
/// throw".
class JobHandle
{
private:
    friend class PrefetchThreadPool;

    /// Passkey: lets the pool construct handles via `make_shared` while
    /// keeping the ctor closed to everyone else.
    struct ConstructTag
    {
        explicit ConstructTag() = default;
    };

public:
    enum class State : uint8_t
    {
        Queued,
        Running,
        Cancelled,
        Done,
    };

    explicit JobHandle(ConstructTag);

    /// True iff the task hadn't started yet AND was prevented from running.
    bool tryCancel();

    /// Block until the task resolves; rethrows an exception the task body
    /// threw. A revoked task resolves cleanly - read `state()` for the verdict.
    void get();

    /// Non-blocking probe: true once the worker has set the promise.
    bool isFinished() const noexcept;

    State state() const;

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
    /// `queue_size` caps scheduled jobs (running + queued); defaults to
    /// `pool_size * 10`.
    explicit PrefetchThreadPool(size_t pool_size, size_t queue_size = 0);

    virtual ~PrefetchThreadPool() = default;

    /// Returns nullptr when the queue is full / scheduling failed - the caller
    /// falls back to doing the work synchronously, no exception propagated.
    /// Virtual so tests can install a mock that controls timing.
    virtual std::shared_ptr<JobHandle> submitJob(std::function<void()> task);

    /// Test-only factory: a `Done`-state handle (`JobHandle` is only
    /// constructible via the pool's passkey, so mocks can't make one).
    static std::shared_ptr<JobHandle> makeCompletedJobHandleForTest();

protected:
    /// Test-only constructor: no workers, for `submitJob`-overriding mocks.
    struct NoWorkers {};
    explicit PrefetchThreadPool(NoWorkers);

private:
    ThreadPool pool;
};

}
