#pragma once

#include <Common/ThreadPool_fwd.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <mutex>

namespace DB
{

/// Type-erased body of one phase. It lives on the caller's stack for the phase's duration, so handing
/// it to the workers costs no allocation - a `std::function` would heap-allocate on every phase once
/// the captures exceed its small-buffer size.
struct PhaseBody
{
    virtual void run(size_t index) = 0;
    virtual ~PhaseBody() = default;
};

template <typename F>
class PhaseBodyOf final : public PhaseBody
{
public:
    explicit PhaseBodyOf(F & f_) : f(f_) {}
    void run(size_t index) override { f(index); }

private:
    F & f;
};

/** A fixed set of workers, started once and reused for every phase, where a phase is a unit of work
  * that ends in a barrier: the caller publishes it, the workers run it, the caller waits for all of
  * them.
  *
  * The point is to keep the per-job cost of a thread pool off a path that runs many small phases. Every
  * pool job attaches the thread to the query's thread group and detaches it afterwards, and that
  * attach/detach pair resets a ~1500-entry profile-event array, reads three of the `/proc/thread-self` files,
  * and creates and destroys two POSIX timers when a trace collector is configured. Scheduling one job
  * per worker per phase therefore makes that cost scale with the number of phases rather than with the
  * work; a caller running a phase per data block pays it thousands of times. Here the workers stay on
  * their pool jobs and are handed each phase through a condition variable, so the attach happens once
  * per worker per query.
  *
  * Not a general-purpose replacement for `ThreadPoolCallbackRunner`: it fits only a caller that runs
  * many small phases, needs a join point after each (so the phases cannot simply be pipelined), and can
  * hold pool jobs for its whole lifetime. Prefer the runner otherwise.
  *
  * Ordering: everything a phase writes is visible to the next phase. Workers publish their writes by
  * taking `mutex` to report completion, and the caller takes the same `mutex` to observe it and again
  * to publish the next phase. Do not weaken the completion counter to a lock-free atomic without
  * replacing that release/acquire pair.
  *
  * Threading: all public methods are for the owning (single) caller thread only. Calling them from
  * inside a phase body would deadlock, so it is rejected instead.
  *
  * A parked worker holds a pool job but consumes no CPU. The pool must therefore not be shared with
  * anything else that needs to make progress, and the worker count is capped at the pool's thread count
  * so a started worker always has a thread to run on.
  */
class PhasedWorkers
{
public:
    /// `max_workers` is clamped to the pool's thread count: a phase must never be able to wait on a job
    /// the pool cannot schedule, which would hang with no diagnostic.
    PhasedWorkers(ThreadPool & pool_, ThreadName thread_name_, size_t max_workers_);
    ~PhasedWorkers();

    PhasedWorkers(const PhasedWorkers &) = delete;
    PhasedWorkers & operator=(const PhasedWorkers &) = delete;

    /// Upper bound on `active` accepted by the run methods below.
    size_t maxWorkers() const { return max_workers; }

    /// Run `body(w)` once for each of the first `active` workers, passing its own index. Use when each
    /// worker owns a fixed slice of the input that has to line up across phases.
    void runPerWorker(PhaseBody & body, size_t active);

    /// Run `body(i)` for every i in [0, total), the first `active` workers pulling through an atomic
    /// cursor. Use when the items are independent and may be claimed in any order.
    void runDispatch(PhaseBody & body, size_t active, size_t total);

private:
    enum class PhaseKind : uint8_t
    {
        PerWorker,
        Dispatch,
    };

    void ensureStarted(size_t needed);
    /// The annotations below document the locking discipline and are checked for accesses from anywhere
    /// else, but these two bodies wait on a condition variable, which the analysis cannot follow through
    /// `std::unique_lock` - so they opt out, as `MergeTreeBackgroundExecutor::threadFunction` does.
    void runPhase(PhaseBody & body, PhaseKind kind, size_t active, size_t total) TSA_NO_THREAD_SAFETY_ANALYSIS;
    void workerLoop(size_t worker_index) TSA_NO_THREAD_SAFETY_ANALYSIS;

    const size_t max_workers;
    ThreadPoolCallbackRunnerLocal<void> runner;
    /// Owning-thread-only, so unguarded: workers never start workers.
    size_t started_workers = 0;

    std::mutex mutex;
    std::condition_variable work_available;
    std::condition_variable phase_finished;

    /// Bumped for every published phase. A worker compares it against the last phase it saw, which is
    /// how it tells a new phase from a spurious wake-up.
    size_t phase_seq TSA_GUARDED_BY(mutex) = 0;
    size_t done_count TSA_GUARDED_BY(mutex) = 0;
    size_t active_workers TSA_GUARDED_BY(mutex) = 0;
    size_t dispatch_total TSA_GUARDED_BY(mutex) = 0;
    PhaseBody * body TSA_GUARDED_BY(mutex) = nullptr;
    PhaseKind kind TSA_GUARDED_BY(mutex) = PhaseKind::PerWorker;
    bool phase_running TSA_GUARDED_BY(mutex) = false;
    bool stop TSA_GUARDED_BY(mutex) = false;
    std::exception_ptr first_error TSA_GUARDED_BY(mutex);

    /// Read without the mutex by every worker of a dispatch phase.
    std::atomic<size_t> cursor{0};
    /// Set by the first worker that throws, so the others stop claiming items instead of running a
    /// phase whose result is already going to be discarded.
    std::atomic<bool> phase_failed{false};
};

}
