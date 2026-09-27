#include <Processors/Transforms/PhasedWorkers.h>

#include <Common/Exception.h>
#include <Common/ThreadPool.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PhasedWorkers::PhasedWorkers(ThreadPool & pool_, ThreadName thread_name_, size_t max_workers_)
    : max_workers(std::min(max_workers_, pool_.getMaxThreads()))
    , runner(pool_, thread_name_)
{
}

PhasedWorkers::~PhasedWorkers()
{
    {
        std::lock_guard lock(mutex);
        stop = true;
    }
    work_available.notify_all();
    try
    {
        runner.waitForAllToFinishAndRethrowFirstError();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void PhasedWorkers::runPerWorker(PhaseBody & body_, size_t active)
{
    runPhase(body_, PhaseKind::PerWorker, active, 0);
}

void PhasedWorkers::runDispatch(PhaseBody & body_, size_t active, size_t total)
{
    runPhase(body_, PhaseKind::Dispatch, active, total);
}

void PhasedWorkers::ensureStarted(size_t needed)
{
    /// Grown only up to the largest worker count any phase has needed, so a caller whose phases only
    /// ever use a few workers never parks the whole pool. If enqueueing throws part way, the workers
    /// already started stay counted and are stopped by the destructor, and the phase is not published.
    for (; started_workers < needed; ++started_workers)
        runner.enqueueAndKeepTrack([this, w = started_workers] { workerLoop(w); }, Priority{});
}

void PhasedWorkers::runPhase(PhaseBody & body_, PhaseKind kind_, size_t active, size_t total) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    /// Not an assertion: `chassert` is compiled out in release builds, and publishing a phase with more
    /// active workers than there are started ones would wait for a completion that can never arrive -
    /// a hang with no diagnostic.
    if (active == 0 || active > max_workers)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PhasedWorkers: {} active workers requested, must be in [1, {}]",
            active,
            max_workers);

    ensureStarted(active);

    {
        std::lock_guard lock(mutex);

        if (phase_running)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "PhasedWorkers: a phase is already running; the run methods are for the owning thread "
                "only and must not be called from inside a phase body");

        body = &body_;
        kind = kind_;
        active_workers = active;
        dispatch_total = total;
        cursor.store(0, std::memory_order_relaxed);
        phase_failed.store(false, std::memory_order_relaxed);
        done_count = 0;
        phase_running = true;
        ++phase_seq;
    }
    work_available.notify_all();

    std::unique_lock lock(mutex);
    phase_finished.wait(lock, [this] TSA_REQUIRES(mutex) { return done_count == active_workers; });
    phase_running = false;
    body = nullptr;

    if (first_error)
    {
        auto error = first_error;
        first_error = {};
        std::rethrow_exception(error);
    }
}

void PhasedWorkers::workerLoop(size_t worker_index) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    size_t seen_seq = 0;
    while (true)
    {
        PhaseBody * phase_body = nullptr;
        PhaseKind phase_kind = PhaseKind::PerWorker;
        size_t total = 0;
        {
            std::unique_lock lock(mutex);
            work_available.wait(lock, [this, &seen_seq] TSA_REQUIRES(mutex) { return stop || phase_seq != seen_seq; });
            if (stop)
                return;
            seen_seq = phase_seq;

            /// Not taking part in this phase: back to waiting, without counting towards it. The caller
            /// waits on `active_workers`, so a worker that skips is not waited for.
            if (worker_index >= active_workers)
                continue;

            phase_body = body;
            phase_kind = kind;
            total = dispatch_total;
        }

        try
        {
            if (phase_kind == PhaseKind::PerWorker)
            {
                phase_body->run(worker_index);
            }
            else
            {
                while (!phase_failed.load(std::memory_order_relaxed))
                {
                    const size_t i = cursor.fetch_add(1, std::memory_order_relaxed);
                    if (i >= total)
                        break;
                    phase_body->run(i);
                }
            }
        }
        catch (...)
        {
            phase_failed.store(true, std::memory_order_relaxed);
            std::lock_guard lock(mutex);
            if (!first_error)
                first_error = std::current_exception();
        }

        {
            std::lock_guard lock(mutex);
            ++done_count;
            if (done_count == active_workers)
            {
                /// Notified with the lock held: the waiter's predicate reads `done_count`.
                phase_finished.notify_one();
            }
        }
    }
}

}
