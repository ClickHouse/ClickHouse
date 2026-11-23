/// Throughput of `ThreadPool` job hand-off under concurrent schedulers.
///
/// Every case runs `benchmark::State::threads` schedulers at once: a 0.5 s warm-up, then a 2 s measured window
/// (see `Run`). Parameters:
///   - `cpus`: the process is pinned to its first `cpus` CPUs, as on a loaded server.
///   - `work_ns`: thread CPU time each job spins for. Short jobs expose the cost of the hand-off itself.
///   - the pool size and the number of jobs each scheduler keeps in flight.
///
/// Counters: `jobs_per_second` (the result); `global_lock_wait_us_per_job` and `local_lock_wait_us_per_job`
/// (`GlobalThreadPoolLockWaitMicroseconds` and `LocalThreadPoolLockWaitMicroseconds` per job);
/// `offered_jobs_per_cpu`; for `FreePool` also `runnable_jobs_per_cpu` (the same, capped by the pool size).
///
/// Cases:
///   - `FreePool`: schedulers share one `FreeThreadPool` and each waits for its own batch of jobs.
///   - `QueryPools`: each iteration is one short query: a local `ThreadPool` of `query_threads` takes threads
///     from the global pool (server defaults: 10000 threads, 1000 kept free), runs one job per thread and is
///     destroyed.
///
/// The global pool keeps state between cases, so run each case in its own process, for example:
///   benchmark_thread_pool --benchmark_filter='^FreePool/cpus:4/pool_size:256/batch:16/work_ns:0/iterations:1/manual_time/threads:16$'

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <functional>
#include <latch>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#if defined(OS_LINUX)
#include <sched.h>
#endif

namespace CurrentMetrics
{
    extern const Metric GlobalThread;
    extern const Metric GlobalThreadActive;
    extern const Metric GlobalThreadScheduled;
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace ProfileEvents
{
    extern const Event GlobalThreadPoolLockWaitMicroseconds;
    extern const Event LocalThreadPoolLockWaitMicroseconds;
}

namespace
{

using Clock = std::chrono::steady_clock;

constexpr auto warmup_s = std::chrono::milliseconds(500);
constexpr auto measure_s = std::chrono::seconds(2);
constexpr size_t global_pool_max_free_threads = 1000;

/// Pins every thread of the process to the first `cpus` CPUs it may run on, or back to all of them if 0.
/// Threads created later inherit the mask of their creator. Returns false if that is not possible.
bool limitCpus(size_t cpus)
{
#if defined(OS_LINUX)
    static const cpu_set_t initial = []
    {
        cpu_set_t mask;
        CPU_ZERO(&mask);
        sched_getaffinity(0, sizeof(mask), &mask);
        return mask;
    }();

    cpu_set_t mask = initial;
    if (cpus)
    {
        if (static_cast<size_t>(CPU_COUNT(&initial)) < cpus)
            return false;
        CPU_ZERO(&mask);
        size_t taken = 0;
        for (int cpu = 0; cpu < CPU_SETSIZE && taken < cpus; ++cpu)
        {
            if (CPU_ISSET(cpu, &initial))
            {
                CPU_SET(cpu, &mask);
                ++taken;
            }
        }
    }

    bool ok = true;
    for (const auto & task : std::filesystem::directory_iterator("/proc/self/task"))
    {
        const auto tid = static_cast<pid_t>(std::stol(task.path().filename().string()));
        cpu_set_t applied;
        ok = ok && sched_setaffinity(tid, sizeof(mask), &mask) == 0 && sched_getaffinity(tid, sizeof(applied), &applied) == 0
            && CPU_EQUAL(&applied, &mask);
    }
    return ok;
#else
    return cpus == 0;
#endif
}

int64_t threadCpuNanoseconds()
{
    timespec ts{};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1'000'000'000 + ts.tv_nsec;
}

/// Thread CPU time, so that preemption does not shorten the work.
void spin(int64_t work_ns)
{
    if (work_ns <= 0)
        return;
    const int64_t deadline = threadCpuNanoseconds() + work_ns;
    while (threadCpuNanoseconds() < deadline)
        benchmark::ClobberMemory();
}

struct LockWait
{
    uint64_t global = 0;
    uint64_t local = 0;

    static LockWait now()
    {
        return {
            ProfileEvents::global_counters[ProfileEvents::GlobalThreadPoolLockWaitMicroseconds],
            ProfileEvents::global_counters[ProfileEvents::LocalThreadPoolLockWaitMicroseconds]};
    }
};

/// Makes `threads` workers of `pool` exist and be idle: each of them takes one job that waits for all others.
template <typename Pool>
void createIdleWorkers(Pool & pool, size_t threads)
{
    std::latch started(threads);
    size_t scheduled = 0;
    try
    {
        for (; scheduled < threads; ++scheduled)
            pool.scheduleOrThrowOnError([&] { started.arrive_and_wait(); });
    }
    catch (...)
    {
        /// Release the jobs already scheduled before `started` goes away.
        started.count_down(static_cast<std::ptrdiff_t>(threads - scheduled));
        pool.wait();
        throw;
    }
    pool.wait();
}

struct Run;

struct OnPhaseEnd
{
    Run * run;
    void operator()() noexcept;
};

/// State shared by the schedulers of one case, set up by thread 0. Every scheduler runs units of work (`unit`
/// returns the number of jobs it ran) until a common deadline, first to warm up, then for the measured window.
/// Each phase ends at a barrier, which waits until the pool has no scheduled jobs: a worker decrements that
/// count right after recording its lock wait, so the window and its counters cover exactly its own jobs.
struct Run
{
    bool ok = true;
    std::string error;
    Clock::time_point warmup_end;
    Clock::time_point window_start;
    Clock::time_point window_deadline;
    Clock::time_point window_end;
    LockWait lock_wait_start;
    LockWait lock_wait_end;
    std::atomic<size_t> jobs{0};
    size_t phase = 0;
    std::unique_ptr<std::barrier<OnPhaseEnd>> phase_end;
    /// Number of jobs scheduled in the pool that the units use.
    std::function<size_t()> scheduled_jobs;
    std::atomic<bool> unit_failed{false};
    std::mutex unit_error_mutex;
    std::string unit_error;

    void start(size_t cpus, size_t schedulers)
    {
        ok = limitCpus(cpus);
        error = ok ? "" : "cannot pin to " + std::to_string(cpus) + " CPUs";
        jobs = 0;
        phase = 0;
        unit_failed = false;
        unit_error.clear();
        phase_end = std::make_unique<std::barrier<OnPhaseEnd>>(static_cast<std::ptrdiff_t>(schedulers), OnPhaseEnd{this});
    }

    void fail(const std::string & message)
    {
        if (ok)
            error = message;
        ok = false;
    }

    void startWarmup() { warmup_end = Clock::now() + warmup_s; }

    void waitUntilIdle() const
    {
        while (scheduled_jobs() != 0)
            std::this_thread::sleep_for(std::chrono::microseconds(20));
    }

    void onPhaseEnd() noexcept
    {
        if (phase++ == 0)
        {
            waitUntilIdle();
            lock_wait_start = LockWait::now();
            window_start = Clock::now();
            window_deadline = window_start + measure_s;
        }
        else
        {
            window_end = Clock::now();
            waitUntilIdle();
            lock_wait_end = LockWait::now();
        }
    }

    /// An exception stops every scheduler at its next check, so none waits at a barrier forever.
    template <typename Unit>
    size_t runUnit(Unit & unit)
    {
        try
        {
            return unit();
        }
        catch (...)
        {
            std::lock_guard lock(unit_error_mutex);
            if (!unit_failed.exchange(true))
                unit_error = DB::getCurrentExceptionMessage(/* with_stacktrace */ false);
            return 0;
        }
    }

    template <typename Unit>
    void loop(benchmark::State & state, Unit && unit)
    {
        while (Clock::now() < warmup_end && !unit_failed)
            runUnit(unit);
        phase_end->arrive_and_wait();

        size_t measured = 0;
        while (Clock::now() < window_deadline && !unit_failed)
            measured += runUnit(unit);
        jobs += measured;
        phase_end->arrive_and_wait();

        state.SetIterationTime(std::chrono::duration<double>(window_end - window_start).count());
    }

    void report(benchmark::State & state, size_t cpus, double offered_jobs)
    {
        if (unit_failed)
        {
            state.SkipWithError(unit_error);
            return;
        }
        const double total_jobs = static_cast<double>(jobs.load());
        const double seconds = std::chrono::duration<double>(window_end - window_start).count();
        state.counters["jobs_per_second"] = total_jobs / seconds;
        state.counters["global_lock_wait_us_per_job"] = static_cast<double>(lock_wait_end.global - lock_wait_start.global) / total_jobs;
        state.counters["local_lock_wait_us_per_job"] = static_cast<double>(lock_wait_end.local - lock_wait_start.local) / total_jobs;
        state.counters["offered_jobs_per_cpu"] = offered_jobs / static_cast<double>(cpus);
    }
};

void OnPhaseEnd::operator()() noexcept
{
    run->onPhaseEnd();
}

Run run;
std::unique_ptr<FreeThreadPool> free_pool;

/// Args: cpus, pool_size, batch (jobs in flight per scheduler), work_ns.
void FreePool(benchmark::State & state)
{
    const auto cpus = static_cast<size_t>(state.range(0));
    const auto pool_size = static_cast<size_t>(state.range(1));
    const auto batch = static_cast<size_t>(state.range(2));
    const int64_t work_ns = state.range(3);
    const auto schedulers = static_cast<size_t>(state.threads());

    if (state.thread_index() == 0)
    {
        run.start(cpus, schedulers);
        try
        {
            free_pool = std::make_unique<FreeThreadPool>(
                CurrentMetrics::GlobalThread, CurrentMetrics::GlobalThreadActive, CurrentMetrics::GlobalThreadScheduled,
                pool_size, pool_size, /* queue_size */ 0);
            createIdleWorkers(*free_pool, pool_size);
            run.scheduled_jobs = [] { return free_pool->active(); };
            if (static_cast<size_t>(CurrentMetrics::get(CurrentMetrics::GlobalThread)) != pool_size)
                run.fail("idle workers were not created");
        }
        catch (const std::exception & e)
        {
            run.fail(e.what());
        }
        run.startWarmup();
    }

    for (auto _ : state)
    {
        if (!run.ok)
        {
            state.SetIterationTime(1);
            continue;
        }
        run.loop(state, [&]
        {
            std::latch done(batch);
            size_t scheduled = 0;
            try
            {
                for (; scheduled < batch; ++scheduled)
                    free_pool->scheduleOrThrowOnError([&] { spin(work_ns); done.count_down(); });
            }
            catch (...)
            {
                /// Wait for the jobs already scheduled before `done` goes away.
                done.count_down(static_cast<std::ptrdiff_t>(batch - scheduled));
                done.wait();
                throw;
            }
            done.wait();
            return batch;
        });
    }

    if (state.thread_index() == 0)
    {
        if (run.ok)
        {
            run.report(state, cpus, static_cast<double>(batch * schedulers));
            state.counters["runnable_jobs_per_cpu"]
                = static_cast<double>(std::min(batch * schedulers, pool_size)) / static_cast<double>(cpus);
        }
        else
            state.SkipWithError(run.error);
        free_pool.reset();
        limitCpus(0);
    }
}

/// Args: cpus, query_threads, work_ns.
void QueryPools(benchmark::State & state)
{
    const auto cpus = static_cast<size_t>(state.range(0));
    const auto query_threads = static_cast<size_t>(state.range(1));
    const int64_t work_ns = state.range(2);
    const auto schedulers = static_cast<size_t>(state.threads());

    if (state.thread_index() == 0)
    {
        run.start(cpus, schedulers);
        try
        {
            static std::once_flag global_pool_initialized;
            std::call_once(global_pool_initialized, [] { GlobalThreadPool::initialize(10000, global_pool_max_free_threads); });
            const size_t idle_workers = std::min(schedulers * query_threads, global_pool_max_free_threads);
            createIdleWorkers(GlobalThreadPool::instance(), idle_workers);
            /// Each unit destroys its local pool, so only global workers can still be busy at a barrier.
            run.scheduled_jobs = [] { return GlobalThreadPool::instance().active(); };
            if (static_cast<size_t>(CurrentMetrics::get(CurrentMetrics::GlobalThread)) < idle_workers)
                run.fail("idle workers were not created");
        }
        catch (const std::exception & e)
        {
            run.fail(e.what());
        }
        run.startWarmup();
    }

    for (auto _ : state)
    {
        if (!run.ok)
        {
            state.SetIterationTime(1);
            continue;
        }
        run.loop(state, [&]
        {
            ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, query_threads);
            for (size_t i = 0; i < query_threads; ++i)
                pool.scheduleOrThrowOnError([work_ns] { spin(work_ns); });
            pool.wait();
            return query_threads;
        });
    }

    if (state.thread_index() == 0)
    {
        if (run.ok)
            run.report(state, cpus, static_cast<double>(query_threads * schedulers));
        else
            state.SkipWithError(run.error);
        limitCpus(0);
    }
}

void freePoolArgs(benchmark::Benchmark * b)
{
    b->ArgNames({"cpus", "pool_size", "batch", "work_ns"});
    for (int64_t cpus : {4, 16})
        for (int64_t pool_size : {16, 256})
            for (int64_t work_ns : {0, 10'000})
                b->Args({cpus, pool_size, 16, work_ns});
}

void queryPoolsArgs(benchmark::Benchmark * b)
{
    b->ArgNames({"cpus", "query_threads", "work_ns"});
    for (int64_t cpus : {4, 16})
        for (int64_t query_threads : {16, 58})
            for (int64_t work_ns : {0, 50'000})
                b->Args({cpus, query_threads, work_ns});
}

}

BENCHMARK(FreePool)->Apply(freePoolArgs)->ThreadRange(1, 64, 4)->Iterations(1)->UseManualTime();
BENCHMARK(QueryPools)->Apply(queryPoolsArgs)->ThreadRange(1, 64, 4)->Iterations(1)->UseManualTime();
