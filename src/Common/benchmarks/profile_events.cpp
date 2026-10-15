/// Benchmark for `ProfileEvents::Counters::increment` on the realistic
/// thread → thread-group → user → global propagation chain
/// (matches `ThreadStatusExt.cpp:353` and `ProcessList.cpp:278`).
///
/// Setup:
///   - 500 worker threads
///   - 50 thread-group `Counters` (`VariableContext::Process`)
///   - 1 user `Counters` (`VariableContext::User`, parent = `global_counters`)
///   - groups round-robin onto users → ~10 groups per user
///   - threads round-robin onto groups → ~10 threads per group
///
/// What this stresses:
///   - Thread shard: uncontended (single writer).
///   - Group shard: ~10 threads → cache-line ping-pong.
///   - User shard: ~100 threads × 10 groups → heavy ping-pong.
///   - Global shard: all 500 threads, but sharded by CPU (atomic per-CPU row).
///
/// The reader variants add one thread taking the full `global_counters`
/// snapshot (`END` events × per-CPU rows), as `metric_log` does, to measure the
/// read-side interference on the writer hot path:
///   - `ConcurrentRead`: snapshots back-to-back (worst case).
///   - `PeriodicRead`: snapshots, then waits `read_us` between scans (state.range(1)),
///     i.e. the realistic "once in a while" exporter cadence.
///   - `ReadLatency`: throughput hides per-scrape stalls. Here one pinned victim times
///     each increment individually while background writers and a pinned reader run,
///     reporting the latency tail (p50/p99/p999/p9999, with OS-descheduling outliers
///     counted separately). The victim has a private chain, so the only cross-core traffic on its
///     path is the reader pulling its global per-CPU row — that coherence miss is the tail.

#include <Common/PerCPU.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <cmath>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <thread>
#include <vector>

#if defined(OS_LINUX)
#include <pthread.h>
#include <sched.h>
#endif

namespace
{

constexpr size_t NUM_THREADS = 500;
constexpr size_t NUM_GROUPS = 50;
constexpr size_t NUM_USERS = 1;

/// Pick a stable event id near the start of the table; any event would do,
/// the sharded write doesn't depend on which one.
const ProfileEvents::Event TEST_EVENT{0};

/// Shared control state polled by worker threads every iteration through reference captures.
/// Must occupy its own cache line: as a plain stack local it can land on the same line as
/// locals the measuring thread writes each iteration (the stack's 64-byte phase depends on
/// argv/env size), and the resulting false sharing adds ~500 ns to every measured op.
struct alignas(DB::CH_CACHE_LINE_SIZE) PaddedAtomicBool
{
    std::atomic<bool> value{false};
};

struct alignas(DB::CH_CACHE_LINE_SIZE) PaddedAtomicUInt64
{
    std::atomic<uint64_t> value{0};
};

/// The realistic propagation chain: `num_threads` thread shards round-robin onto
/// `NUM_GROUPS` process shards, themselves round-robin onto `NUM_USERS` user shards,
/// all rooted at `global_counters`.
struct Chain
{
    std::vector<std::unique_ptr<ProfileEvents::Counters>> users;
    std::vector<std::unique_ptr<ProfileEvents::Counters>> groups;
    std::vector<std::unique_ptr<ProfileEvents::Counters>> threads;
};

Chain buildChain(size_t num_threads)
{
    Chain c;
    c.users.reserve(NUM_USERS);
    for (size_t u = 0; u < NUM_USERS; ++u)
        c.users.push_back(std::make_unique<ProfileEvents::Counters>(VariableContext::User, &ProfileEvents::global_counters));

    c.groups.reserve(NUM_GROUPS);
    for (size_t g = 0; g < NUM_GROUPS; ++g)
        c.groups.push_back(std::make_unique<ProfileEvents::Counters>(VariableContext::Process, c.users[g % NUM_USERS].get()));

    c.threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
        c.threads.push_back(std::make_unique<ProfileEvents::Counters>(VariableContext::Thread, c.groups[t % NUM_GROUPS].get()));

    return c;
}

#if defined(OS_LINUX)
bool pinToCPU(unsigned cpu)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
}

/// Restores the calling thread's affinity on scope exit. The benchmark driver thread runs every
/// benchmark in the binary, so a leaked pin would confine all subsequent ones to a single core.
struct AffinityRestorer
{
    cpu_set_t saved;
    bool valid = false;
    AffinityRestorer() { valid = pthread_getaffinity_np(pthread_self(), sizeof(saved), &saved) == 0; }
    ~AffinityRestorer()
    {
        if (valid)
            pthread_setaffinity_np(pthread_self(), sizeof(saved), &saved);
    }
};
#else
bool pinToCPU(unsigned) { return false; }
struct AffinityRestorer {};
#endif

/// `reader_period_us` < 0 disables the reader; 0 snapshots back-to-back;
/// > 0 waits that many microseconds between snapshots.
void run(benchmark::State & state, int64_t reader_period_us)
{
    const bool with_reader = reader_period_us >= 0;
    /// Iterations performed by each thread per outer step.
    const size_t inner_iters = static_cast<size_t>(state.range(0));

    Chain chain = buildChain(NUM_THREADS);
    auto & thread_counters = chain.threads;

    PaddedAtomicBool stop;
    std::atomic<uint64_t> total_ops{0};

    /// `metric_log`-style reader: sums every per-CPU row of every event.
    PaddedAtomicBool stop_reader;
    PaddedAtomicUInt64 global_scans;
    std::thread reader;
    if (with_reader)
    {
        reader = std::thread([&]()
        {
            const size_t end = ProfileEvents::end();
            while (!stop_reader.value.load(std::memory_order_relaxed))
            {
                ProfileEvents::Count acc = 0;
                for (size_t i = 0; i < end; ++i)
                    acc += ProfileEvents::global_counters[ProfileEvents::Event(i)];
                benchmark::DoNotOptimize(acc);
                global_scans.value.fetch_add(1, std::memory_order_relaxed);
                if (reader_period_us > 0)
                    std::this_thread::sleep_for(std::chrono::microseconds(reader_period_us));
            }
        });
    }

    /// +1 for the benchmark driver thread, which arrives at the barrier
    /// both after starting the workers and before joining them — that gives
    /// us a well-defined window where all workers are simultaneously active.
    std::barrier<> sync_start(NUM_THREADS + 1);
    std::barrier<> sync_end(NUM_THREADS + 1);

    std::vector<std::thread> workers;
    workers.reserve(NUM_THREADS);
    for (size_t t = 0; t < NUM_THREADS; ++t)
    {
        workers.emplace_back([&, t]()
        {
            auto & cnt = *thread_counters[t];
            while (true)
            {
                sync_start.arrive_and_wait();
                if (stop.value.load(std::memory_order_relaxed))
                    return;
                for (size_t i = 0; i < inner_iters; ++i)
                    cnt.increment(TEST_EVENT, 1);
                sync_end.arrive_and_wait();
            }
        });
    }

    for (auto _ : state)
    {
        sync_start.arrive_and_wait();
        sync_end.arrive_and_wait();
        total_ops.fetch_add(static_cast<uint64_t>(NUM_THREADS) * inner_iters,
                            std::memory_order_relaxed);
    }

    stop.value.store(true, std::memory_order_relaxed);
    sync_start.arrive_and_wait();
    for (auto & w : workers)
        w.join();

    if (with_reader)
    {
        stop_reader.value.store(true, std::memory_order_relaxed);
        reader.join();
        state.counters["global_scans"] = static_cast<double>(global_scans.value.load());
        state.counters["read_us"] = static_cast<double>(reader_period_us);
    }

    state.SetItemsProcessed(static_cast<int64_t>(total_ops.load()));
    /// Inverse rate: wall-time ns per increment, summed across threads.
    state.counters["ns/inc"] = benchmark::Counter(
        static_cast<double>(total_ops.load()),
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.counters["threads"] = NUM_THREADS;
    state.counters["groups"] = NUM_GROUPS;
    state.counters["users"] = NUM_USERS;
}

void BM_ProfileEvents(benchmark::State & state) { run(state, /*reader_period_us=*/ -1); }
void BM_ProfileEventsConcurrentRead(benchmark::State & state) { run(state, /*reader_period_us=*/ 0); }
void BM_ProfileEventsPeriodicRead(benchmark::State & state) { run(state, /*reader_period_us=*/ state.range(1)); }

/// Fixed-resolution (1ns) latency histogram over the recorded per-increment samples.
struct LatencyHist
{
    static constexpr size_t N = 4096;
    std::array<uint64_t, N> buckets{};
    uint64_t overflow = 0;
    uint64_t max_ns = 0;
    uint64_t count = 0;

    void add(uint64_t ns)
    {
        if (ns < N)
            ++buckets[ns];
        else
            ++overflow;
        max_ns = std::max(ns, max_ns);
        ++count;
    }

    /// Percentile over the in-histogram samples only (nearest-rank): overflow samples are OS
    /// descheduling (reported separately as `descheduled`), not coherence, and must not be able
    /// to turn a tail percentile into a preemption outlier.
    double percentile(double p) const
    {
        const uint64_t in_range = count - overflow;
        if (!in_range)
            return 0;
        const uint64_t target = std::max<uint64_t>(static_cast<uint64_t>(std::ceil(p * static_cast<double>(in_range))), 1);
        uint64_t cum = 0;
        for (size_t i = 0; i < N; ++i)
        {
            cum += buckets[i];
            if (cum >= target)
                return static_cast<double>(i);
        }
        return static_cast<double>(N - 1);
    }
};

void BM_ProfileEventsReadLatency(benchmark::State & state)
{
    const int64_t reader_period_us = state.range(0);
    const size_t batch = static_cast<size_t>(state.range(1));
    const bool with_reader = reader_period_us >= 0;

    const uint32_t num_cpus = PerCPU::getNumCPUs();
    /// Cores 0 and 1 are the victim and the reader. Background writers use only the lower
    /// half of the logical CPUs (assuming 2-way SMT with siblings in the upper half; on
    /// core-paired sibling enumerations the victim and reader may share a core and the tail
    /// then includes SMT sharing), leaving every used core's sibling idle so the victim is
    /// rarely descheduled - otherwise OS preemption (ms-scale) swamps the ~100ns coherence
    /// signal. Still leaves enough hot per-CPU rows for the scrape to sum.
    if (num_cpus < 6)
    {
        state.SkipWithError("requires at least 6 CPUs (distinct cores for victim, reader and background writers)");
        return;
    }
    const size_t usable = num_cpus / 2;
    const size_t num_bg = usable - 2;
    Chain bg = buildChain(num_bg);

    /// Pin the driver (victim) thread before spawning workers; restore on exit so the pin does
    /// not leak into subsequently run benchmarks.
    [[maybe_unused]] AffinityRestorer affinity_restorer;
    if (!pinToCPU(0))
    {
        state.SkipWithError("cannot pin the victim thread");
        return;
    }

    /// Private victim chain: thread/group/user shards are single-writer, so the only
    /// cross-core traffic on the victim's path is the reader pulling its global row.
    auto victim_user = std::make_unique<ProfileEvents::Counters>(VariableContext::User, &ProfileEvents::global_counters);
    auto victim_group = std::make_unique<ProfileEvents::Counters>(VariableContext::Process, victim_user.get());
    auto victim_thread = std::make_unique<ProfileEvents::Counters>(VariableContext::Thread, victim_group.get());

    PaddedAtomicBool stop;
    PaddedAtomicBool stop_reader;
    PaddedAtomicUInt64 global_scans;

    std::vector<std::thread> bg_workers;
    bg_workers.reserve(num_bg);
    for (size_t t = 0; t < num_bg; ++t)
    {
        bg_workers.emplace_back([&, t]()
        {
            pinToCPU(static_cast<unsigned>(2 + t));
            auto & cnt = *bg.threads[t];
            while (!stop.value.load(std::memory_order_relaxed))
                cnt.increment(TEST_EVENT, 1);
        });
    }

    std::thread reader;
    if (with_reader)
    {
        reader = std::thread([&]()
        {
            pinToCPU(1);
            const size_t end = ProfileEvents::end();
            while (!stop_reader.value.load(std::memory_order_relaxed))
            {
                ProfileEvents::Count acc = 0;
                for (size_t i = 0; i < end; ++i)
                    acc += ProfileEvents::global_counters[ProfileEvents::Event(i)];
                benchmark::DoNotOptimize(acc);
                global_scans.value.fetch_add(1, std::memory_order_relaxed);
                if (reader_period_us > 0)
                    std::this_thread::sleep_for(std::chrono::microseconds(reader_period_us));
            }
        });
    }

    LatencyHist hist;
    Stopwatch sw(CLOCK_MONOTONIC);
    for (auto _ : state)
    {
        sw.restart();
        for (size_t i = 0; i < batch; ++i)
            victim_thread->increment(TEST_EVENT, 1);
        const uint64_t ns = sw.elapsedNanoseconds() / batch;
        benchmark::DoNotOptimize(ns);
        hist.add(ns);
    }

    stop.value.store(true, std::memory_order_relaxed);
    stop_reader.value.store(true, std::memory_order_relaxed);
    for (auto & w : bg_workers)
        w.join();
    if (with_reader)
        reader.join();

    state.counters["p50_ns"] = hist.percentile(0.50);
    state.counters["p99_ns"] = hist.percentile(0.99);
    state.counters["p999_ns"] = hist.percentile(0.999);
    state.counters["p9999_ns"] = hist.percentile(0.9999);
    /// Samples above the histogram (> N ns) - OS descheduling, not coherence; kept visible
    /// so a noisy run (large `descheduled`) is not mistaken for read interference.
    state.counters["descheduled"] = static_cast<double>(hist.overflow);
    state.counters["global_scans"] = static_cast<double>(global_scans.value.load());
    state.counters["read_us"] = static_cast<double>(reader_period_us);
    state.counters["bg_threads"] = static_cast<double>(num_bg);
}

/// Cost of one full `metric_log`-style read of every event, with per-CPU sharding off
/// (`Thread` level, single row) vs on (`User` level, sums `cpus` rows). No writers, so this
/// isolates the collector-side scan cost that per-CPU multiplies by the row count.
void BM_ProfileEventsReadAll(benchmark::State & state)
{
    const bool per_cpu = state.range(0) != 0;
    ProfileEvents::Counters counters(per_cpu ? VariableContext::User : VariableContext::Thread, nullptr);

    const size_t end = ProfileEvents::end();
    for (auto _ : state)
    {
        ProfileEvents::Count acc = 0;
        for (size_t e = 0; e < end; ++e)
            acc += counters[ProfileEvents::Event(e)];
        benchmark::DoNotOptimize(acc);
    }

    state.counters["events"] = static_cast<double>(end);
    state.counters["ncpus"] = static_cast<double>(PerCPU::getNumCPUs());
}

}

BENCHMARK(BM_ProfileEvents)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Unit(benchmark::kMillisecond)
    ->MeasureProcessCPUTime()
    ->UseRealTime();

BENCHMARK(BM_ProfileEventsConcurrentRead)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Unit(benchmark::kMillisecond)
    ->MeasureProcessCPUTime()
    ->UseRealTime();

/// Fixed write load, varying scrape cadence: {inner_iters, read_us}.
BENCHMARK(BM_ProfileEventsPeriodicRead)
    ->Args({100000, 100})
    ->Args({100000, 1000})
    ->Args({100000, 10000})
    ->ArgNames({"iters", "read_us"})
    ->Unit(benchmark::kMillisecond)
    ->MeasureProcessCPUTime()
    ->UseRealTime();

/// Per-increment latency tail vs scrape cadence: {read_us, batch}. read_us=-1 is the
/// reader-off baseline; compare its tail/stalls against the reader-on rows.
BENCHMARK(BM_ProfileEventsReadLatency)
    ->Args({-1, 1})
    ->Args({0, 1})
    ->Args({1000, 1})
    ->Args({1000000, 1})
    ->ArgNames({"read_us", "batch"})
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime()
    ->MinTime(2.0);

/// Full-scan read cost, sharding off vs on: per_cpu = 0 (single row) / 1 (cpus + 1 rows).
BENCHMARK(BM_ProfileEventsReadAll)
    ->Arg(0)
    ->Arg(1)
    ->ArgNames({"per_cpu"})
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK_MAIN();
