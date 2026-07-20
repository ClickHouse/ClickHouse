/// Micro-benchmark comparing per-CPU counter increments via rseq vs
/// `__atomic_add_fetch(..., __ATOMIC_RELAXED)`, plus a shared-counter baseline.
///
/// In the per-CPU modes each worker increments the counter for the CPU it is
/// currently running on; cache lines never bounce (each CPU's counter sits in
/// its own cache-aligned slot and is touched by at most one CPU at a time), so
/// the atomic/rseq difference isolates the bus-locked `xadd` vs. the rseq
/// compare-and-store. In the `shared` mode every worker increments the same
/// slot — the contended single-counter layout that per-CPU sharding replaces.
///
/// Usage:
///   clickhouse-examples rseq_vs_atomic_benchmark [--threads N] [--ops N]
///
/// Possible results:
///
///     threads=64 ops=50000000 slot_count=64 rseq_supported=yes
///
///     shared: 954.86 ns/op (wall 50.71 s)
///     atomic: 3.97 ns/op (wall 0.22 s)
///     rseq:   2.86 ns/op (wall 0.16 s)
///     speedup (atomic / rseq):   1.39x
///     speedup (shared / atomic): 240.38x
///


#include <atomic>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <thread>
#include <vector>

#include <sched.h>
#include <unistd.h>

#include <Common/CacheLine.h>
#include <Common/Stopwatch.h>

#include <Examples/clickhouse_examples.h>

#include "config.h"

#if USE_LIBRSEQ
#    include <rseq/rseq.h>
#endif

using namespace DB;

namespace
{

struct alignas(CH_CACHE_LINE_SIZE) Slot
{
    UInt64 value = 0;
};

enum class Mode { Shared, Atomic, RSeq };

struct Options
{
    int threads = static_cast<int>(std::thread::hardware_concurrency());
    Int64 ops = 50'000'000;
};

void usage()
{
    std::cerr << "Usage: rseq_vs_atomic_benchmark [--threads N] [--ops N]\n";
}

bool parseNumber(const char * s, Int64 & out)
{
    if (!s)
        return false;
    const char * end = s + strlen(s);
    auto [parsed_end, ec] = std::from_chars(s, end, out);
    return ec == std::errc() && parsed_end == end;
}

bool parseOptions(int argc, char ** argv, Options & opts)
{
    for (int i = 1; i < argc; ++i)
    {
        std::string_view a = argv[i];
        auto next = [&]() -> const char * { return (i + 1 < argc) ? argv[++i] : nullptr; };
        Int64 value = 0;
        if (!parseNumber(next(), value))
        {
            usage();
            return false;
        }
        if (a == "--threads" && value <= std::numeric_limits<int>::max())
            opts.threads = static_cast<int>(value);
        else if (a == "--ops")
            opts.ops = value;
        else
        {
            usage();
            return false;
        }
    }
    return opts.threads > 0 && opts.ops > 0;
}

ALWAYS_INLINE void atomicBump(Slot * slots, int slot_count)
{
    int cpu = ::sched_getcpu();
    /// `_SC_NPROCESSORS_CONF` is a count, not an upper bound for logical CPU ids, so on
    /// sparse numbering (or a `sched_getcpu` error) the id may fall outside the slot array.
    /// Fall back to slot 0 — one increment per iteration, like `ProfileEvents::Counters::fetchAdd`.
    if (unlikely(static_cast<unsigned>(cpu) >= static_cast<unsigned>(slot_count)))
        cpu = 0;
    __atomic_add_fetch(&slots[cpu].value, 1, __ATOMIC_RELAXED);
}

/// All threads hammer one cache line — no CPU-id lookup, like an unsharded counter.
ALWAYS_INLINE void sharedBump(Slot * slots)
{
    __atomic_add_fetch(&slots[0].value, 1, __ATOMIC_RELAXED);
}

#if USE_LIBRSEQ
ALWAYS_INLINE void rseqBump(Slot * slots, int slot_count)
{
    int cpu = static_cast<int>(rseq_cpu_start());

    while (true)
    {
        /// The rseq critical section only commits while running on `cpu`, so an out-of-range id
        /// cannot be redirected to another slot within it. Fall back to an atomic increment on
        /// slot 0 instead — one increment per iteration, like `ProfileEvents::Counters::fetchAdd`.
        if (unlikely(static_cast<unsigned>(cpu) >= static_cast<unsigned>(slot_count)))
        {
            __atomic_add_fetch(&slots[0].value, 1, __ATOMIC_RELAXED);
            return;
        }

        UInt64 * counter = &slots[cpu].value;
        UInt64 current = __atomic_load_n(counter, __ATOMIC_RELAXED);
        UInt64 next = current + 1;
        int r = rseq_load_cbne_store__ptr(
            RSEQ_MO_RELAXED,
            RSEQ_PERCPU_CPU_ID,
            reinterpret_cast<intptr_t *>(counter),
            static_cast<intptr_t>(current),
            static_cast<intptr_t>(next),
            cpu);
        if (likely(r == 0))
            return;
        cpu = static_cast<int>(rseq_cpu_start());
    }
}
#endif

void runMode(Mode mode, const Options & opts, Slot * slots, int slot_count, double & ns_per_op_out, double & wall_s_out)
{
    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    std::vector<UInt64> elapsed_ns(opts.threads, 0);
    workers.reserve(opts.threads);

    for (int t = 0; t < opts.threads; ++t)
    {
        workers.emplace_back([&, t]()
        {
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();

            Stopwatch sw;
            switch (mode)
            {
                case Mode::Shared:
                    for (Int64 i = 0; i < opts.ops; ++i)
                        sharedBump(slots);
                    break;
                case Mode::Atomic:
                    for (Int64 i = 0; i < opts.ops; ++i)
                        atomicBump(slots, slot_count);
                    break;
                case Mode::RSeq:
#if USE_LIBRSEQ
                    for (Int64 i = 0; i < opts.ops; ++i)
                        rseqBump(slots, slot_count);
#endif
                    break;
            }
            elapsed_ns[t] = sw.elapsedNanoseconds();
        });
    }

    Stopwatch wall;
    start.store(true, std::memory_order_release);
    for (auto & w : workers)
        w.join();

    UInt64 sum_ns = 0;
    for (auto x : elapsed_ns) sum_ns += x;

    ns_per_op_out = static_cast<double>(sum_ns) / static_cast<double>(opts.threads) / static_cast<double>(opts.ops);
    wall_s_out = static_cast<double>(wall.elapsedNanoseconds()) / 1e9;
}

}

int mainEntryExampleRSeqVsAtomicBenchmark(int argc, char ** argv)
{
    Options opts;
    if (!parseOptions(argc, argv, opts))
        return 1;

#if USE_LIBRSEQ
    /// With glibc >= 2.35 every thread is registered by libc; librseq only adopts that
    /// registration here, so one process-wide init is enough for the workers below.
    const bool rseq_supported = rseq_init() == RSEQ_INIT_OK && rseq_size > 0;
#else
    const bool rseq_supported = false;
#endif

    int slot_count = 0;
#if USE_LIBRSEQ
    Int64 n = ::sysconf(_SC_NPROCESSORS_CONF);
    slot_count = n > 0 ? static_cast<int>(n) : 0;
#endif
    if (slot_count == 0)
    {
        std::cerr << "Could not determine CPU count\n";
        return 1;
    }

    std::vector<Slot> slots(slot_count);

    std::cout << "threads=" << opts.threads
              << " ops=" << opts.ops
              << " slot_count=" << slot_count
              << " rseq_supported=" << (rseq_supported ? "yes" : "no")
              << "\n\n";

    double shared_ns = 0;
    double shared_wall_s = 0;
    runMode(Mode::Shared, opts, slots.data(), slot_count, shared_ns, shared_wall_s);
    std::cout << std::fixed << std::setprecision(2)
              << "shared: " << shared_ns << " ns/op (wall " << shared_wall_s << " s)\n";

    std::fill(slots.begin(), slots.end(), Slot{});
    double atomic_ns = 0;
    double atomic_wall_s = 0;
    runMode(Mode::Atomic, opts, slots.data(), slot_count, atomic_ns, atomic_wall_s);
    std::cout << "atomic: " << atomic_ns << " ns/op (wall " << atomic_wall_s << " s)\n";

    if (rseq_supported)
    {
        std::fill(slots.begin(), slots.end(), Slot{});
        double rseq_ns = 0;
        double rseq_wall_s = 0;
        runMode(Mode::RSeq, opts, slots.data(), slot_count, rseq_ns, rseq_wall_s);
        std::cout << "rseq:   " << rseq_ns << " ns/op (wall " << rseq_wall_s << " s)\n";
        if (rseq_ns > 0)
            std::cout << "speedup (atomic / rseq):   " << (atomic_ns / rseq_ns) << "x\n";
    }
    else
    {
        std::cout << "rseq:   <unavailable>\n";
    }

    if (atomic_ns > 0)
        std::cout << "speedup (shared / atomic): " << (shared_ns / atomic_ns) << "x\n";

    return 0;
}
