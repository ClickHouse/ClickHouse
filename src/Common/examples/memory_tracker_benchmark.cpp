/// Benchmark of CurrentMemoryTracker::{alloc,free} under a 4-level nested
/// MemoryTracker hierarchy (Global -> User -> Process -> Thread).
///
/// Usage:
///   clickhouse-examples memory_tracker_benchmark
///       [--threads N] [--ops N] [--size N]
///       [--mode pair|alloc-only|free-only|new-delete]
///
/// Defaults: --threads=hardware_concurrency, --ops=1_000_000, --size=64,
/// --mode=pair (each iteration is an alloc/free pair so the per-thread
/// accumulator oscillates around zero).
///
/// `new-delete` mode does real `new char[size]` / `delete[]` per iteration —
/// pays jemalloc cost on top of the tracker. Diff against `pair` to isolate
/// the allocator-vs-tracker share.
///
/// Reports per-thread and aggregate ops/sec, plus ns/op.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadStatus.h>

using namespace DB;

namespace
{

enum class Mode
{
    Pair,
    AllocOnly,
    FreeOnly,
    NewDelete,
};

/// Defeat the optimiser without touching the page (we want allocator+tracker
/// cost, not the cost of a page fault on first write).
__attribute__((noinline)) void clobber(void * p)
{
    __asm__ volatile ("" : : "r"(p) : "memory");
}

struct Options
{
    int threads = static_cast<int>(std::thread::hardware_concurrency());
    Int64 ops = 1'000'000;
    Int64 size = 64;
    Mode mode = Mode::Pair;
};

bool parseArgs(int argc, char ** argv, Options & opts)
{
    for (int i = 1; i < argc; ++i)
    {
        std::string_view a{argv[i]};
        auto next = [&]() -> const char *
        {
            if (i + 1 >= argc)
                return nullptr;
            return argv[++i];
        };
        if (a == "--threads")
        {
            if (const auto * v = next()) opts.threads = static_cast<int>(std::strtol(v, nullptr, 10));
            else return false;
        }
        else if (a == "--ops")
        {
            if (const auto * v = next()) opts.ops = std::strtoll(v, nullptr, 10);
            else return false;
        }
        else if (a == "--size")
        {
            if (const auto * v = next()) opts.size = std::strtoll(v, nullptr, 10);
            else return false;
        }
        else if (a == "--mode")
        {
            const char * v = next();
            if (!v) return false;
            std::string_view sv{v};
            if      (sv == "pair")       opts.mode = Mode::Pair;
            else if (sv == "alloc-only") opts.mode = Mode::AllocOnly;
            else if (sv == "free-only")  opts.mode = Mode::FreeOnly;
            else if (sv == "new-delete") opts.mode = Mode::NewDelete;
            else return false;
        }
        else if (a == "--help" || a == "-h")
        {
            return false;
        }
        else
        {
            std::cerr << "Unknown argument: " << a << '\n';
            return false;
        }
    }
    if (opts.threads <= 0) opts.threads = 1;
    if (opts.ops <= 0) opts.ops = 1;
    if (opts.size <= 0) opts.size = 1;
    return true;
}

void printHelp()
{
    std::cerr <<
        "Usage: memory_tracker_benchmark [--threads N] [--ops N] [--size N]\n"
        "                                [--mode pair|alloc-only|free-only|new-delete]\n"
        "\n"
        "Each worker thread executes --ops iterations against a 4-level\n"
        "MemoryTracker hierarchy (Global -> User -> Process -> Thread).\n"
        "\n"
        "Modes:\n"
        "  pair        balanced CurrentMemoryTracker::{alloc,free}(size) — pure tracker cost\n"
        "  alloc-only  N tracker allocs, then one batched free — alloc-side flushes\n"
        "  free-only   one alloc, then N tracker frees — free-side flushes\n"
        "  new-delete  real new char[size] / delete[] — adds jemalloc on top of pair\n";
}

void runWorker(
    MemoryTracker * process_tracker,
    const Options & opts,
    Int64 & out_ops_done,
    UInt64 & out_elapsed_ns)
{
    /// Construct ThreadStatus on the worker stack so `current_thread` is
    /// non-null and `CurrentMemoryTracker` exercises the per-thread +
    /// per-CPU code paths. Re-parent the thread tracker to the process
    /// (query) level so the chain is Thread -> Process -> User -> Global.
    ThreadStatus thread_status;
    CurrentThread::get().memory_tracker.setParent(process_tracker);

    Stopwatch sw;

    Int64 ops = opts.ops;
    Int64 size = opts.size;
    Int64 done = 0;

    switch (opts.mode)
    {
        case Mode::Pair:
            for (Int64 i = 0; i < ops; ++i)
            {
                std::ignore = CurrentMemoryTracker::allocNoThrow(size);
                std::ignore = CurrentMemoryTracker::free(size);
                ++done;
            }
            break;
        case Mode::AllocOnly:
            for (Int64 i = 0; i < ops; ++i)
            {
                std::ignore = CurrentMemoryTracker::allocNoThrow(size);
                ++done;
            }
            /// Free everything we allocated so the trackers go back to zero
            /// before destruction (and the result is comparable).
            std::ignore = CurrentMemoryTracker::free(size * ops);
            break;
        case Mode::FreeOnly:
            std::ignore = CurrentMemoryTracker::allocNoThrow(size * ops);
            for (Int64 i = 0; i < ops; ++i)
            {
                std::ignore = CurrentMemoryTracker::free(size);
                ++done;
            }
            break;
        case Mode::NewDelete:
            for (Int64 i = 0; i < ops; ++i)
            {
                /// Goes through operator new -> Memory::trackMemory ->
                /// CurrentMemoryTracker::allocNoThrow -> jemalloc;
                /// delete[] mirrors with Memory::untrackMemory + jefree.
                /// Use volatile asm to prevent the compiler from eliding the
                /// pair, but skip touching the memory so we measure the
                /// allocator + tracker cost, not page-fault cost.
                char * ptr = new char[static_cast<size_t>(size)];
                clobber(ptr);
                delete[] ptr;
                ++done;
            }
            break;
    }

    out_elapsed_ns = sw.elapsedNanoseconds();
    out_ops_done = done;
}

}

int mainEntryExampleMemoryTrackerBenchmark(int argc, char ** argv)
{
    Options opts;
    if (!parseArgs(argc, argv, opts))
    {
        printHelp();
        return 1;
    }

    /// Make sure the global tracker (and the budget machinery it triggers
    /// on first use) is initialised on the main thread before workers run.
    MainThreadStatus::getInstance();

    /// Build the hierarchy above the workers.
    /// Global is `total_memory_tracker` (defined in MemoryTracker.cpp).
    MemoryTracker user_tracker(&total_memory_tracker, VariableContext::User);
    user_tracker.setDescription("benchmark-user");
    MemoryTracker process_tracker(&user_tracker, VariableContext::Process);
    process_tracker.setDescription("benchmark-process");

    std::cout << "Threads: "    << opts.threads
              << ", ops/thread: " << opts.ops
              << ", size: "     << opts.size
              << ", mode: "
              << (opts.mode == Mode::Pair       ? "pair"       :
                  opts.mode == Mode::AllocOnly  ? "alloc-only" :
                  opts.mode == Mode::FreeOnly   ? "free-only"  : "new-delete")
              << "\n";

    std::vector<std::thread> workers;
    workers.reserve(opts.threads);
    std::vector<Int64> ops_done(opts.threads, 0);
    std::vector<UInt64> elapsed_ns(opts.threads, 0);

    std::atomic<bool> start{false};

    for (int t = 0; t < opts.threads; ++t)
    {
        workers.emplace_back([&, t]()
        {
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();
            runWorker(&process_tracker, opts, ops_done[t], elapsed_ns[t]);
        });
    }

    Stopwatch wall;
    start.store(true, std::memory_order_release);
    for (auto & w : workers)
        w.join();
    UInt64 wall_ns = wall.elapsedNanoseconds();

    Int64  total_ops = 0;
    UInt64 max_thread_ns = 0;
    UInt64 sum_thread_ns = 0;
    for (int t = 0; t < opts.threads; ++t)
    {
        total_ops += ops_done[t];
        sum_thread_ns += elapsed_ns[t];
        max_thread_ns = std::max(max_thread_ns, elapsed_ns[t]);
    }

    auto ns_per_op_per_thread = opts.ops > 0 ? static_cast<double>(sum_thread_ns) / static_cast<double>(opts.threads) / static_cast<double>(opts.ops) : 0.0;
    auto wall_throughput = wall_ns > 0 ? static_cast<double>(total_ops) * 1e9 / static_cast<double>(wall_ns) : 0.0;
    auto wall_ms = static_cast<double>(wall_ns) / 1e6;

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Wall:           " << wall_ms << " ms\n";
    std::cout << "Total ops:      " << total_ops << "\n";
    std::cout << "Throughput:     " << (wall_throughput / 1e6) << " Mops/sec (wall)\n";
    std::cout << "Per-thread:     " << ns_per_op_per_thread << " ns/op (avg across threads)\n";
    std::cout << "Max thread ns:  " << max_thread_ns << " ns ("
              << (static_cast<double>(max_thread_ns) / static_cast<double>(opts.ops)) << " ns/op)\n";
    std::cout << "Tracker amount: " << total_memory_tracker.get() << " bytes\n";

    return 0;
}
