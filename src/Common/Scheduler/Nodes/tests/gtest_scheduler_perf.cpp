#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <print>
#include <thread>
#include <vector>

#include <Common/Scheduler/ResourceGuard.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/WorkloadResourceManager.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>

#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ParserCreateWorkloadQuery.h>
#include <Parsers/ParserCreateResourceQuery.h>
#include <Parsers/parseQuery.h>

/*
 * Scheduler request-path microbenchmark (finding #7: cost of the per-request accounting RMWs).
 *
 * Measures throughput (requests/second) of the shared per-request scheduler path
 * (`RequestQueue::enqueueRequest` -> scheduler-thread `dequeueRequest` -> `ResourceGuard::finish`),
 * which carries the per-query attained/vruntime accounting atomic RMWs. A real
 * `WorkloadResourceManager` runs one resource + one `all` workload + its scheduler thread; requests
 * come not from queries but from N stress threads in a closed loop, all sharing ONE classifier so
 * they contend on the same per-query state cache line (worst case for the accounting atomics).
 *
 * Modes (env):
 *   default            short functional check (small, fast, CI-safe) — asserts the loop progresses.
 *   SCHED_PERF_STRESS=1  real measurement — long run, many threads, prints RPS per algorithm/load.
 *     SCHED_PERF_THREADS=<n>  thread count (default 2*cores)
 *     SCHED_PERF_MS=<ms>      per-round duration (default 2000)
 *     SCHED_PERF_ROUNDS=<r>   rounds per case (default 5)
 *
 * A/B for the RMWs: build twice — as-is, and with the dequeue/finish RMWs compiled out — compare RPS.
 */

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Minimal in-memory workload entity storage that turns CREATE/DROP RESOURCE/WORKLOAD SQL into
/// entity operations, so a real WorkloadResourceManager builds the resource + scheduler thread.
class BenchStorage : public WorkloadEntityStorageBase
{
public:
    BenchStorage() : WorkloadEntityStorageBase(Context::getGlobalContextInstance()) {}

    std::string_view getName() const override { return "bench"; }

    void executeQuery(const String & query)
    {
        ParserCreateWorkloadQuery create_workload_p;
        ParserCreateResourceQuery create_resource_p;

        auto parse = [&](IParser & parser) -> ASTPtr
        {
            String error;
            const char * begin = query.data();
            return tryParseQuery(parser, begin, query.data() + query.size(), error,
                false, "", false, 0,
                DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS, true);
        };

        if (ASTPtr create_workload = parse(create_workload_p))
        {
            auto & parsed = create_workload->as<ASTCreateWorkloadQuery &>();
            storeEntity(nullptr, WorkloadEntityType::Workload, parsed.getWorkloadName(), create_workload,
                /*throw_if_exists=*/ !parsed.if_not_exists && !parsed.or_replace,
                /*replace_if_exists=*/ parsed.or_replace, {});
        }
        else if (ASTPtr create_resource = parse(create_resource_p))
        {
            auto & parsed = create_resource->as<ASTCreateResourceQuery &>();
            storeEntity(nullptr, WorkloadEntityType::Resource, parsed.getResourceName(), create_resource,
                /*throw_if_exists=*/ !parsed.if_not_exists && !parsed.or_replace,
                /*replace_if_exists=*/ parsed.or_replace, {});
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "BenchStorage: unsupported query: {}", query);
    }

private:
    OperationResult storeEntityImpl(const ContextPtr &, WorkloadEntityType, const String &, ASTPtr, bool, bool, const Settings &) override
    {
        return OperationResult::Ok;
    }
    OperationResult removeEntityImpl(const ContextPtr &, WorkloadEntityType, const String &, bool) override
    {
        return OperationResult::Ok;
    }
};

struct PerfEnv
{
    bool stress = false;
    size_t threads = 0;
    uint64_t duration_ms = 0;
    size_t rounds = 0;
    String only_sched;  // SCHED_PERF_SCHED: restrict to one algorithm (e.g. profiling a single scheduler)
    String only_load;   // SCHED_PERF_LOAD:  restrict to one load ("cpu_like" / "io_like")

    PerfEnv()
    {
        stress = std::getenv("SCHED_PERF_STRESS") != nullptr; // NOLINT(concurrency-mt-unsafe)
        const size_t cores = std::max<size_t>(1, std::thread::hardware_concurrency());

        if (const char * t = std::getenv("SCHED_PERF_THREADS")) // NOLINT(concurrency-mt-unsafe)
            threads = std::stoul(t);
        else
            threads = stress ? 2 * cores : 2;

        if (const char * m = std::getenv("SCHED_PERF_MS")) // NOLINT(concurrency-mt-unsafe)
            duration_ms = std::stoull(m);
        else
            duration_ms = stress ? 2000 : 50;

        if (const char * r = std::getenv("SCHED_PERF_ROUNDS")) // NOLINT(concurrency-mt-unsafe)
            rounds = std::stoul(r);
        else
            rounds = stress ? 5 : 1;

        if (const char * s = std::getenv("SCHED_PERF_SCHED")) // NOLINT(concurrency-mt-unsafe)
            only_sched = s;
        if (const char * l = std::getenv("SCHED_PERF_LOAD")) // NOLINT(concurrency-mt-unsafe)
            only_load = l;
    }
};

/// N threads share one classifier/link; each loops enqueue -> grant -> finish. Returns requests/sec.
double measureRps(WorkloadResourceManager & manager, const String & workload, const String & resource,
                  ResourceCost cost, size_t threads, uint64_t duration_ms)
{
    ClassifierPtr classifier = manager.acquire(workload, {});
    ResourceLink link = classifier->get(resource);

    std::atomic<bool> go{false};
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> completed{0};

    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (size_t i = 0; i < threads; ++i)
    {
        workers.emplace_back([&, classifier]
        {
            while (!go.load(std::memory_order_acquire)) { }
            uint64_t local = 0;
            while (!stop.load(std::memory_order_relaxed))
            {
                ResourceGuard g(ResourceGuard::Metrics::getIOWrite(), link, cost); // enqueue + wait grant
                g.unlock(cost);                                                    // finish (real == estimate)
                ++local;
            }
            completed.fetch_add(local, std::memory_order_relaxed);
        });
    }

    go.store(true, std::memory_order_release);
    const auto t0 = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
    stop.store(true, std::memory_order_relaxed);
    for (auto & w : workers)
        w.join();
    const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    return secs > 0 ? static_cast<double>(completed.load()) / secs : 0.0;
}

void runLoad(const PerfEnv & env, const char * load, const String & resource_ddl, const String & resource, ResourceCost cost)
{
    if (!env.only_load.empty() && env.only_load != load)
        return;
    BenchStorage storage;
    auto manager = std::make_shared<WorkloadResourceManager>(
        std::shared_ptr<IWorkloadEntityStorage>(&storage, [](IWorkloadEntityStorage *) {}));
    storage.executeQuery(resource_ddl);
    storage.executeQuery("CREATE WORKLOAD all SETTINGS scheduler = 'fifo'");
    for (const char * sched : {"fifo", "fair", "las", "priority"})
    {
        if (!env.only_sched.empty() && env.only_sched != sched)
            continue;
        storage.executeQuery(fmt::format("CREATE OR REPLACE WORKLOAD all SETTINGS scheduler = '{}'", sched));
        for (size_t r = 0; r < env.rounds; ++r)
        {
            double rps = measureRps(*manager, "all", resource, cost, env.threads, env.duration_ms);
            EXPECT_GT(rps, 0.0);
            if (env.stress)
                std::println("SCHED_PERF\tload={}\tscheduler={}\tthreads={}\tround={}\trps={:.0f}",
                             load, sched, env.threads, r, rps);
        }
    }
}

} // namespace

/// CPU-lease-like: tiny fixed cost, maximum request churn (approximates high-frequency lease renewals).
TEST(SchedulerPerf, CpuLeaseLikeAllAlgorithms)
{
    PerfEnv env;
    runLoad(env, "cpu_like", "CREATE RESOURCE cpu_like (WRITE DISK cpu_like_d, READ DISK cpu_like_d)", "cpu_like", /*cost=*/1);
}

/// IO-like: byte-sized cost per request.
TEST(SchedulerPerf, IoLikeAllAlgorithms)
{
    PerfEnv env;
    runLoad(env, "io_like", "CREATE RESOURCE io_like (WRITE DISK io_like_d, READ DISK io_like_d)", "io_like", /*cost=*/1 << 20);
}
