#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <Core/Settings.h>

#include <Common/EventRateMeter.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <Common/Scheduler/CPUSlotsAllocation.h>
#include <Common/Scheduler/CPULeaseAllocation.h>
#include <Common/Scheduler/Nodes/tests/ResourceTest.h>
#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>
#include <Common/Scheduler/Nodes/WorkloadResourceManager.h>
#include <Common/getNumberOfCPUCoresToUse.h>

#include <base/scope_guard.h>

#include <Interpreters/Context.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTDropWorkloadQuery.h>
#include <Parsers/ASTDropResourceQuery.h>
#include <Parsers/ParserCreateWorkloadQuery.h>
#include <Parsers/ParserCreateResourceQuery.h>
#include <Parsers/ParserDropWorkloadQuery.h>
#include <Parsers/ParserDropResourceQuery.h>

#if 0
#include <iostream>
#include <base/getThreadId.h>
#define DBG_PRINT(...) std::cout << fmt::format("\033[01;3{}m[{}] {} {} {}\033[00m {}:{}\n", 1 + getThreadId() % 8, getThreadId(), reinterpret_cast<void*>(this), fmt::format(__VA_ARGS__), __PRETTY_FUNCTION__, __FILE__, __LINE__)
#else
#include <base/defines.h>
#define DBG_PRINT(...) UNUSED(__VA_ARGS__)
#endif

using namespace DB;

namespace ProfileEvents
{
    extern const Event ConcurrencyControlUpscales;
    extern const Event ConcurrencyControlDownscales;
}

namespace CurrentMetrics
{
    extern const Metric QueryPipelineExecutorThreads;
    extern const Metric QueryPipelineExecutorThreadsActive;
    extern const Metric QueryPipelineExecutorThreadsScheduled;
    extern const Metric ConcurrencyControlAcquired;
    extern const Metric ConcurrencyControlPreempted;
}

class WorkloadEntityTestStorage : public WorkloadEntityStorageBase
{
public:
    WorkloadEntityTestStorage()
        : WorkloadEntityStorageBase(Context::getGlobalContextInstance())
    {}

    std::string_view getName() const override { return "test"; }

    void loadEntities(const Poco::Util::AbstractConfiguration & config) override
    {
        WorkloadEntityStorageBase::loadEntities(config);
    }

    void executeQuery(const String & query)
    {
        ParserCreateWorkloadQuery create_workload_p;
        ParserDropWorkloadQuery drop_workload_p;
        ParserCreateResourceQuery create_resource_p;
        ParserDropResourceQuery drop_resource_p;

        auto parse = [&] (IParser & parser)
        {
            String error;
            const char * end = query.data();
            return tryParseQuery(
                parser,
                end,
                query.data() + query.size(),
                error,
                false,
                "",
                false,
                0,
                DBMS_DEFAULT_MAX_PARSER_DEPTH,
                DBMS_DEFAULT_MAX_PARSER_BACKTRACKS,
                true);
        };

        if (ASTPtr create_workload = parse(create_workload_p))
        {
            auto & parsed = create_workload->as<ASTCreateWorkloadQuery &>();
            auto workload_name = parsed.getWorkloadName();
            bool throw_if_exists = !parsed.if_not_exists && !parsed.or_replace;
            bool replace_if_exists = parsed.or_replace;

            storeEntity(
                nullptr,
                WorkloadEntityType::Workload,
                workload_name,
                create_workload,
                throw_if_exists,
                replace_if_exists,
                {});
        }
        else if (ASTPtr create_resource = parse(create_resource_p))
        {
            auto & parsed = create_resource->as<ASTCreateResourceQuery &>();
            auto resource_name = parsed.getResourceName();
            bool throw_if_exists = !parsed.if_not_exists && !parsed.or_replace;
            bool replace_if_exists = parsed.or_replace;

            storeEntity(
                nullptr,
                WorkloadEntityType::Resource,
                resource_name,
                create_resource,
                throw_if_exists,
                replace_if_exists,
                {});
        }
        else if (ASTPtr drop_workload = parse(drop_workload_p))
        {
            auto & parsed = drop_workload->as<ASTDropWorkloadQuery &>();
            bool throw_if_not_exists = !parsed.if_exists;
            removeEntity(
                nullptr,
                WorkloadEntityType::Workload,
                parsed.workload_name,
                throw_if_not_exists);
        }
        else if (ASTPtr drop_resource = parse(drop_resource_p))
        {
            auto & parsed = drop_resource->as<ASTDropResourceQuery &>();
            bool throw_if_not_exists = !parsed.if_exists;
            removeEntity(
                nullptr,
                WorkloadEntityType::Resource,
                parsed.resource_name,
                throw_if_not_exists);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid query in WorkloadEntityTestStorage: {}", query);
    }

private:
    WorkloadEntityStorageBase::OperationResult storeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        ASTPtr create_entity_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override
    {
        UNUSED(current_context, entity_type, entity_name, create_entity_query, throw_if_exists, replace_if_exists, settings);
        return OperationResult::Ok;
    }

    WorkloadEntityStorageBase::OperationResult removeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists) override
    {
        UNUSED(current_context, entity_type, entity_name, throw_if_not_exists);
        return OperationResult::Ok;
    }
};

struct ResourceTest : ResourceTestManager<WorkloadResourceManager>
{
    WorkloadEntityTestStorage storage;

    explicit ResourceTest(size_t thread_count = 1)
        : ResourceTestManager(thread_count, DoNotInitManager)
    {
        manager = std::make_shared<WorkloadResourceManager>(storage);
    }

    void query(const String & query_str)
    {
        storage.executeQuery(query_str);
    }

    template <class Func>
    ThreadFromGlobalPool * async(Func func)
    {
        std::scoped_lock lock{threads_mutex};
        threads.emplace_back([func2 = std::move(func)] mutable
        {
            func2();
        });
        return &threads.back();
    }

    template <class Func>
    ThreadFromGlobalPool * async(const String & workload, Func func)
    {
        std::scoped_lock lock{threads_mutex};
        threads.emplace_back([=, this, func2 = std::move(func)] mutable
        {
            ClassifierPtr classifier = manager->acquire(workload);
            func2(classifier);
        });
        return &threads.back();
    }

    template <class Func>
    ThreadFromGlobalPool * async(const String & workload, const String & resource, Func func)
    {
        std::scoped_lock lock{threads_mutex};
        threads.emplace_back([=, this, func2 = std::move(func)] mutable
        {
            ClassifierPtr classifier = manager->acquire(workload);
            ResourceLink link = classifier->get(resource);
            func2(link);
        });
        return &threads.back();
    }

    template <class Func>
    ThreadFromGlobalPool * async(const String & workload, const String & resource1, const String & resource2, Func func)
    {
        std::scoped_lock lock{threads_mutex};
        threads.emplace_back([=, this, func2 = std::move(func)] mutable
        {
            ClassifierPtr classifier = manager->acquire(workload);
            ResourceLink link1 = resource1.empty() ? ResourceLink{} : classifier->get(resource1);
            ResourceLink link2 = resource2.empty() ? ResourceLink{} : classifier->get(resource2);
            func2(link1, link2);
        });
        return &threads.back();
    }

    template <class Func>
    void async(ThreadPool & pool, Func func)
    {
        pool.scheduleOrThrowOnError([func2 = std::move(func)] mutable
        {
            func2();
        });
    }

    template <class Func>
    void async(ThreadPool & pool, const String & workload, Func func)
    {
        pool.scheduleOrThrowOnError([=, this, func2 = std::move(func)] mutable
        {
            ClassifierPtr classifier = manager->acquire(workload);
            func2(classifier);
        });
    }

    template <class Func>
    void async(ThreadPool & pool, const String & workload, const String & resource, Func func)
    {
        pool.scheduleOrThrowOnError([=, this, func2 = std::move(func)] mutable
        {
            ClassifierPtr classifier = manager->acquire(workload);
            ResourceLink link = classifier->get(resource);
            func2(link);
        });
    }

    template <class Func>
    void async(ThreadPool & pool, const String & workload, const String & resource1, const String & resource2, Func func)
    {
        pool.scheduleOrThrowOnError([=, this, func2 = std::move(func)] mutable
        {
            ClassifierPtr classifier = manager->acquire(workload);
            ResourceLink link1 = resource1.empty() ? ResourceLink{} : classifier->get(resource1);
            ResourceLink link2 = resource2.empty() ? ResourceLink{} : classifier->get(resource2);
            func2(link1, link2);
        });
    }
};

using TestGuard = ResourceTest::Guard;

TEST(SchedulerWorkloadResourceManager, Smoke)
{
    ResourceTest t;

    t.query("CREATE RESOURCE res1 (WRITE DISK disk, READ DISK disk)");
    t.query("CREATE WORKLOAD all SETTINGS max_io_requests = 10");
    t.query("CREATE WORKLOAD A in all");
    t.query("CREATE WORKLOAD B in all SETTINGS weight = 3");

    ClassifierPtr c_a = t.manager->acquire("A");
    ClassifierPtr c_b = t.manager->acquire("B");

    for (int i = 0; i < 10; i++)
    {
        ResourceGuard g_a(ResourceGuard::Metrics::getIOWrite(), c_a->get("res1"), 1, ResourceGuard::Lock::Defer);
        g_a.lock();
        g_a.consume(1);
        g_a.unlock();

        ResourceGuard g_b(ResourceGuard::Metrics::getIOWrite(), c_b->get("res1"));
        g_b.unlock();

        ResourceGuard g_c(ResourceGuard::Metrics::getIORead(), c_b->get("res1"));
        g_b.consume(2);
    }
}

TEST(SchedulerWorkloadResourceManager, Fairness)
{
    // Total cost for A and B cannot differ for more than 1 (every request has cost equal to 1).
    // Requests from A use `value = 1` and from B `value = -1` is used.
    std::atomic<Int64> unfairness = 0;
    auto fairness_diff = [&] (Int64 value)
    {
        Int64 cur_unfairness = unfairness.fetch_add(value, std::memory_order_relaxed) + value;
        EXPECT_NEAR(cur_unfairness, 0, 1);
    };

    constexpr size_t threads_per_queue = 2;
    int requests_per_thread = 100;
    ResourceTest t(2 * threads_per_queue + 1);

    t.query("CREATE RESOURCE res1 (WRITE DISK disk, READ DISK disk)");
    t.query("CREATE WORKLOAD all SETTINGS max_io_requests = 1");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all");
    t.query("CREATE WORKLOAD leader IN all");

    for (int thread = 0; thread < threads_per_queue; thread++)
    {
        t.async([&]
        {
            ClassifierPtr c = t.manager->acquire("A");
            ResourceLink link = c->get("res1");
            t.startBusyPeriod(link, 1, requests_per_thread);
            for (int request = 0; request < requests_per_thread; request++)
            {
                TestGuard g(t, link, 1);
                fairness_diff(1);
            }
        });
    }

    for (int thread = 0; thread < threads_per_queue; thread++)
    {
        t.async([&]
        {
            ClassifierPtr c = t.manager->acquire("B");
            ResourceLink link = c->get("res1");
            t.startBusyPeriod(link, 1, requests_per_thread);
            for (int request = 0; request < requests_per_thread; request++)
            {
                TestGuard g(t, link, 1);
                fairness_diff(-1);
            }
        });
    }

    ClassifierPtr c = t.manager->acquire("leader");
    ResourceLink link = c->get("res1");
    t.blockResource(link);

    t.wait(); // Wait for threads to finish before destructing locals
}

TEST(SchedulerWorkloadResourceManager, DropNotEmptyQueue)
{
    ResourceTest t;

    t.query("CREATE RESOURCE res1 (WRITE DISK disk, READ DISK disk)");
    t.query("CREATE WORKLOAD all SETTINGS max_io_requests = 1");
    t.query("CREATE WORKLOAD intermediate IN all");

    std::barrier<std::__empty_completion> sync_before_enqueue(2);
    std::barrier<std::__empty_completion> sync_before_drop(3);
    std::barrier<std::__empty_completion> sync_after_drop(2);
    t.async("intermediate", "res1", [&] (ResourceLink link)
    {
        TestGuard g(t, link, 1);
        sync_before_enqueue.arrive_and_wait();
        sync_before_drop.arrive_and_wait(); // 1st resource request is consuming
        sync_after_drop.arrive_and_wait(); // 1st resource request is still consuming
    });

    sync_before_enqueue.arrive_and_wait(); // to maintain correct order of resource requests

    t.async("intermediate", "res1", [&] (ResourceLink link)
    {
        TestGuard g(t, link, 1, EnqueueOnly);
        sync_before_drop.arrive_and_wait(); // 2nd resource request is enqueued
        g.waitFailed("is about to be destructed");
    });

    sync_before_drop.arrive_and_wait(); // main thread triggers FifoQueue destruction by adding a unified child
    t.query("CREATE WORKLOAD leaf IN intermediate");
    sync_after_drop.arrive_and_wait();

    t.wait(); // Wait for threads to finish before destructing locals
}

TEST(SchedulerWorkloadResourceManager, DropNotEmptyQueueLong)
{
    ResourceTest t;

    t.query("CREATE RESOURCE res1 (WRITE DISK disk, READ DISK disk)");
    t.query("CREATE WORKLOAD all SETTINGS max_io_requests = 1");
    t.query("CREATE WORKLOAD intermediate IN all");

    static constexpr int queue_size = 100;
    std::barrier<std::__empty_completion> sync_before_enqueue(2);
    std::barrier<std::__empty_completion> sync_before_drop(2 + queue_size);
    std::barrier<std::__empty_completion> sync_after_drop(2);
    t.async("intermediate", "res1", [&] (ResourceLink link)
    {
        TestGuard g(t, link, 1);
        sync_before_enqueue.arrive_and_wait();
        sync_before_drop.arrive_and_wait(); // 1st resource request is consuming
        sync_after_drop.arrive_and_wait(); // 1st resource request is still consuming
    });

    sync_before_enqueue.arrive_and_wait(); // to maintain correct order of resource requests

    for (int i = 0; i < queue_size; i++)
    {
        t.async("intermediate", "res1", [&] (ResourceLink link)
        {
            TestGuard g(t, link, 1, EnqueueOnly);
            sync_before_drop.arrive_and_wait(); // many resource requests are enqueued
            g.waitFailed("is about to be destructed");
        });
    }

    sync_before_drop.arrive_and_wait(); // main thread triggers FifoQueue destruction by adding a unified child
    t.query("CREATE WORKLOAD leaf IN intermediate");
    sync_after_drop.arrive_and_wait();

    t.wait(); // Wait for threads to finish before destructing locals
}

class ThreadMetrics
{
public:
    ThreadMetrics() = default;
    ThreadMetrics(ThreadMetrics && other) noexcept
        : last_update_ns(other.last_update_ns)
        , consumed(other.consumed.load(std::memory_order_relaxed))
    {}

    ThreadMetrics & operator=(ThreadMetrics && other) noexcept
    {
        last_update_ns = other.last_update_ns;
        consumed.store(other.consumed.load(std::memory_order_relaxed), std::memory_order_relaxed);
        return *this;
    }

    void start()
    {
        UNUSED(padding);
        last_update_ns = clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID);
        consumed = 0;
    }

    // Make sure that it is called periodically from the same thread as start()
    void update()
    {
        UInt64 now = clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID);
        consumed.fetch_add(now - last_update_ns, std::memory_order_relaxed);
        last_update_ns = now;
    }

    UInt64 takeConsumed()
    {
        return consumed.exchange(0, std::memory_order_relaxed);
    }

private:
    UInt64 last_update_ns = 0;
    std::atomic<UInt64> consumed{0};
    char padding[64]; // to avoid false sharing
};

class ThreadMetricsGroup : public boost::noncopyable
{
public:
    explicit ThreadMetricsGroup(size_t size = 0)
        : metrics(size)
    {}

    void resize(size_t size)
    {
        metrics.resize(size);
    }

    void start(size_t thread_num)
    {
        metrics[thread_num].start();
    }

    void update(size_t thread_num)
    {
        metrics[thread_num].update();
    }

    UInt64 takeConsumed()
    {
        UInt64 total_consumed = 0;
        for (auto & metric : metrics)
            total_consumed += metric.takeConsumed();
        return total_consumed;
    }

private:
    std::vector<ThreadMetrics> metrics;
};

class ThreadMetricsTester
{
public:
    static double now()
    {
        return static_cast<double>(clock_gettime_ns());
    }

    struct Assertion
    {
        ThreadMetricsGroup * group;
        double share = 0.0; // share of total consumed by all groups; zero means no share assertion
        double max_speed = 0.0; // in cpus; zero means no throttling assertion
        UInt64 max_burst_ns = 0; // in nanoseconds
        bool exclude_from_total = false; // if true, this assertion will not be included in total consumed

        EventRateMeter consumed{now(), 120'000'000 /*ns*/};
        UInt64 consumed_integral_ns = 0; // Total consumed by this assertion

        void init(double now_ns)
        {
            group->takeConsumed(); // Reset consumed value before starting
            consumed.reset(now_ns);
        }

        UInt64 process(double now_ns)
        {
            UInt64 consumed_ns = group->takeConsumed();
            consumed.add(now_ns, static_cast<double>(consumed_ns));
            consumed_integral_ns += consumed_ns;
            return exclude_from_total ? 0 : consumed_ns;
        }
    };

    /// Waits for share of group to stabilize on given value
    /// WARNING: do not add the same group to both expectShare and expectThrottling
    ThreadMetricsTester & expectShare(ThreadMetricsGroup * group, double share)
    {
        assertions.emplace_back(Assertion
            {
                .group = group,
                .share = share
            });
        return *this;
    }

    UInt64 burstUpperBound(double burst_sec, size_t max_concurrent_threads, size_t max_concurrent_queries, size_t quantum_ns) const
    {
        // It is hard to calculate tight upper bound for burst.
        // First, we take into account configured burst,
        // then we add quantum for every thread that can run concurrently (according to configure slots limit)
        // then we add quantum for every query because it is allowed to run one extra thread.
        // and finally due to report_ns period, quantum can be extended by 10%
        return static_cast<UInt64>(burst_sec * 1'000'000'000 + (max_concurrent_threads + max_concurrent_queries) * (quantum_ns * 1.1));
    }

    /// Waits for share of group to stabilize on given value
    ThreadMetricsTester & expectThrottling(
        double speed,
        double burst_sec,
        size_t max_concurrent_threads,
        size_t max_concurrent_queries,
        size_t quantum_ns)
    {
        max_speed = speed;
        max_burst_ns = burstUpperBound(burst_sec, max_concurrent_threads, max_concurrent_queries, quantum_ns);
        return *this;
    }

    /// Waits for share of group to stabilize on given value
    /// WARNING: do not add the same group to both expectShare and expectThrottling
    ThreadMetricsTester & expectThrottling(
        ThreadMetricsGroup * group,
        double speed,
        double burst_sec,
        size_t max_concurrent_threads,
        size_t max_concurrent_queries,
        size_t quantum_ns,
        bool exclude_from_total = false)
    {
        assertions.emplace_back(Assertion
            {
                .group = group,
                .max_speed = speed,
                .max_burst_ns = burstUpperBound(burst_sec, max_concurrent_threads, max_concurrent_queries, quantum_ns),
                .exclude_from_total = exclude_from_total
            });
        return *this;
    }

    ThreadMetricsTester & runAtLeast(double seconds)
    {
        runtime_ns = seconds * 1'000'000'000;
        return *this;
    }

    /// Makes sure that checks are done only when total CPUs is not less than given value.
    /// If there is not enough CPUs, we skip assertions.
    /// This is useful for CI environment where number of CPUs can be very low.
    ThreadMetricsTester & assumeCPUs(double cpus)
    {
        assume_cpus = cpus;
        return *this;
    }

    void check()
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(12)); // Skip to avoid measurements during transition period

        double start_ns = now();
        for (auto & assertion : assertions)
            assertion.init(start_ns);

        // We check infinite time to avoid flakiness.
        // CI environment is very unstable and it can take a lot of time to reach the expected shares.
        bool passed = false;
        EventRateMeter total_consumed(now(), 120'000'000 /*ns*/);
        UInt64 cpu_assumption_fail_count = 0;
        UInt64 total_consumed_integral_ns = 0;
        while (!passed && now() < start_ns + runtime_ns)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(24)); // Sleep to allow threads to consume resources
            double now_ns = now();
            UInt64 total_consumed_delta = 0;
            for (auto & assertion : assertions)
                total_consumed_delta += assertion.process(now_ns);
            double measured_ns = now(); // This timestamp should be done after all measurements for token bucket assertions
            total_consumed.add(now_ns, static_cast<double>(total_consumed_delta));
            total_consumed_integral_ns += total_consumed_delta;
            double consumed_cpus = total_consumed.rate(now_ns);
            DBG_PRINT("[{}] Total consumed delta: {} ns, CPU usage: {:.2f} cpus", now_ns - start_ns, total_consumed_delta, consumed_cpus);
            if (consumed_cpus < assume_cpus * 0.9) // Allow some margin of error
            {
                cpu_assumption_fail_count++;
                if (cpu_assumption_fail_count > 10)
                {
                    DBG_PRINT("CPUs ({}/{}) is not enough for assertions, skipping checks", consumed_cpus, assume_cpus);
                    return; // Skip checks if we do not have enough CPUs
                }
            }
            else
            {
                passed = true;
                for (auto & assertion : assertions)
                {
                    if (assertion.share > 0.0)
                    {
                        double actual_share = assertion.consumed.rate(now_ns) / consumed_cpus;
                        if (std::abs(actual_share - assertion.share) / assertion.share > 0.1)
                        {
                            passed = false;
                            DBG_PRINT("Assertion failed: expected share {:.2f}, actual share {:.2f}, total cpus {:.2f}", assertion.share, actual_share, consumed_cpus);
                        }
                        else
                        {
                            DBG_PRINT("Assertion passed: expected share {:.2f}, actual share {:.2f}, total cpus {:.2f}", assertion.share, actual_share, consumed_cpus);
                        }
                    }
                    if (assertion.max_speed > 0.0)
                    {
                        double allowed_consumption_ns = assertion.max_speed * (measured_ns - start_ns) + assertion.max_burst_ns;
                        if (assertion.consumed_integral_ns > allowed_consumption_ns)
                        {
                            DBG_PRINT("Assertion failed: expected less than {:.2f} ms, actual {:.2f} ms", allowed_consumption_ns / 1'000'000.0, assertion.consumed_integral_ns / 1'000'000.0);
                            GTEST_FAIL();
                        }
                        else
                        {
                            DBG_PRINT("Assertion passed: expected less than {:.2f} ms, actual {:.2f} ms", allowed_consumption_ns / 1'000'000.0, assertion.consumed_integral_ns / 1'000'000.0);
                        }
                    }
                }

                if (max_speed > 0.0)
                {
                    double allowed_consumption_ns = max_speed * (measured_ns - start_ns) + max_burst_ns;
                    if (total_consumed_integral_ns > allowed_consumption_ns)
                    {
                        DBG_PRINT("Assertion failed: expected less than {:.2f} ms, actual {:.2f} ms", allowed_consumption_ns / 1'000'000.0, total_consumed_integral_ns / 1'000'000.0);
                        GTEST_FAIL();
                    }
                    else
                    {
                        DBG_PRINT("Assertion passed: expected less than {:.2f} ms, actual {:.2f} ms", allowed_consumption_ns / 1'000'000.0, total_consumed_integral_ns / 1'000'000.0);
                    }
                }
            }
        }
    }

private:
    std::vector<Assertion> assertions;
    double assume_cpus = 0.0;
    double max_speed = 0.0;
    UInt64 max_burst_ns = 0;
    double runtime_ns = 0.0;
};

// It emulates how PipelineExecutor interacts with CPU scheduler
struct TestQuery {
    ResourceTest & t;

    std::mutex slots_mutex;
    SlotAllocationPtr slots;

    std::mutex mutex;
    std::condition_variable cv;
    size_t max_threads = 1;
    size_t threads_finished = 0;
    size_t active_threads = 0;
    size_t started_threads = 0;
    bool query_is_finished = false;
    UInt64 work_left = UInt64(-1);
    String name;

    // Only used if preemption is enabled
    struct ThreadStatus
    {
        bool is_active = false;
    };
    std::vector<ThreadStatus> threads;

    ThreadFromGlobalPool * master_thread = nullptr;
    std::unique_ptr<ThreadPool> pool;

    ThreadMetricsGroup metrics;

    static constexpr int us_per_work = 10;

    explicit TestQuery(ResourceTest & t_)
        : t(t_)
    {}

    ~TestQuery()
    {
        finish();
        if (master_thread && master_thread->joinable())
            master_thread->join();
    }

    AcquiredSlotPtr trySpawn()
    {
        {
            std::unique_lock lock{mutex};
            if (query_is_finished)
                return {};
        }
        return slots->tryAcquire();
    }

    bool controlConcurrency(ISlotLease * cpu_lease)
    {
        // upscale if possible
        while (auto slot = trySpawn())
        {
            t.async(*pool, [this, my_slot = std::move(slot)] mutable
            {
                threadFunc(std::move(my_slot));
            });
        }

        // preemption and downscaling
        if (cpu_lease)
            return cpu_lease->renew();
        else
            return true;
    }

    void finish()
    {
        std::unique_lock lock{mutex};
        DBG_PRINT("=== FINISHED ===");
        query_is_finished = true;
        cv.notify_all();
    }

    void finishThread(size_t count = 1)
    {
        std::unique_lock lock{mutex};
        threads_finished = std::min(max_threads, threads_finished + count);
        cv.notify_all();
    }

    // Wait until number of active threads is not less than specified
    void waitActiveThreads(size_t thread_num_to_wait)
    {
        std::unique_lock lock{mutex};
        cv.wait(lock, [=, this] () { return active_threads >= thread_num_to_wait; });
    }

    // Wait until number of started threads (including already finished) is not less than specified
    void waitStartedThreads(size_t thread_num_to_wait)
    {
        std::unique_lock lock{mutex};
        cv.wait(lock, [=, this] () { return started_threads >= thread_num_to_wait; });
    }

    // Wait until resource request is enqueued
    void waitEnqueued()
    {
        while (true)
        {
            {
                std::unique_lock lock{slots_mutex};
                if (slots && slots->isRequesting())
                    return;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }

    // Returns unique thread number
    size_t onThreadStart(ISlotLease * cpu_lease)
    {
        std::unique_lock lock{mutex};

        // We cannot start more threads than max_threads
        cv.wait(lock, [=, this] { return active_threads < max_threads; });

        active_threads++;
        started_threads++;
        cv.notify_all();

        // We could reuse thread numbers if preemption (and downscaling) are enabled, otherwise do not reuse
        if (cpu_lease)
        {
            // Iterate through all thread and find the first free thread number
            size_t thread_num = 0;
            while (thread_num < max_threads && threads[thread_num].is_active)
                thread_num++;
            chassert(thread_num < max_threads);
            threads[thread_num].is_active = true;
            return thread_num;
        }
        else
        {
            return started_threads - 1;
        }
    }

    void onThreadStop(ISlotLease * cpu_lease, size_t thread_num)
    {
        std::scoped_lock lock{mutex};
        active_threads--;
        cv.notify_all();
        if (cpu_lease)
        {
            chassert(thread_num < max_threads);
            chassert(threads[thread_num].is_active);
            threads[thread_num].is_active = false;
            DBG_PRINT("Thread {} finished", thread_num);
        }
        else
        {
            DBG_PRINT("Thread {} finished (no preemption)", thread_num);
        }
    }

    // Returns true iff query should continue execution
    bool doWork(size_t thread_num)
    {
        {
            // Take one piece of work to do for the next 10 us
            std::unique_lock lock{mutex};
            if (work_left > 0)
                work_left--;
        }

        auto predicate = [=, this]
        {
            return query_is_finished
                || work_left == 0
                || (thread_num < threads_finished // When finish not the last thread make sure query will have at least 1 active thread afterwards
                    && (active_threads > 1 || threads_finished == max_threads));
        };
        auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(us_per_work);
        volatile uint64_t count = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            count = count + 1; // CPU-intensive work
            metrics.update(thread_num);
            if (count % 128 == 0) {
                std::unique_lock lock2{mutex};
                if (predicate())
                    return false;
            }
        }
        std::unique_lock lock2{mutex};
        return !predicate();
    }

    void threadFunc(AcquiredSlotPtr self_slot)
    {
        chassert(self_slot);

        ISlotLease * cpu_lease = dynamic_cast<ISlotLease *>(self_slot.get());
        const size_t thread_num = onThreadStart(cpu_lease);
        SCOPE_EXIT({ onThreadStop(cpu_lease, thread_num); });

        // Start consumption and metrics for this thread
        if (cpu_lease)
            cpu_lease->startConsumption();
        metrics.start(thread_num);

        DB::setThreadName(DB::ThreadName::TEST_SCHEDULER);
        while (true)
        {
            if (!controlConcurrency(cpu_lease))
                break;
            if (!doWork(thread_num))
                break;
        }
    }

    enum AllocationType
    {
        AllocateSlots, // CPUSlotsAllocation (slot count fairness, no preemption)
        AllocateLease, // CPULeaseAllocation (cpu time fairness + preemption)
        AllocateLeaseNoDownscale, // CPULeaseAllocation (cpu time fairness + preemption w/o timeout)
        AllocateLeaseQuantumX3, // CPULeaseAllocation (cpu time fairness, preemption, x3 larger resource request cost)
    };

    SlotAllocationPtr allocateCPUSlots(AllocationType type, ResourceLink master_link, ResourceLink worker_link, const String & workload)
    {
        CPULeaseSettings settings;
        settings.workload = workload;
        settings.preemption_timeout = std::chrono::milliseconds(12); // We use smaller timeout to make tests faster
        switch (type) {
            case AllocateSlots:
                return std::make_shared<CPUSlotsAllocation>(1, max_threads - 1, master_link, worker_link);
            case AllocateLease:
                return std::make_shared<CPULeaseAllocation>(max_threads, master_link, worker_link, settings);
            case AllocateLeaseNoDownscale:
                settings.preemption_timeout = std::chrono::milliseconds::max();
                return std::make_shared<CPULeaseAllocation>(max_threads, master_link, worker_link, settings);
            case AllocateLeaseQuantumX3:
                settings.quantum_ns = CPULeaseSettings::default_quantum_ns * 3;
                return std::make_shared<CPULeaseAllocation>(max_threads, master_link, worker_link, settings);
        }
    }

    void start(String workload, SlotCount max_threads_, UInt64 runtime_us = UInt64(-1))
    {
        start(AllocateSlots, workload, max_threads_, runtime_us);
    }

    void start(AllocationType type, String workload, SlotCount max_threads_, UInt64 runtime_us = UInt64(-1))
    {
        std::scoped_lock lock{mutex};
        name = workload;
        max_threads = max_threads_;
        if (runtime_us != UInt64(-1))
            work_left = runtime_us / us_per_work;
        threads.resize(max_threads);
        metrics.resize(max_threads);
        pool = std::make_unique<ThreadPool>(CurrentMetrics::QueryPipelineExecutorThreads, CurrentMetrics::QueryPipelineExecutorThreadsActive, CurrentMetrics::QueryPipelineExecutorThreadsScheduled, max_threads);
        master_thread = t.async(workload, t.storage.getMasterThreadResourceName(), t.storage.getWorkerThreadResourceName(),
            [&, type, workload] (ResourceLink master_link, ResourceLink worker_link)
            {
                setThreadName(ThreadName::TEST_SCHEDULER);
                {
                    std::unique_lock in_thread_lock{slots_mutex};
                    slots = allocateCPUSlots(type, master_link, worker_link, workload);
                }
                threadFunc(slots->acquire());

                // TODO(serxa): this is not needed any longer. we do pool->wait(). Remove and check tests.
                // We have to keep this thread alive even when threadFunc() is finished
                // because `slots` keep references to ProfileEvent stored in thread-local storage.
                // PipelineExecutor master thread does similar thing by joining all additional threads during finalize.
                {
                    std::unique_lock lock3{mutex};
                    cv.wait(lock3, [this] () { return query_is_finished; });
                }

                // Acquired slot holds a reference to the allocation, we need to wait for release of all acquired slots
                if (pool)
                    pool->wait();

                std::unique_lock lock4{slots_mutex};
                chassert(slots.use_count() == 1); // We need to destroy slots here, before thread exit
                slots.reset();
            });
    }
};

using TestQueryPtr = std::shared_ptr<TestQuery>;

TEST(SchedulerWorkloadResourceManager, CPUSchedulingRoundRobin)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 4");

    // Do multiple iterations to check that:
    // (a) scheduling is memoryless
    // (b) resource request canceling works as expected
    for (int iteration = 0; iteration < 4; ++iteration)
    {
        std::vector<TestQueryPtr> queries;
        for (int query = 0; query < 2; query++)
            queries.push_back(std::make_shared<TestQuery>(t));

        auto ensure = [&] (size_t q0_threads, size_t q1_threads)
        {
            if (q0_threads > 0)
                queries[0]->waitStartedThreads(q0_threads);
            if (q1_threads > 0)
                queries[1]->waitStartedThreads(q1_threads);
        };

        auto finish = [&] (size_t q0_threads, size_t q1_threads)
        {
            if (q0_threads > 0)
                queries[0]->finishThread(q0_threads);
            if (q1_threads > 0)
                queries[1]->finishThread(q1_threads);
        };

        // Note that:
        // * thread with number 0 is noncompeting (i.e. does not enqueue resource request)
        // * every query enqueues at most 1 resource request to the scheduler queue
        // * round-robin is emulated using FIFO queue: the query that enqueued it's request first is the next to receive the slot
        queries[0]->start("all", 7);
        ensure(5, 0); // Q0: 0 1 2 3 4; Q1: -
        queries[1]->start("all", 8);
        ensure(5, 1); // Q0: 0 1 2 3 4; Q1: 0
        finish(2, 0); // Q0: 2 3 4; Q1: 0
        ensure(6, 1); // Q0: 2 3 4 5; Q2: 0
        finish(1, 0); // Q0: 3 4 5; Q1: 0
        ensure(6, 2); // Q0: 3 4 5; Q1: 0 1
        finish(1, 1); // Q0: 4 5; Q1: 1
        ensure(7, 1); // Q0: 4 5 6; Q1: 1
        finish(1, 0); // Q0: 5 6; Q1: 1
        ensure(7, 3); // Q0: 5 6; Q1: 1 2

        // Q0 - is done, Q1 still have pending resource requests, but we cancel it
        queries.clear();
    }

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, CPUSchedulingFairness)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 4");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all");
    t.query("CREATE WORKLOAD leader IN all");

    auto leader = std::make_shared<TestQuery>(t);
    std::vector<TestQueryPtr> queries;
    for (int query = 0; query < 2; query++)
        queries.push_back(std::make_shared<TestQuery>(t));

    // Acquire all slots to create overloaded state
    leader->start("leader", 4);
    leader->waitStartedThreads(4);

    // One of queries will be the first, but we do not case which one
    queries[0]->start("A", 10);
    queries[1]->start("B", 10);

    // Make sure requests are enqueued and release all the slots
    queries[0]->waitEnqueued();
    queries[1]->waitEnqueued();
    leader.reset();

    for (int i = 2; i < 10; i++)
    {
        // Acquire two slots
        queries[0]->waitStartedThreads(i);
        queries[1]->waitStartedThreads(i);

        // Release two slots
        queries[0]->finishThread(1);
        queries[1]->finishThread(1);
    }

    queries.clear();

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, CPUSchedulingWeights)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 8");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all SETTINGS weight = 3");
    t.query("CREATE WORKLOAD leader IN all");

    auto leader = std::make_shared<TestQuery>(t);
    std::vector<TestQueryPtr> queries;
    for (int query = 0; query < 2; query++)
        queries.push_back(std::make_shared<TestQuery>(t));

    // Acquire all slots to create overloaded state
    leader->start("leader", 8);
    leader->waitStartedThreads(8);

    // One of queries will be the first, but we do not case which one
    queries[0]->start("A", 10);
    queries[1]->start("B", 30);

    // Make sure requests are enqueued and release all the slots
    queries[0]->waitEnqueued();
    queries[1]->waitEnqueued();
    leader.reset();

    for (int i = 2; i < 10; i++)
    {
        // Acquire slots
        queries[0]->waitStartedThreads(i);
        queries[1]->waitStartedThreads(i * 3);

        // Release slots
        queries[0]->finishThread(1);
        queries[1]->finishThread(3);
    }

    queries.clear();

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, CPUSchedulingPriorities)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 4");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all SETTINGS priority = 1"); // lower priority
    t.query("CREATE WORKLOAD leader IN all");

    auto leader = std::make_shared<TestQuery>(t);
    std::vector<TestQueryPtr> queries;
    for (int query = 0; query < 2; query++)
        queries.push_back(std::make_shared<TestQuery>(t));

    // Acquire all slots to create overloaded state
    leader->start("leader", 4);
    leader->waitStartedThreads(4);

    // One of queries will be the first, but we do not case which one
    queries[0]->start("A", 10);
    queries[1]->start("B", 10);

    // Make sure requests are enqueued and release all the slots
    queries[0]->waitEnqueued();
    queries[1]->waitEnqueued();
    leader.reset();

    queries[0]->waitStartedThreads(4);
    queries[0]->finishThread(3);
    queries[0]->waitStartedThreads(7);
    queries[0]->finishThread(3);
    queries[0]->waitStartedThreads(10);
    queries[0]->finishThread(3);
    queries[1]->waitStartedThreads(3);
    queries[0]->finishThread(1);
    queries[1]->waitStartedThreads(4);
    queries[1]->finishThread(3);
    queries[1]->waitStartedThreads(7);
    queries[1]->finishThread(3);
    queries[1]->waitStartedThreads(10);

    queries.clear();

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, CPUSchedulingIndependentPools)
{
    std::barrier<std::__empty_completion> sync_start(2);

    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all");
    t.query("CREATE WORKLOAD pool1 IN all SETTINGS max_concurrent_threads = 4");
    t.query("CREATE WORKLOAD pool2 IN all SETTINGS max_concurrent_threads = 4");
    t.query("CREATE WORKLOAD A1 IN pool1");
    t.query("CREATE WORKLOAD B1 IN pool1");
    t.query("CREATE WORKLOAD leader1 IN pool1");
    t.query("CREATE WORKLOAD A2 IN pool2");
    t.query("CREATE WORKLOAD B2 IN pool2");
    t.query("CREATE WORKLOAD leader2 IN pool2");

    for (int pool = 1; pool <= 2; pool++)
    {
        t.async([&, pool]
        {
            auto leader = std::make_shared<TestQuery>(t);
            std::vector<TestQueryPtr> queries;
            for (int query = 0; query < 2; query++)
                queries.push_back(std::make_shared<TestQuery>(t));

            auto ensure = [&] (size_t q0_threads, size_t q1_threads)
            {
                if (q0_threads > 0)
                    queries[0]->waitStartedThreads(q0_threads);
                if (q1_threads > 0)
                    queries[1]->waitStartedThreads(q1_threads);
            };

            auto finish = [&] (size_t q0_threads, size_t q1_threads)
            {
                if (q0_threads > 0)
                    queries[0]->finishThread(q0_threads);
                if (q1_threads > 0)
                    queries[1]->finishThread(q1_threads);
            };

            // Acquire all slots to create overloaded state
            leader->start(fmt::format("leader{}", pool), 4);
            leader->waitStartedThreads(4);

            // Ensure both pools have started and used all their threads simultaneously
            sync_start.arrive_and_wait();

            // One of queries will be the first, but we do not case which one
            queries[0]->start(fmt::format("A{}", pool), 10);
            queries[1]->start(fmt::format("B{}", pool), 10);

            // Make sure requests are enqueued and release all the slots
            queries[0]->waitEnqueued();
            queries[1]->waitEnqueued();
            leader.reset();

            for (int i = 2; i < 10; i++)
            {
                ensure(i, i); // acquire two slots
                finish(1, 1); // release two slots
            }

            queries.clear();
        });
    }

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, MaxCPUsDerivedFromShare)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    // Only max_cpu_share is set, max_cpus is unset
    t.query("CREATE WORKLOAD all SETTINGS max_cpu_share = 0.5");
    ClassifierPtr c = t.manager->acquire("all");

    // The expected hard cap is max_cpu_share * getNumberOfCPUCoresToUse()
    WorkloadSettings settings = c->getWorkloadSettings("cpu");
    double expected_cap = 0.5 * getNumberOfCPUCoresToUse();
    double actual_cap = settings.max_cpus;

    EXPECT_DOUBLE_EQ(actual_cap, expected_cap);
}

auto getAcquired()
{
    return CurrentMetrics::get(CurrentMetrics::ConcurrencyControlAcquired);
}

auto getPreempted()
{
    return CurrentMetrics::get(CurrentMetrics::ConcurrencyControlPreempted);
}

struct EventCounter
{
    ProfileEvents::Event event;
    size_t initial_value;

    explicit EventCounter(ProfileEvents::Event event_)
        : event(event_)
    {
        initial_value = getValue();
    }

    size_t count() const
    {
        return getValue() - initial_value;
    }

private:
    size_t getValue() const
    {
        return ProfileEvents::global_counters[event].load(std::memory_order_relaxed);
    }
};

TEST(SchedulerWorkloadResourceManager, PreemptiveCPUSchedulingPreemption)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 8");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all");

    // Do multiple iterations to check that:
    // (a) scheduling is memoryless
    // (b) resource request canceling works as expected
    for (int iteration = 0; iteration < 4; ++iteration)
    {
        DBG_PRINT("--- Start #{} ---", iteration);
        std::vector<TestQueryPtr> queries;
        for (int query = 0; query < 2; query++)
            queries.push_back(std::make_shared<TestQuery>(t));

        DBG_PRINT("--- Q0 ---");
        queries[0]->start(TestQuery::AllocateLeaseNoDownscale, "A", 8);
        queries[0]->waitStartedThreads(8);
        while (getAcquired() < 8) std::this_thread::yield(); // Wait Q0 to upscale to all 8 threads
        DBG_PRINT("--- Q1 ---");
        queries[1]->start(TestQuery::AllocateLeaseNoDownscale, "B", 8);
        DBG_PRINT("--- Wait preemption ---");
        while (getAcquired() < 8 || getPreempted() < 4) std::this_thread::yield(); // Wait 4 threads of Q0 to became preempted
        DBG_PRINT("--- Stop ---");
    }

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, PreemptiveCPUSchedulingFairness)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 8");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all");

    DBG_PRINT("--- Start ---");
    std::vector<TestQueryPtr> queries;
    for (int query = 0; query < 2; query++)
        queries.push_back(std::make_shared<TestQuery>(t));

    queries[0]->start(TestQuery::AllocateLeaseNoDownscale, "A", 8);
    queries[1]->start(TestQuery::AllocateLeaseNoDownscale, "B", 8);
    ThreadMetricsTester()
        .expectShare(&queries[0]->metrics, 0.5)
        .expectShare(&queries[1]->metrics, 0.5)
        .check();
    DBG_PRINT("--- Stop ---");

    queries.clear();

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, PreemptiveCPUSchedulingFairnessIsQuantumIndependent)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 8");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all");

    DBG_PRINT("--- Start ---");
    std::vector<TestQueryPtr> queries;
    for (int query = 0; query < 2; query++)
        queries.push_back(std::make_shared<TestQuery>(t));

    queries[0]->start(TestQuery::AllocateLease, "A", 8);
    queries[1]->start(TestQuery::AllocateLeaseQuantumX3, "B", 8);
    ThreadMetricsTester()
        .expectShare(&queries[0]->metrics, 0.5)
        .expectShare(&queries[1]->metrics, 0.5)
        .check();
    DBG_PRINT("--- Stop ---");

    queries.clear();

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, PreemptiveCPUSchedulingFairnessIsQueryNumberIndependent)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 8");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all");

    DBG_PRINT("--- 1xA vs 5xB ---");
    std::vector<TestQueryPtr> queries;
    for (int query = 0; query < 6; query++)
        queries.push_back(std::make_shared<TestQuery>(t));
    queries[0]->start(TestQuery::AllocateLease, "A", 8);
    queries[1]->start(TestQuery::AllocateLease, "B", 8);
    queries[2]->start(TestQuery::AllocateLease, "B", 8);
    queries[3]->start(TestQuery::AllocateLease, "B", 8);
    queries[4]->start(TestQuery::AllocateLease, "B", 8);
    queries[5]->start(TestQuery::AllocateLease, "B", 8);
    ThreadMetricsTester()
        .expectShare(&queries[0]->metrics, 0.5)
        .expectShare(&queries[1]->metrics, 0.1)
        .expectShare(&queries[2]->metrics, 0.1)
        .expectShare(&queries[3]->metrics, 0.1)
        .expectShare(&queries[4]->metrics, 0.1)
        .expectShare(&queries[5]->metrics, 0.1)
        .check();
    DBG_PRINT("--- Stop ---");
    queries.clear();

    DBG_PRINT("--- 4xA vs 2xB ---");
    for (int query = 0; query < 6; query++)
        queries.push_back(std::make_shared<TestQuery>(t));
    queries[0]->start(TestQuery::AllocateLease, "A", 8);
    queries[1]->start(TestQuery::AllocateLease, "A", 8);
    queries[2]->start(TestQuery::AllocateLease, "A", 8);
    queries[3]->start(TestQuery::AllocateLease, "A", 8);
    queries[4]->start(TestQuery::AllocateLease, "B", 8);
    queries[5]->start(TestQuery::AllocateLease, "B", 8);
    ThreadMetricsTester()
        .expectShare(&queries[0]->metrics, 0.125)
        .expectShare(&queries[1]->metrics, 0.125)
        .expectShare(&queries[2]->metrics, 0.125)
        .expectShare(&queries[3]->metrics, 0.125)
        .expectShare(&queries[4]->metrics, 0.25)
        .expectShare(&queries[5]->metrics, 0.25)
        .check();
    DBG_PRINT("--- Stop ---");
    queries.clear();

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, PreemptiveCPUSchedulingWeights)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 8");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all SETTINGS weight = 3");

    DBG_PRINT("--- Start ---");
    std::vector<TestQueryPtr> queries;
    for (int query = 0; query < 2; query++)
        queries.push_back(std::make_shared<TestQuery>(t));

    queries[0]->start(TestQuery::AllocateLeaseNoDownscale, "A", 8);
    queries[1]->start(TestQuery::AllocateLeaseNoDownscale, "B", 8);
    ThreadMetricsTester()
        .expectShare(&queries[0]->metrics, 0.25)
        .expectShare(&queries[1]->metrics, 0.75)
        .check();
    DBG_PRINT("--- Stop ---");

    queries.clear();

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, PreemptiveCPUSchedulingMaxMinFairness)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 10");
    t.query("CREATE WORKLOAD A IN all SETTINGS weight = 6");
    t.query("CREATE WORKLOAD B IN all");
    t.query("CREATE WORKLOAD C IN all");
    t.query("CREATE WORKLOAD D IN all SETTINGS weight = 2");

    DBG_PRINT("--- Start ---");
    std::vector<TestQueryPtr> queries;
    for (int query = 0; query < 4; query++)
        queries.push_back(std::make_shared<TestQuery>(t));

    queries[0]->start(TestQuery::AllocateLeaseNoDownscale, "A", 2);
    queries[1]->start(TestQuery::AllocateLeaseNoDownscale, "B", 8);
    queries[2]->start(TestQuery::AllocateLeaseNoDownscale, "C", 8);
    queries[3]->start(TestQuery::AllocateLeaseNoDownscale, "D", 8);

    // Check max-min fair allocation
    // A: guaranteed share is 6/10, but it uses only 2/10 threads, so 8 threads left for others
    // B: fair share is 2/10
    // C: fair share is 2/10
    // D: fair share is 4/10
    ThreadMetricsTester()
        .expectShare(&queries[0]->metrics, 2.0 / 10.0)
        .expectShare(&queries[1]->metrics, 2.0 / 10.0)
        .expectShare(&queries[2]->metrics, 2.0 / 10.0)
        .expectShare(&queries[3]->metrics, 4.0 / 10.0)
        .assumeCPUs(10.0) // in CI environment we can have less CPU in total and A workload will have greater share
        .check();
    DBG_PRINT("--- Stop ---");

    queries.clear();

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, PreemptiveCPUSchedulingDownscaling)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 8");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all");

    // Do multiple iterations to check that:
    // (a) scheduling is memoryless
    // (b) resource request canceling works as expected
    for (int iteration = 0; iteration < 4; ++iteration)
    {
        DBG_PRINT("--- Start #{} ---", iteration);
        std::vector<TestQueryPtr> queries;
        for (int query = 0; query < 2; query++)
            queries.push_back(std::make_shared<TestQuery>(t));

        DBG_PRINT("--- Q0 ---");
        queries[0]->start(TestQuery::AllocateLease, "A", 8);
        queries[0]->waitStartedThreads(8);
        while (getAcquired() < 8) std::this_thread::yield(); // Wait Q0 to upscale to all 8 threads
        DBG_PRINT("--- Q1 ---");
        EventCounter downscales(ProfileEvents::ConcurrencyControlDownscales);
        queries[1]->start(TestQuery::AllocateLease, "B", 8);
        DBG_PRINT("--- Wait downscaling ---");
        // Wait 3 threads of Q0 to became preempted and downscaled.
        // Note that we do not check all 4 thread to be downscaled, due to the fact cpu lease allows
        // to run one more thread than slots allocated as long as this thread does not consume too much resources.
        while (downscales.count() < 3) std::this_thread::yield();
        DBG_PRINT("--- Stop ---");
    }

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, PreemptiveCPUSchedulingUpscaling)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 8");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all");

    std::vector<TestQueryPtr> queries;
    for (int query = 0; query < 2; query++)
        queries.push_back(std::make_shared<TestQuery>(t));

    DBG_PRINT("--- Q0 ---");
    queries[0]->start(TestQuery::AllocateLease, "A", 8);
    queries[0]->waitStartedThreads(8);

    for (int iteration = 0; iteration < 4; ++iteration)
    {
        while (getAcquired() < 8) std::this_thread::yield(); // Wait Q0 to upscale to all 8 threads
        DBG_PRINT("--- Q1 ---");
        EventCounter downscales(ProfileEvents::ConcurrencyControlDownscales);
        queries[1]->start(TestQuery::AllocateLease, "B", 8);
        DBG_PRINT("--- Wait downscaling ---");
        // Wait 3 threads of Q0 to became preempted and downscaled.
        // Note that we do not check all 4 thread to be downscaled, due to the fact cpu lease allows
        // to run one more thread than slots allocated as long as this thread does not consume too much resources.
        while (downscales.count() < 3) std::this_thread::yield();
        DBG_PRINT("--- Wait upscaling ---");
        EventCounter upscales(ProfileEvents::ConcurrencyControlUpscales);
        queries[1].reset(); // Release all slots of Q1 to allow Q0 to upscale
        while (upscales.count() < 3) std::this_thread::yield();
        queries[1] = std::make_shared<TestQuery>(t); // Recreate Q1 for the next iteration
    }

    queries.clear();

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, PreemptiveCPUSchedulingThrottlingHalfCore)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 8, max_cpus = 0.5, max_burst_cpu_seconds = 0.05");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all");

    // Make two iterations to check different initial bursts:
    // First iteration starts with full token bucket and the following iterations start with unknown number of tokens in bucket.
    // (Bucket should be empty, but in CI it may be again full, so the test is the same)
    for (int iteration = 0; iteration < 3; ++iteration)
    {
        DBG_PRINT("--- Start #{} ---", iteration);
        std::vector<TestQueryPtr> queries;
        for (int query = 0; query < 2; query++)
            queries.push_back(std::make_shared<TestQuery>(t));

        queries[0]->start(TestQuery::AllocateLease, "A", 8);
        queries[1]->start(TestQuery::AllocateLease, "B", 8);
        DBG_PRINT("--- Check throttling ---");
        ThreadMetricsTester()
            .expectShare(&queries[0]->metrics, 0.5)
            .expectShare(&queries[1]->metrics, 0.5)
            .expectThrottling(0.5, 0.05, 8, 2, CPULeaseSettings::default_quantum_ns)
            .runAtLeast(0.1)
            .check();
        DBG_PRINT("--- Stop ---");
    }

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, PreemptiveCPUSchedulingThrottlingTwoCore)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 8, max_cpus = 2, max_burst_cpu_seconds = 0.2");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all");

    // Make two iterations to check different initial bursts:
    // First iteration starts with full token bucket and the following iterations start with unknown number of tokens in bucket.
    // (Bucket should be empty, but in CI it may be again full, so the test is the same)
    for (int iteration = 0; iteration < 3; ++iteration)
    {
        DBG_PRINT("--- Start #{} ---", iteration);
        std::vector<TestQueryPtr> queries;
        for (int query = 0; query < 2; query++)
            queries.push_back(std::make_shared<TestQuery>(t));

        queries[0]->start(TestQuery::AllocateLease, "A", 8);
        queries[1]->start(TestQuery::AllocateLease, "B", 8);
        DBG_PRINT("--- Check throttling ---");
        ThreadMetricsTester()
            .expectShare(&queries[0]->metrics, 0.5)
            .expectShare(&queries[1]->metrics, 0.5)
            .expectThrottling(2, 0.2, 8, 2, CPULeaseSettings::default_quantum_ns)
            .runAtLeast(0.1)
            .check();
        DBG_PRINT("--- Stop ---");
    }

    t.wait();
}

TEST(SchedulerWorkloadResourceManager, PreemptiveCPUSchedulingThrottlingAndFairness)
{
    ResourceTest t;

    t.query("CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)");
    t.query("CREATE WORKLOAD all SETTINGS max_concurrent_threads = 8");
    t.query("CREATE WORKLOAD A IN all SETTINGS max_cpus = 2");
    t.query("CREATE WORKLOAD B IN all");
    t.query("CREATE WORKLOAD C IN all");

    // Make two iterations to check different initial bursts:
    // First iteration starts with full token bucket and the following iterations start with unknown number of tokens in bucket.
    // (Bucket should be empty, but in CI it may be again full, so the test is the same)
    for (int iteration = 0; iteration < 3; ++iteration)
    {
        DBG_PRINT("--- Start #{} ---", iteration);
        std::vector<TestQueryPtr> queries;
        for (int query = 0; query < 3; query++)
            queries.push_back(std::make_shared<TestQuery>(t));

        queries[0]->start(TestQuery::AllocateLease, "A", 8);
        queries[1]->start(TestQuery::AllocateLease, "B", 8);
        queries[2]->start(TestQuery::AllocateLease, "C", 8);
        DBG_PRINT("--- Check throttling and shares ---");
        ThreadMetricsTester()
            .expectThrottling(&queries[0]->metrics, 2, 0.01, 8, 1, CPULeaseSettings::default_quantum_ns, true)
            .expectShare(&queries[1]->metrics, 0.5)
            .expectShare(&queries[2]->metrics, 0.5)
            .runAtLeast(0.1)
            .check();
        DBG_PRINT("--- Stop ---");
    }

    t.wait();
}
