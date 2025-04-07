#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <Core/Settings.h>

#include <Common/setThreadName.h>
#include <Common/Scheduler/CPUSlotsAllocation.h>
#include <Common/Scheduler/Nodes/tests/ResourceTest.h>
#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>
#include <Common/Scheduler/Nodes/WorkloadResourceManager.h>

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

using namespace DB;

class WorkloadEntityTestStorage : public WorkloadEntityStorageBase
{
public:
    WorkloadEntityTestStorage()
        : WorkloadEntityStorageBase(Context::getGlobalContextInstance())
    {}

    void loadEntities() override {}

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

    std::barrier sync_before_enqueue(2);
    std::barrier sync_before_drop(3);
    std::barrier sync_after_drop(2);
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
    std::barrier sync_before_enqueue(2);
    std::barrier sync_before_drop(2 + queue_size);
    std::barrier sync_after_drop(2);
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

// It emulates how PipelineExecutor interacts with CPU scheduler
struct TestQuery {
    ResourceTest & t;

    std::mutex slots_mutex;
    std::shared_ptr<CPUSlotsAllocation> slots;

    std::mutex mutex;
    std::condition_variable cv;
    size_t max_threads = 1;
    size_t threads_finished = 0;
    size_t active_threads = 0;
    size_t started_threads = 0;
    bool query_is_finished = false;
    UInt64 work_left = UInt64(-1);
    String name;

    ThreadFromGlobalPool * master_thread = nullptr;

    std::mutex worker_threads_mutex;
    std::vector<ThreadFromGlobalPool *> worker_threads;

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

    void joinWorkerThreads()
    {
        while (true)
        {
            std::vector<ThreadFromGlobalPool *> threads_to_join;
            {
                std::scoped_lock lock{worker_threads_mutex};
                threads_to_join.swap(worker_threads);
            }
            if (threads_to_join.empty())
                break;
            for (ThreadFromGlobalPool * thread : threads_to_join)
            {
                if (thread->joinable())
                    thread->join();
            }
            // We have to repeat because threads we have just joined could have created new threads in the meantime
        }
    }

    void upscaleIfPossible()
    {
        while (auto slot = slots->tryAcquire())
        {
            ThreadFromGlobalPool * thread = t.async([this, my_slot = std::move(slot)] mutable
            {
                threadFunc(std::move(my_slot));
            });

            std::scoped_lock lock{worker_threads_mutex};
            worker_threads.push_back(thread);
        }
    }

    void finish()
    {
        std::unique_lock lock{mutex};
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
    size_t onThreadStart()
    {
        std::scoped_lock lock{mutex};
        active_threads++;
        started_threads++;
        cv.notify_all();
        return started_threads - 1;
    }

    void onThreadStop()
    {
        std::scoped_lock lock{mutex};
        active_threads--;
    }

    // Returns true iff query should continue execution
    bool doWork(size_t thread_num)
    {
        std::unique_lock lock{mutex};

        // Take one piece of work to do for the next 10 us
        if (work_left > 0)
            work_left--;

        // Emulate work with waiting on cv
        bool timeout = !cv.wait_for(lock, std::chrono::microseconds(us_per_work), [=, this]
        {
            return query_is_finished
                || work_left == 0
                || (thread_num < threads_finished // When finish not the last thread make sure query will have at least 1 active thread afterwards
                    && (active_threads > 1 || threads_finished == max_threads));
        });
        return timeout;
    }

    void threadFunc(AcquiredSlotPtr self_slot)
    {
        chassert(self_slot);

        size_t thread_num = onThreadStart();
        SCOPE_EXIT({ onThreadStop(); });

        setThreadName(fmt::format("name.{}", name, thread_num).c_str());
        while (true)
        {
            upscaleIfPossible();
            if (!doWork(thread_num))
                break;
        }
    }

    void start(String workload, SlotCount max_threads_, UInt64 runtime_us = UInt64(-1))
    {
        std::scoped_lock lock{mutex};
        name = workload;
        max_threads = max_threads_;
        if (runtime_us != UInt64(-1))
            work_left = runtime_us / us_per_work;
        master_thread = t.async(workload, t.storage.getMasterThreadResourceName(), t.storage.getWorkerThreadResourceName(),
            [&, workload] (ResourceLink master_link, ResourceLink worker_link)
            {
                setThreadName(workload.c_str());
                {
                    std::scoped_lock lock2{slots_mutex};
                    slots = std::make_shared<CPUSlotsAllocation>(1, max_threads - 1, master_link, worker_link);
                }
                threadFunc(slots->acquire());

                // We have to keep this thread alive even when threadFunc() is finished
                // because `slots` keep references to ProfileEvent stored in thread-local storage.
                // PipelineExecutor master thread does similar thing by joining all additional threads during finalize.
                {
                    std::unique_lock lock3{mutex};
                    cv.wait(lock3, [this] () { return query_is_finished; });
                }

                // Acquired slot holds a reference to the allocation, we need to wait for release of all acquired slots
                joinWorkerThreads();

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
    std::barrier sync_start(2);

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
