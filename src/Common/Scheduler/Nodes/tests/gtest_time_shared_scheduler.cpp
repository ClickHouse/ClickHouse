#include <gtest/gtest.h>

#include <Common/Scheduler/Nodes/SpaceShared/SpaceSharedScheduler.h>
#include <Common/Scheduler/Nodes/TimeShared/SemaphoreConstraint.h>
#include <Common/Scheduler/Nodes/TimeShared/ThrottlerConstraint.h>
#include <Common/Scheduler/Nodes/TimeShared/TimeSharedScheduler.h>
#include <Common/Scheduler/Nodes/tests/ResourceTest.h>

#include <Common/randomSeed.h>

#include <barrier>
#include <future>
#include <pcg_random.hpp>

using namespace DB;

struct ResourceTest : public ResourceTestBase
{
    TimeSharedScheduler scheduler;

    ResourceTest()
    {
        scheduler.start(ThreadName::TEST_SCHEDULER);
    }

    ~ResourceTest()
    {
        scheduler.stop(true);
    }

    std::mutex rng_mutex;
    pcg64 rng{randomSeed()};

    template <typename T>
    T randomInt(T from, T to)
    {
        std::uniform_int_distribution<T> distribution(from, to);
        std::lock_guard lock(rng_mutex);
        return distribution(rng);
    }
};

struct ResourceHolder
{
    ResourceTest & t;
    SchedulerNodePtr root_node;

    explicit ResourceHolder(ResourceTest & t_)
        : t(t_)
    {}

    ~ResourceHolder()
    {
        unregisterResource();
    }

    template <class TClass, class... Args>
    TClass * add(const String & path, Args... args)
    {
        return ResourceTest::add<TClass>(t.scheduler.event_queue, root_node, path, std::forward<Args>(args)...);
    }

    template <class... Args>
    ResourceLink addQueue(const String & path, Args... args)
    {
        return {.queue = static_cast<ISchedulerQueue *>(ResourceTest::add<FifoQueue>(t.scheduler.event_queue, root_node, path, std::forward<Args>(args)...))};
    }

    void registerResource()
    {
        std::promise<void> p;
        auto f = p.get_future();
        t.scheduler.event_queue.enqueue([this, &p]
        {
            t.scheduler.attachChild(root_node);
            p.set_value();
        });
        f.get();
    }

    void unregisterResource()
    {
        std::promise<void> p;
        auto f = p.get_future();
        t.scheduler.event_queue.enqueue([this, &p]
        {
            t.scheduler.removeChild(root_node.get());
            root_node.reset(); // Destruct the whole node tree inside the scheduler thread
            p.set_value();
        });
        f.get();
    }
};

struct MyRequest : public ResourceRequest
{
    std::function<void()> on_execute;

    explicit MyRequest(ResourceCost cost_, std::function<void()> on_execute_)
        : ResourceRequest(cost_)
        , on_execute(on_execute_)
    {}

    void execute() override
    {
        if (on_execute)
            on_execute();
    }

    void failed(const std::exception_ptr &) override
    {
        FAIL();
    }
};

TEST(SchedulerTimeShared, Smoke)
{
    ResourceTest t;

    ResourceHolder r1(t);
    auto * fc1 = r1.add<SemaphoreConstraint>("/", SchedulerNodeInfo{}, /*max_requests=*/ 1);
    r1.add<PriorityPolicy>("/prio");
    auto a = r1.addQueue("/prio/A", SchedulerNodeInfo(1.0, Priority{1}));
    auto b = r1.addQueue("/prio/B", SchedulerNodeInfo(1.0, Priority{2}));
    r1.registerResource();

    ResourceHolder r2(t);
    auto * fc2 = r2.add<SemaphoreConstraint>("/", SchedulerNodeInfo{}, /*max_requests=*/ 1);
    r2.add<PriorityPolicy>("/prio");
    auto c = r2.addQueue("/prio/C", SchedulerNodeInfo(1.0, Priority{-1}));
    auto d = r2.addQueue("/prio/D", SchedulerNodeInfo(1.0, Priority{-2}));
    r2.registerResource();

    {
        ResourceGuard rg(ResourceGuard::Metrics::getIOWrite(), a);
        EXPECT_TRUE(fc1->getInflights().first == 1);
        rg.consume(1);
    }

    {
        ResourceGuard rg(ResourceGuard::Metrics::getIOWrite(), b);
        EXPECT_TRUE(fc1->getInflights().first == 1);
        rg.consume(1);
    }

    {
        ResourceGuard rg(ResourceGuard::Metrics::getIOWrite(), c);
        EXPECT_TRUE(fc2->getInflights().first == 1);
        rg.consume(1);
    }

    {
        ResourceGuard rg(ResourceGuard::Metrics::getIOWrite(), d);
        EXPECT_TRUE(fc2->getInflights().first == 1);
        rg.consume(1);
    }
}

TEST(SchedulerTimeShared, Budget)
{
    ResourceTest t;

    ResourceHolder r1(t);
    r1.add<SemaphoreConstraint>("/", SchedulerNodeInfo{}, /*max_requests=*/ 1);
    r1.add<PriorityPolicy>("/prio");
    auto a = r1.addQueue("/prio/A");
    r1.registerResource();

    ResourceCost total_real_cost = 0;
    int total_requests = 10;
    for (int i = 0 ; i < total_requests; i++)
    {
        ResourceCost est_cost = t.randomInt(1, 10);
        ResourceCost real_cost = t.randomInt(0, 10);
        ResourceGuard rg(ResourceGuard::Metrics::getIOWrite(), a, est_cost);
        rg.consume(real_cost);
        total_real_cost += real_cost;
    }

    EXPECT_EQ(total_requests, a.queue->dequeued_requests);
    EXPECT_EQ(total_real_cost, a.queue->dequeued_cost - a.queue->getBudget());
}

TEST(SchedulerTimeShared, BudgetAfterFailedEnqueue)
{
    ResourceTest t;

    ResourceHolder r1(t);
    r1.add<SemaphoreConstraint>("/", SchedulerNodeInfo{}, /*max_requests=*/ 1);
    r1.add<PriorityPolicy>("/prio");
    auto a = r1.addQueue("/prio/A");
    r1.registerResource();

    // Create a negative budget (overconsumption debt) so that `ResourceBudget::ask` raises the next
    // request's cost above its estimation, making a leaked budget transaction observable.
    {
        ResourceGuard rg(ResourceGuard::Metrics::getIOWrite(), a, 1);
        rg.consume(10);
    }
    ResourceCost budget_before = a.queue->getBudget();
    EXPECT_LT(budget_before, 0);

    // Make `enqueueRequest` throw. The budget transaction made by `enqueueRequestUsingBudget` for the
    // failed request must be rolled back: the request never entered the scheduler and will never be
    // finished, so otherwise the debt would be lost and subsequent requests would ask for too little.
    a.queue->purgeQueue();
    EXPECT_THROW(ResourceGuard rg(ResourceGuard::Metrics::getIOWrite(), a, 1), Exception);
    EXPECT_EQ(a.queue->getBudget(), budget_before);
}

TEST(SchedulerTimeShared, Cancel)
{
    // This barrier is used in the scheduler thread, so we should not destroy it before thread in ~ResourceTest
    std::barrier<std::__empty_completion> destruct_sync(2);

    ResourceTest t;

    ResourceHolder r1(t);
    auto * fc1 = r1.add<SemaphoreConstraint>("/", SchedulerNodeInfo{}, /*max_requests=*/ 1);
    r1.add<PriorityPolicy>("/prio");
    auto a = r1.addQueue("/prio/A", SchedulerNodeInfo(1.0, Priority{1}));
    auto b = r1.addQueue("/prio/B", SchedulerNodeInfo(1.0, Priority{2}));
    r1.registerResource();

    std::barrier<std::__empty_completion> sync(2);
    std::thread consumer1([&]
    {
        MyRequest request(1,[&]
        {
            sync.arrive_and_wait(); // (A)
            EXPECT_TRUE(fc1->getInflights().first == 1);
            sync.arrive_and_wait(); // (B)
            request.finish();
            destruct_sync.arrive_and_wait(); // (C)
        });
        a.queue->enqueueRequest(&request);
        destruct_sync.arrive_and_wait(); // (C)
    });

    std::thread consumer2([&]
    {
        MyRequest request(1,[&]
        {
            FAIL() << "This request must be canceled, but instead executes";
        });
        sync.arrive_and_wait(); // (A) wait for request of consumer1 to be inside execute, so that constraint is in violated state and our request will not be executed immediately
        b.queue->enqueueRequest(&request);
        bool canceled = b.queue->cancelRequest(&request);
        EXPECT_TRUE(canceled);
        sync.arrive_and_wait(); // (B) release request of consumer1 to be finished
    });

    consumer1.join();
    consumer2.join();

    EXPECT_TRUE(fc1->getInflights().first == 0);
}
