#include <gtest/gtest.h>

#include <Common/Scheduler/SchedulerRoot.h>

#include <Common/Scheduler/Nodes/tests/ResourceTest.h>

#include <barrier>
#include <future>

using namespace DB;

struct ResourceTest : public ResourceTestBase
{
    SchedulerRoot scheduler;

    ResourceTest()
    {
        scheduler.start();
    }

    ~ResourceTest()
    {
        scheduler.stop(true);
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

    template <class TClass>
    TClass * add(const String & path, const String & xml = {})
    {
        return ResourceTest::add<TClass>(t.scheduler.event_queue, root_node, path, xml);
    }

    ResourceLink addQueue(const String & path, const String & xml = {})
    {
        return {.queue = static_cast<ISchedulerQueue *>(ResourceTest::add<FifoQueue>(t.scheduler.event_queue, root_node, path, xml))};
    }

    void registerResource()
    {
        std::promise<void> p;
        auto f = p.get_future();
        t.scheduler.event_queue->enqueue([this, &p]
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
        t.scheduler.event_queue->enqueue([this, &p]
        {
            t.scheduler.removeChild(root_node.get());
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
};

TEST(SchedulerRoot, Smoke)
{
    ResourceTest t;

    ResourceHolder r1(t);
    auto * fc1 = r1.add<ConstraintTest>("/", "<max_requests>1</max_requests>");
    r1.add<PriorityPolicy>("/prio");
    auto a = r1.addQueue("/prio/A", "<priority>1</priority>");
    auto b = r1.addQueue("/prio/B", "<priority>2</priority>");
    r1.registerResource();

    ResourceHolder r2(t);
    auto * fc2 = r2.add<ConstraintTest>("/", "<max_requests>1</max_requests>");
    r2.add<PriorityPolicy>("/prio");
    auto c = r2.addQueue("/prio/C", "<priority>-1</priority>");
    auto d = r2.addQueue("/prio/D", "<priority>-2</priority>");
    r2.registerResource();

    {
        ResourceGuard rg(a);
        EXPECT_TRUE(fc1->requests.contains(&rg.request));
    }

    {
        ResourceGuard rg(b);
        EXPECT_TRUE(fc1->requests.contains(&rg.request));
    }

    {
        ResourceGuard rg(c);
        EXPECT_TRUE(fc2->requests.contains(&rg.request));
    }

    {
        ResourceGuard rg(d);
        EXPECT_TRUE(fc2->requests.contains(&rg.request));
    }
}

TEST(SchedulerRoot, Cancel)
{
    ResourceTest t;

    ResourceHolder r1(t);
    auto * fc1 = r1.add<ConstraintTest>("/", "<max_requests>1</max_requests>");
    r1.add<PriorityPolicy>("/prio");
    auto a = r1.addQueue("/prio/A", "<priority>1</priority>");
    auto b = r1.addQueue("/prio/B", "<priority>2</priority>");
    r1.registerResource();

    std::barrier destruct_sync(2);
    std::barrier sync(2);
    std::thread consumer1([&]
    {
        MyRequest request(1,[&]
        {
            sync.arrive_and_wait(); // (A)
            EXPECT_TRUE(fc1->requests.contains(&request));
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

    EXPECT_TRUE(fc1->requests.empty());
}
