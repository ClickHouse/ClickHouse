#include <gtest/gtest.h>

#include <Common/Scheduler/Nodes/tests/ResourceTest.h>

#include <Common/Scheduler/Nodes/CustomResourceManager.h>
#include <Poco/Util/XMLConfiguration.h>

using namespace DB;

using ResourceTest = ResourceTestManager<CustomResourceManager>;
using TestGuard = ResourceTest::Guard;

TEST(SchedulerCustomResourceManager, Smoke)
{
    ResourceTest t;

    t.update(R"CONFIG(
        <clickhouse>
            <resources>
                <res1>
                    <node path="/"><type>inflight_limit</type><max_requests>10</max_requests></node>
                    <node path="/fair"><type>fair</type></node>
                    <node path="/fair/A"><type>fifo</type></node>
                    <node path="/fair/B"><type>fifo</type><weight>3</weight></node>
                </res1>
            </resources>
            <workload_classifiers>
                <A><res1>/fair/A</res1></A>
                <B><res1>/fair/B</res1></B>
            </workload_classifiers>
        </clickhouse>
    )CONFIG");

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

TEST(SchedulerCustomResourceManager, Fairness)
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

    t.update(R"CONFIG(
        <clickhouse>
            <resources>
                <res1>
                    <node path="/">           <type>inflight_limit</type><max_requests>1</max_requests></node>
                    <node path="/fair">       <type>fair</type></node>
                    <node path="/fair/A">     <type>fifo</type></node>
                    <node path="/fair/B">     <type>fifo</type></node>
                    <node path="/fair/leader"><type>fifo</type></node>
                </res1>
            </resources>
            <workload_classifiers>
                <A><res1>/fair/A</res1></A>
                <B><res1>/fair/B</res1></B>
                <leader><res1>/fair/leader</res1></leader>
            </workload_classifiers>
        </clickhouse>
    )CONFIG");

    for (int thread = 0; thread < threads_per_queue; thread++)
    {
        t.threads.emplace_back([&]
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
        t.threads.emplace_back([&]
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
}
