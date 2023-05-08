#include <gtest/gtest.h>

#include <IO/Resource/tests/ResourceTest.h>

#include <IO/Resource/DynamicResourceManager.h>
#include <Poco/Util/XMLConfiguration.h>

using namespace DB;

using ResourceTest = ResourceTestManager<DynamicResourceManager>;
using TestGuard = ResourceTest::Guard;

TEST(IOResourceDynamicResourceManager, Smoke)
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
            <classifiers>
                <A><res1>/fair/A</res1></A>
                <B><res1>/fair/B</res1></B>
            </classifiers>
        </clickhouse>
    )CONFIG");

    ClassifierPtr cA = t.manager->acquire("A");
    ClassifierPtr cB = t.manager->acquire("B");

    for (int i = 0; i < 10; i++)
    {
        ResourceGuard gA(cA->get("res1"));
        ResourceGuard gB(cB->get("res1"));
    }
}

TEST(IOResourceDynamicResourceManager, Fairness)
{
    constexpr size_t T = 3; // threads per queue
    int N = 100; // requests per thread
    ResourceTest t(2 * T + 1);

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
            <classifiers>
                <A><res1>/fair/A</res1></A>
                <B><res1>/fair/B</res1></B>
                <leader><res1>/fair/leader</res1></leader>
            </classifiers>
        </clickhouse>
    )CONFIG");


    // Total cost for A and B cannot differ for more than 1 (every request has cost equal to 1).
    // Requests from A use `value = 1` and from B `value = -1` is used.
    std::atomic<Int64> unfairness = 0;
    auto fairness_diff = [&] (Int64 value)
    {
        Int64 cur_unfairness = unfairness.fetch_add(value, std::memory_order_relaxed) + value;
        EXPECT_NEAR(cur_unfairness, 0, 1);
    };

    for (int thr = 0; thr < T; thr++)
    {
        t.threads.emplace_back([&]
        {
            ClassifierPtr c = t.manager->acquire("A");
            ResourceLink link = c->get("res1");
            t.startBusyPeriod(link, 1, N);
            for (int req = 0; req < N; req++)
            {
                TestGuard g(t, link, 1);
                fairness_diff(1);
            }
        });
    }

    for (int thr = 0; thr < T; thr++)
    {
        t.threads.emplace_back([&]
        {
            ClassifierPtr c = t.manager->acquire("B");
            ResourceLink link = c->get("res1");
            t.startBusyPeriod(link, 1, N);
            for (int req = 0; req < N; req++)
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
