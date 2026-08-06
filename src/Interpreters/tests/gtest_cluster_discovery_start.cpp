#include <Interpreters/ClusterDiscovery.h>
#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/ServerUUID.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>

#include <atomic>
#include <sstream>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

Poco::AutoPtr<Poco::Util::XMLConfiguration> makeDiscoveryConfig()
{
    /// Observer mode avoids ephemeral registration; initialUpdate may still fail without ZooKeeper,
    /// which is fine — startImpl() still assigns the worker thread after catching the exception.
    std::istringstream config_stream{R"(
        <clickhouse>
            <remote_servers>
                <test_cluster>
                    <discovery>
                        <path>/clickhouse/discovery/test_cluster_concurrent_start</path>
                        <observer/>
                    </discovery>
                </test_cluster>
            </remote_servers>
        </clickhouse>
    )"};
    return new Poco::Util::XMLConfiguration(config_stream);
}

}

/// Regression: concurrent start() / updateFromConfig (ensureWorkerStarted) must not
/// double-assign ThreadFromGlobalPool (which aborts if already initialized).
TEST(ClusterDiscovery, ConcurrentStartDoesNotAbort)
{
    ServerUUID::setRandomForUnitTests();

    auto context = Context::createCopy(getContext().context);
    auto config = makeDiscoveryConfig();
    auto discovery = std::make_unique<ClusterDiscovery>(*config, context, context->getMacros());

    constexpr size_t num_threads = 8;
    constexpr size_t iterations = 40;
    std::atomic<size_t> started_calls{0};

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&]
        {
            for (size_t j = 0; j < iterations; ++j)
            {
                if ((j % 2) == 0)
                    discovery->start();
                else
                    discovery->updateFromConfig(*config);
                started_calls.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto & t : threads)
        t.join();

    EXPECT_EQ(started_calls.load(), num_threads * iterations);

    /// Destructor joins the worker; surviving to here means no abort on double-start.
    discovery.reset();
}
