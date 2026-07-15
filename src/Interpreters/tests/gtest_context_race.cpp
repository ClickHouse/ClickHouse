#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>
#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <vector>
#include <sstream>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

using namespace DB;

template <typename Ptr>
void run(Ptr context)
{
    for (size_t i = 0; i < 100; ++i)
    {
        std::thread t1([context]
        {
            if constexpr (std::is_same_v<ContextWeakPtr, Ptr>)
                context.lock()->getAsyncReadCounters();
            else
                context->getAsyncReadCounters();
        });

        std::thread t2([context]
        {
            Context::createCopy(context);
        });

        t1.join();
        t2.join();
    }
}

TEST(Context, MutableRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    run<ContextMutablePtr>(context);
}

TEST(Context, ConstRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    run<ContextPtr>(context);
}

TEST(Context, WeakRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    run<ContextWeakPtr>(context);
}

/// Test for data race in Context::getAccess() where need_recalculate_access
/// was written under a shared lock while being read by another thread.
/// Multiple threads call getAccess() on the same context while another thread
/// toggles need_recalculate_access via setSetting with an access-dependent setting.
TEST(Context, GetAccessRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    /// Populate the cached access object.
    context->getAccess();

    constexpr size_t num_reader_threads = 4;
    constexpr size_t num_iterations = 1000;
    std::atomic<bool> stop{false};

    /// Reader threads: call getAccess() concurrently on the same context.
    std::vector<std::thread> readers;
    for (size_t i = 0; i < num_reader_threads; ++i)
    {
        readers.emplace_back([&context, &stop]
        {
            while (!stop.load(std::memory_order_relaxed))
                context->getAccess();
        });
    }

    /// Writer thread: toggle need_recalculate_access by setting allow_ddl
    /// (one of the three settings in ContextAccessParams::dependsOnSettingName).
    std::thread writer([&context, &stop]
    {
        for (size_t i = 0; i < num_iterations; ++i)
            context->setSetting("allow_ddl", Field(UInt64(1)));
        stop.store(true, std::memory_order_relaxed);
    });

    writer.join();
    for (auto & t : readers)
        t.join();
}

/// Regression test for a startup race between DNSCacheUpdater and ConfigReloader.
///
/// DNSCacheUpdater::run() can call Context::reloadClusterConfig() before the first
/// ConfigReloader pass stores a ConfigurationPtr into shared->clusters_config.
/// reloadClusterConfig() falls back to getConfigRef() and sets shared->clusters (non-null)
/// but leaves shared->clusters_config null. When setClustersConfig() subsequently runs
/// it used to dereference shared->clusters_config unconditionally whenever shared->clusters
/// was non-null, throwing Poco::NullPointerException.
///
/// The fix adds a shared->clusters_config null-guard before the isSameConfiguration() call.
/// This test reproduces the exact call order deterministically (no real threading needed)
/// and fails without the fix.
TEST(Context, SetClustersConfigAfterReloadClusterConfig)
{
    auto context = Context::createCopy(getContext().context);

    /// Simulate DNSCacheUpdater firing before ConfigReloader: this populates
    /// shared->clusters via the getConfigRef() fallback, leaving clusters_config null.
    ASSERT_NO_THROW(context->reloadClusterConfig());

    /// Now simulate the first ConfigReloader pass calling setClustersConfig().
    /// Without the fix this throws Poco::NullPointerException because shared->clusters
    /// is non-null but shared->clusters_config is still null.
    std::istringstream config_stream{"<clickhouse><remote_servers/></clickhouse>"};
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(config_stream);
    ASSERT_NO_THROW(context->setClustersConfig(config, /*enable_discovery=*/false));

    /// Verify the recovered state is sane: getClusters() must not throw and must
    /// reflect the config we just applied (empty <remote_servers> → empty map).
    std::map<String, ClusterPtr> clusters;
    ASSERT_NO_THROW(clusters = context->getClusters());
    EXPECT_TRUE(clusters.empty());
}
