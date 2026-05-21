#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>
#include <gtest/gtest.h>
#include <thread>
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
