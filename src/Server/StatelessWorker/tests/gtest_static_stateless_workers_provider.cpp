#include <gtest/gtest.h>

#include <Server/StatelessWorker/StatelessWorkerAllocation.h>
#include <Server/StatelessWorker/StaticStatelessWorkersProvider.h>
#include <Server/StatelessWorker/StatelessWorkersProviderRegistry.h>
#include <Interpreters/Context.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/tests/gtest_global_context.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

#include <map>
#include <string>

namespace ProfileEvents
{
    extern const Event StatelessWorkerProvided;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// Static providers never wait, so the provisioning budget passed to `allocate` is irrelevant.
constexpr uint64_t PROVISIONING_TIMEOUT_MS = 30000;

/// Install `inner_xml` (a `<stateless_worker_client>` block) as the global server configuration,
/// so the static provider and the registry — which both read `Context::getGlobalContextInstance()`
/// — resolve against it.
void applyStatelessWorkerConfig(const std::string & inner_xml)
{
    const std::string xml = "<clickhouse>\n" + inner_xml + "</clickhouse>\n";
    DB::WriteBufferFromFile f("./sw_provider_config_for_test.xml");
    DB::writeText(xml, f);
    f.finalize();
    DB::ConfigProcessor processor("./sw_provider_config_for_test.xml", true, true);
    auto loaded = processor.loadConfig(false);
    getContext().context->setConfig(loaded.configuration);
    /// Parse any `<remote_servers>` into the context's clusters so the static provider's `<cluster>`
    /// form can resolve them via tryGetCluster (harmless when the config defines none).
    getContext().context->setClustersConfig(loaded.configuration);
}

}

/// A static cluster's `<host>` form serves exactly its configured host, carries no auth token,
/// and ignores the requested worker count.
TEST(StaticStatelessWorkersProvider, HostFormServesConfiguredHostAuthlessIgnoringCount)
{
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <stat><host>stat-host</host></stat>
        </clusters>
    </stateless_worker_client>
)");
    StaticStatelessWorkersProvider provider(getContext().context->getConfigRef(), "stateless_worker_client.clusters.stat");

    const auto provided_before = ProfileEvents::global_counters[ProfileEvents::StatelessWorkerProvided];
    auto one = provider.allocate(0, PROVISIONING_TIMEOUT_MS);
    ASSERT_EQ(one->getAddresses().size(), 1u);
    EXPECT_EQ(one->getAddresses()[0].host, "stat-host");
    EXPECT_EQ(one->getAuthToken(), "");
    /// A static allocation has no worker_ids; the provided count must still reflect its addresses.
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::StatelessWorkerProvided] - provided_before, 1u);

    auto many = provider.allocate(5, PROVISIONING_TIMEOUT_MS);
    ASSERT_EQ(many->getAddresses().size(), 1u);
    EXPECT_EQ(many->getAddresses()[0].host, "stat-host");
}

/// A cluster that names no worker source cannot provide workers.
TEST(StaticStatelessWorkersProvider, NoWorkerSourceThrows)
{
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <empty><enabled>1</enabled></empty>
        </clusters>
    </stateless_worker_client>
)");
    /// Resolution happens in the constructor, so a cluster with no worker source fails when built.
    EXPECT_THROW(StaticStatelessWorkersProvider(getContext().context->getConfigRef(), "stateless_worker_client.clusters.empty"), Exception);
}

/// A config reload that reconfigures a cluster retires its provider from the live map, so new queries
/// resolve a freshly built one — while a query still holding the old provider keeps it alive and drains.
TEST(StatelessWorkersProviderRegistry, ReloadRetiresReconfiguredClusterButHoldersKeepTheOldProvider)
{
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <foo><host>host-a</host></foo>
        </clusters>
    </stateless_worker_client>
)");

    StatelessWorkersProviderRegistry registry;
    auto p1 = registry.getProvider("foo");
    ASSERT_TRUE(p1);
    ASSERT_TRUE(registry.tryGetProvider("foo"));

    /// A query holds an allocation from the original cluster across the reload.
    auto a1 = p1->allocate(0, PROVISIONING_TIMEOUT_MS);
    ASSERT_EQ(a1->getAddresses().size(), 1u);
    EXPECT_EQ(a1->getAddresses()[0].host, "host-a");

    /// Change foo's worker source (host-a -> host-b): its config signature changes, so the reload retires it.
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <foo><host>host-b</host></foo>
        </clusters>
    </stateless_worker_client>
)");
    registry.reloadConfig(getContext().context->getConfigRef());

    EXPECT_FALSE(registry.tryGetProvider("foo"));   /// retired from the live map
    /// The in-flight allocation keeps the original provider alive and still resolves the old hosts...
    EXPECT_TRUE(p1);
    ASSERT_EQ(a1->getAddresses().size(), 1u);
    EXPECT_EQ(a1->getAddresses()[0].host, "host-a");

    /// ...while a fresh lookup builds a new provider that serves the new config.
    auto p2 = registry.getProvider("foo");
    EXPECT_NE(p1.get(), p2.get());
    auto a2 = p2->allocate(0, PROVISIONING_TIMEOUT_MS);
    ASSERT_EQ(a2->getAddresses().size(), 1u);
    EXPECT_EQ(a2->getAddresses()[0].host, "host-b");
}

/// A built static provider reports its workers to `system.stateless_workers` (status STATIC, cached,
/// zero refcount) so it is distinguishable from a not-yet-built provider's empty state.
TEST(StaticStatelessWorkersProvider, ReportsWorkersStateForIntrospection)
{
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <stat><host>stat-host</host></stat>
        </clusters>
    </stateless_worker_client>
)");
    StaticStatelessWorkersProvider provider(getContext().context->getConfigRef(), "stateless_worker_client.clusters.stat");
    const auto state = provider.getWorkersState();
    ASSERT_EQ(state.size(), 1u);
    EXPECT_EQ(state[0].hostname, "stat-host");
    EXPECT_EQ(state[0].status, "STATIC");
    EXPECT_TRUE(state[0].cached);
    EXPECT_EQ(state[0].refcount, 0u);
}

/// The static provider's `<cluster>` form reads the single shard's replicas (with their stateless
/// worker / streaming-exchange ports) from `<remote_servers>`.
TEST(StaticStatelessWorkersProvider, ClusterFormUsesSingleShardReplicas)
{
    applyStatelessWorkerConfig(R"(
    <remote_servers>
        <sw_static>
            <shard>
                <replica>
                    <host>replica-host</host>
                    <port>9000</port>
                    <stateless_worker_port>9100</stateless_worker_port>
                    <streaming_exchange_port>9200</streaming_exchange_port>
                </replica>
            </shard>
        </sw_static>
    </remote_servers>
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <statc><cluster>sw_static</cluster></statc>
        </clusters>
    </stateless_worker_client>
)");
    StaticStatelessWorkersProvider provider(getContext().context->getConfigRef(), "stateless_worker_client.clusters.statc");
    auto alloc = provider.allocate(0, PROVISIONING_TIMEOUT_MS);
    ASSERT_EQ(alloc->getAddresses().size(), 1u);
    EXPECT_EQ(alloc->getAddresses()[0].host, "replica-host");
    EXPECT_EQ(alloc->getAddresses()[0].stateless_worker_port, 9100);
    EXPECT_EQ(alloc->getAddresses()[0].streaming_exchange_port, 9200);
}

/// Only a single-shard worker cluster is supported; a multi-shard `<remote_servers>` cluster is rejected.
TEST(StaticStatelessWorkersProvider, ClusterFormRejectsMultipleShards)
{
    applyStatelessWorkerConfig(R"(
    <remote_servers>
        <sw_multishard>
            <shard><replica><host>h1</host><port>9000</port></replica></shard>
            <shard><replica><host>h2</host><port>9000</port></replica></shard>
        </sw_multishard>
    </remote_servers>
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <multishard><cluster>sw_multishard</cluster></multishard>
        </clusters>
    </stateless_worker_client>
)");
    /// The single-shard guard runs in the constructor, so a multi-shard cluster fails when built.
    EXPECT_THROW(StaticStatelessWorkersProvider(getContext().context->getConfigRef(), "stateless_worker_client.clusters.multishard"), Exception);
}

/// A static provider is an immutable snapshot of its config: after a reload changes the host, the
/// already-built provider still allocates the host it was built for, while the registry builds a new
/// provider that serves the new host.
TEST(StaticStatelessWorkersProvider, SnapshotsWorkersAtBuildAcrossReload)
{
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <stat><host>host-a</host></stat>
        </clusters>
    </stateless_worker_client>
)");
    StatelessWorkersProviderRegistry registry;
    auto p1 = registry.getProvider("stat");
    ASSERT_TRUE(p1);
    auto a1 = p1->allocate(0, PROVISIONING_TIMEOUT_MS);
    ASSERT_EQ(a1->getAddresses().size(), 1u);
    EXPECT_EQ(a1->getAddresses()[0].host, "host-a");

    /// Reload to a different host; the config signature changes, so the registry retires p1.
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <stat><host>host-b</host></stat>
        </clusters>
    </stateless_worker_client>
)");
    registry.reloadConfig(getContext().context->getConfigRef());

    /// The already-built provider still serves the host it was built for, even on a fresh allocate.
    auto a1_again = p1->allocate(0, PROVISIONING_TIMEOUT_MS);
    ASSERT_EQ(a1_again->getAddresses().size(), 1u);
    EXPECT_EQ(a1_again->getAddresses()[0].host, "host-a");

    /// A fresh lookup builds a new provider that serves the new host.
    auto p2 = registry.getProvider("stat");
    EXPECT_NE(p1.get(), p2.get());
    auto a2 = p2->allocate(0, PROVISIONING_TIMEOUT_MS);
    ASSERT_EQ(a2->getAddresses().size(), 1u);
    EXPECT_EQ(a2->getAddresses()[0].host, "host-b");
}

/// The static `<cluster>` form points at a `remote_servers` entry: changing that entry's replicas while
/// the `cluster` name stays the same must still retire the snapshotted provider so new queries rebuild it.
TEST(StaticStatelessWorkersProvider, ClusterFormRetiredWhenRemoteServersChange)
{
    applyStatelessWorkerConfig(R"(
    <remote_servers>
        <sw_dyn><shard><replica><host>host-a</host><port>9000</port></replica></shard></sw_dyn>
    </remote_servers>
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters><dyn><cluster>sw_dyn</cluster></dyn></clusters>
    </stateless_worker_client>
)");
    StatelessWorkersProviderRegistry registry;
    auto p1 = registry.getProvider("dyn");
    ASSERT_TRUE(p1);
    EXPECT_EQ(p1->allocate(0, PROVISIONING_TIMEOUT_MS)->getAddresses()[0].host, "host-a");

    /// Same `cluster` name, different `remote_servers` replica → the resolved worker set changed.
    applyStatelessWorkerConfig(R"(
    <remote_servers>
        <sw_dyn><shard><replica><host>host-b</host><port>9000</port></replica></shard></sw_dyn>
    </remote_servers>
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters><dyn><cluster>sw_dyn</cluster></dyn></clusters>
    </stateless_worker_client>
)");
    registry.reloadConfig(getContext().context->getConfigRef());
    EXPECT_FALSE(registry.tryGetProvider("dyn"));   /// retired because the resolved worker set changed

    auto p2 = registry.getProvider("dyn");
    EXPECT_NE(p1.get(), p2.get());
    EXPECT_EQ(p2->allocate(0, PROVISIONING_TIMEOUT_MS)->getAddresses()[0].host, "host-b");
}

/// A lookup after the configuration changed, but before the reload reached the registry, builds a
/// provider from the new configuration instead of returning the stale cached one.
TEST(StatelessWorkersProviderRegistry, LookupRebuildsProviderChangedBeforeReload)
{
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <foo><host>host-a</host></foo>
        </clusters>
    </stateless_worker_client>
)");
    StatelessWorkersProviderRegistry registry;
    auto p1 = registry.getProvider("foo");

    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <foo><host>host-b</host></foo>
        </clusters>
    </stateless_worker_client>
)");
    auto p2 = registry.getProvider("foo");
    EXPECT_NE(p1.get(), p2.get());
    auto alloc = p2->allocate(0, PROVISIONING_TIMEOUT_MS);
    ASSERT_EQ(alloc->getAddresses().size(), 1u);
    EXPECT_EQ(alloc->getAddresses()[0].host, "host-b");
    /// An unchanged configuration keeps returning the cached provider.
    EXPECT_EQ(registry.getProvider("foo").get(), p2.get());
}

/// A reload never throws: a cluster whose new configuration cannot be resolved is retired, and the
/// error surfaces to the next query that selects it instead.
TEST(StatelessWorkersProviderRegistry, ReloadWithBrokenClusterRetiresWithoutThrowing)
{
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <foo><host>host-a</host></foo>
        </clusters>
    </stateless_worker_client>
)");
    StatelessWorkersProviderRegistry registry;
    ASSERT_TRUE(registry.getProvider("foo"));

    /// `foo` now names a `remote_servers` cluster that does not exist.
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <foo><cluster>missing</cluster></foo>
        </clusters>
    </stateless_worker_client>
)");
    EXPECT_NO_THROW(registry.reloadConfig(getContext().context->getConfigRef()));
    EXPECT_FALSE(registry.tryGetProvider("foo"));
    EXPECT_THROW(registry.getProvider("foo"), Exception);
}

/// The registry reports every provider it has built, by cluster, and builds none itself.
TEST(StatelessWorkersProviderRegistry, ListsBuiltProvidersByCluster)
{
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <host>default-host</host>
        <clusters>
            <a><host>host-a</host></a>
            <b><host>host-b</host></b>
        </clusters>
    </stateless_worker_client>
)");
    StatelessWorkersProviderRegistry registry;
    EXPECT_TRUE(registry.getBuiltProviders().empty());

    auto default_provider = registry.getProvider("");
    auto provider_a = registry.getProvider("a");
    auto built = registry.getBuiltProviders();
    ASSERT_EQ(built.size(), 2u);
    std::map<String, StatelessWorkersProviderPtr> by_cluster(built.begin(), built.end());
    EXPECT_EQ(by_cluster.at("").get(), default_provider.get());
    EXPECT_EQ(by_cluster.at("a").get(), provider_a.get());
    EXPECT_FALSE(by_cluster.contains("b"));
}

/// The default cluster is reported under its `default_cluster_name`, not the registry's internal empty key.
TEST(StatelessWorkersProviderRegistry, ListsDefaultClusterUnderItsName)
{
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <default_cluster_name>main</default_cluster_name>
        <host>default-host</host>
    </stateless_worker_client>
)");
    StatelessWorkersProviderRegistry registry;
    auto provider = registry.getProvider("main");
    auto built = registry.getBuiltProviders();
    ASSERT_EQ(built.size(), 1u);
    EXPECT_EQ(built[0].first, "main");
    EXPECT_EQ(built[0].second.get(), provider.get());
}

/// End-to-end: the registry resolves a named static cluster and allocates its workers (no discovery).
TEST(StatelessWorkersProviderRegistry, SelectsNamedStaticClusterThroughRegistry)
{
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <stat><host>static-host</host></stat>
        </clusters>
    </stateless_worker_client>
)");
    StatelessWorkersProviderRegistry registry;
    auto provider = registry.getProvider("stat");
    ASSERT_TRUE(provider);
    auto alloc = provider->allocate(0, PROVISIONING_TIMEOUT_MS);
    ASSERT_EQ(alloc->getAddresses().size(), 1u);
    EXPECT_EQ(alloc->getAddresses()[0].host, "static-host");
    EXPECT_EQ(alloc->getAuthToken(), "");
}

/// A query resolves its selected cluster's provider once and reuses it: repeated calls return the
/// same instance, and a config reload that rebuilds the cluster in the registry does not change the
/// provider the query already resolved — so one query sees one provider (and one worker set) for its
/// whole lifetime and drains on it, while a fresh query resolves the rebuilt one.
TEST(Context, ReusesOneStatelessWorkersProviderPerQuery)
{
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <scluster><host>host-a</host></scluster>
        </clusters>
    </stateless_worker_client>
)");
    /// Resolution goes through the process-global registry; sync it to this config.
    getContext().context->reloadStatelessWorkersConfig(getContext().context->getConfigRef());

    auto query_ctx = Context::createCopy(getContext().context);
    query_ctx->setSetting("distributed_plan_worker_cluster", String("scluster"));
    query_ctx->setQueryContext(query_ctx);

    auto p1 = query_ctx->getStatelessWorkersProvider();
    ASSERT_TRUE(p1);
    EXPECT_EQ(p1->getPlanningNodeCount(5), 1u); /// static → its one configured host, not the requested 5
    /// Resolved once: a second call returns the very same instance.
    EXPECT_EQ(query_ctx->getStatelessWorkersProvider().get(), p1.get());

    /// Reconfigure the cluster (host-a -> host-b) and reload: the registry rebuilds it for new queries...
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <scluster><host>host-b</host></scluster>
        </clusters>
    </stateless_worker_client>
)");
    getContext().context->reloadStatelessWorkersConfig(getContext().context->getConfigRef());

    /// ...but the query that already resolved keeps its original provider (host-a), draining consistently.
    EXPECT_EQ(query_ctx->getStatelessWorkersProvider().get(), p1.get());
    auto held_alloc = query_ctx->getStatelessWorkersProvider()->allocate(0, PROVISIONING_TIMEOUT_MS);
    ASSERT_EQ(held_alloc->getAddresses().size(), 1u);
    EXPECT_EQ(held_alloc->getAddresses()[0].host, "host-a");

    /// A fresh query resolves the rebuilt provider (host-b).
    auto fresh_ctx = Context::createCopy(getContext().context);
    fresh_ctx->setSetting("distributed_plan_worker_cluster", String("scluster"));
    fresh_ctx->setQueryContext(fresh_ctx);
    auto p2 = fresh_ctx->getStatelessWorkersProvider();
    EXPECT_NE(p2.get(), p1.get());
    auto fresh_alloc = p2->allocate(0, PROVISIONING_TIMEOUT_MS);
    ASSERT_EQ(fresh_alloc->getAddresses().size(), 1u);
    EXPECT_EQ(fresh_alloc->getAddresses()[0].host, "host-b");
}

/// A query that resolved its provider keeps allocating from it after a reload disables the client, so
/// it drains instead of failing; a new query cannot resolve a provider any more.
TEST(Context, KeepsResolvedProviderAfterClientIsDisabled)
{
    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <host>host-a</host>
    </stateless_worker_client>
)");
    getContext().context->reloadStatelessWorkersConfig(getContext().context->getConfigRef());

    auto query_ctx = Context::createCopy(getContext().context);
    query_ctx->setQueryContext(query_ctx);
    ASSERT_TRUE(query_ctx->getStatelessWorkersProvider());

    applyStatelessWorkerConfig(R"(
    <stateless_worker_client>
        <enabled>0</enabled>
        <host>host-a</host>
    </stateless_worker_client>
)");
    getContext().context->reloadStatelessWorkersConfig(getContext().context->getConfigRef());

    auto allocation = query_ctx->getOrCreateStatelessWorkerAllocation();
    ASSERT_EQ(allocation->getAddresses().size(), 1u);
    EXPECT_EQ(allocation->getAddresses()[0].host, "host-a");

    auto fresh_ctx = Context::createCopy(getContext().context);
    fresh_ctx->setQueryContext(fresh_ctx);
    EXPECT_THROW(fresh_ctx->getStatelessWorkersProvider(), Exception);
}

/// A server that configures no stateless workers refuses a distributed plan as a disabled client.
TEST(Context, RefusesProviderWhenClientIsNotConfigured)
{
    applyStatelessWorkerConfig("");
    getContext().context->reloadStatelessWorkersConfig(getContext().context->getConfigRef());

    auto query_ctx = Context::createCopy(getContext().context);
    query_ctx->setQueryContext(query_ctx);
    try
    {
        query_ctx->getStatelessWorkersProvider();
        FAIL() << "expected the provider lookup to be refused";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SUPPORT_IS_DISABLED);
    }
}

}
