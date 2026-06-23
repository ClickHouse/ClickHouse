#include <Common/Clusters/ClusterFactory.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Poco/Util/XMLConfiguration.h>

#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ClusterFactory & ClusterFactory::instance()
{
    static ClusterFactory factory;
    return factory;
}

ClusterFactory::~ClusterFactory()
{
    shutdown();
}

void ClusterFactory::initialize()
{
    auto global_context = Context::getGlobalContextInstance();
    if (!global_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory::initialize called before global context exists");

    const bool catalog_configured = global_context->getConfigRef().has("cluster_metadata");
    if (catalog_configured)
    {
        LOG_INFO(
            log,
            "ClusterFactory initialized; SQL cluster metadata will be published from `ClusterMetadataManager`");
    }
    else
    {
        LOG_INFO(
            log,
            "ClusterFactory initialized without SQL cluster metadata: `cluster_metadata` is "
            "not configured. CREATE / ALTER / DROP SHARD, ENDPOINT and CLUSTER DDL are disabled; only "
            "`remote_servers` and cluster discovery clusters are available");
    }
}

void ClusterFactory::shutdown()
{
    shutdown_called.store(true);

    std::lock_guard writer_lock(clusters_writer_mutex);
    clusters_state.set(std::make_unique<ClustersSnapshot>());
}

void ClusterFactory::replaceSQLCatalogClusters(const std::map<String, ClusterPtr> & clusters)
{
    std::unordered_set<String> new_names;
    new_names.reserve(clusters.size());
    for (const auto & [name, _] : clusters)
        new_names.insert(name);

    std::lock_guard writer_lock(clusters_writer_mutex);
    auto current = clusters_state.get();

    auto global_context = Context::getGlobalContextInstance();
    std::shared_ptr<Clusters> builder;
    if (current && current->clusters)
        builder = std::make_shared<Clusters>(*current->clusters);
    else
    {
        if (!global_context)
            return;
        Poco::AutoPtr<Poco::Util::XMLConfiguration> empty_config(new Poco::Util::XMLConfiguration);
        builder = std::make_shared<Clusters>(*empty_config, global_context->getSettingsRef(), global_context->getMacros(), "");
    }

    std::vector<String> to_remove;
    for (const auto & [name, cluster] : builder->getContainer())
    {
        if (cluster && cluster->getDefinitionSource() == ClusterDefinitionSource::SQLCatalog && !new_names.contains(name))
            to_remove.push_back(name);
    }

    for (const auto & name : to_remove)
        builder->removeClusterEntry(name);

    for (const auto & [name, cluster] : clusters)
    {
        if (!cluster)
            continue;

        auto existing = builder->getCluster(name);
        if (existing && existing->getDefinitionSource() == ClusterDefinitionSource::RemoteServersConfig)
            continue;

        const UInt64 version = cluster->getDefinitionSource() == ClusterDefinitionSource::SQLCatalog
            ? cluster->getDefinitionVersion()
            : 0;
        cluster->setDefinitionMetadata(ClusterDefinitionSource::SQLCatalog, version);
        builder->addCluster(name, cluster);
    }

    publishClustersSnapshotLocked(
        std::move(builder),
        current ? current->clusters_config : nullptr,
        current ? current->clusters_version : 0);
}

bool ClusterFactory::hasCluster(const String & name) const
{
    auto snap = clusters_state.get();
    return snap && snap->hasCluster(name);
}

void ClusterFactory::publishClustersSnapshotLocked(
    std::shared_ptr<Clusters> new_clusters,
    ConfigurationPtr new_config,
    size_t new_version)
{
    auto snap = std::make_unique<ClustersSnapshot>();
    snap->clusters = std::move(new_clusters);
    snap->clusters_config = new_config;
    snap->clusters_version = new_version;
    clusters_state.set(std::move(snap));
}

std::shared_ptr<Clusters> ClusterFactory::cloneClustersForWriteLocked(
    const Settings & settings,
    MultiVersion<Macros>::Version macros_snapshot,
    const std::shared_ptr<const ClustersSnapshot> & current)
{
    if (current && current->clusters)
        return std::make_shared<Clusters>(*current->clusters);

    Poco::AutoPtr<Poco::Util::XMLConfiguration> empty_config(new Poco::Util::XMLConfiguration);
    return std::make_shared<Clusters>(*empty_config, settings, macros_snapshot, "");
}

void ClusterFactory::setCluster(const String & cluster_name, ClusterPtr cluster, ClusterDefinitionSource source)
{
    if (!cluster)
        return;

    auto global_context = Context::getGlobalContextInstance();
    if (!global_context)
        return;

    std::lock_guard writer_lock(clusters_writer_mutex);
    auto current = clusters_state.get();

    if (source != ClusterDefinitionSource::RemoteServersConfig && current && current->clusters)
    {
        if (auto existing = current->clusters->getCluster(cluster_name);
            existing && existing->getDefinitionSource() == ClusterDefinitionSource::RemoteServersConfig)
        {
            LOG_DEBUG(
                log,
                "Refusing to upsert cluster `{}` from source `{}`: already defined in `<remote_servers>` config",
                cluster_name,
                static_cast<int>(source));
            return;
        }
    }

    auto builder = cloneClustersForWriteLocked(global_context->getSettingsRef(), global_context->getMacros(), current);

    cluster->setDefinitionMetadata(source, /*version*/ 0);
    builder->addCluster(cluster_name, cluster);

    publishClustersSnapshotLocked(
        std::move(builder),
        current ? current->clusters_config : nullptr,
        current ? current->clusters_version : 0);
}

void ClusterFactory::removeCluster(const String & cluster_name, ClusterDefinitionSource source)
{
    std::lock_guard writer_lock(clusters_writer_mutex);
    auto current = clusters_state.get();
    if (!current || !current->clusters)
        return;

    auto existing = current->clusters->getCluster(cluster_name);
    if (!existing || existing->getDefinitionSource() != source)
        return;

    auto builder = std::make_shared<Clusters>(*current->clusters);
    builder->removeClusterEntry(cluster_name);

    publishClustersSnapshotLocked(std::move(builder), current->clusters_config, current->clusters_version);
}

void ClusterFactory::removeCluster(const String & cluster_name)
{
    std::lock_guard writer_lock(clusters_writer_mutex);
    auto current = clusters_state.get();
    if (!current || !current->clusters)
        return;

    auto builder = std::make_shared<Clusters>(*current->clusters);
    builder->removeClusterEntry(cluster_name);

    publishClustersSnapshotLocked(std::move(builder), current->clusters_config, current->clusters_version);
}

size_t ClusterFactory::getClustersVersion() const
{
    auto snap = clusters_state.get();
    return snap ? snap->clusters_version : 0;
}

bool ClusterFactory::isClusterDefinedOnlyInRemoteServers(const String & cluster_name) const
{
    auto snap = clusters_state.get();
    if (!snap || !snap->clusters)
        return false;
    auto cluster = snap->clusters->getCluster(cluster_name);
    return cluster && cluster->getDefinitionSource() == ClusterDefinitionSource::RemoteServersConfig;
}

void ClusterFactory::registerCatalogClustersInto(Clusters & builder) const
{
    auto snap = clusters_state.get();
    if (!snap || !snap->clusters)
        return;

    for (const auto & [name, cluster] : snap->clusters->getContainer())
    {
        if (!cluster || cluster->getDefinitionSource() != ClusterDefinitionSource::SQLCatalog)
            continue;
        if (!builder.hasCluster(name))
            builder.addCluster(name, cluster);
    }
}

ClusterPtr ClusterFactory::tryGetCluster(const String & cluster_name) const
{
    auto snap = clusters_state.get();
    if (!snap || !snap->clusters)
        return nullptr;
    return snap->clusters->getCluster(cluster_name);
}

std::map<String, ClusterPtr> ClusterFactory::getClusters() const
{
    auto snap = clusters_state.get();
    if (!snap || !snap->clusters)
        return {};
    return snap->clusters->getContainer();
}

void ClusterFactory::applyClustersConfig(
    const ConfigurationPtr & config,
    const Settings & settings,
    MultiVersion<Macros>::Version macros_snapshot,
    const String & config_name,
    ContextPtr)
{
    std::lock_guard writer_lock(clusters_writer_mutex);
    auto current = clusters_state.get();

    if (current && current->clusters && current->clusters_config
        && isSameConfiguration(*config, *current->clusters_config, config_name))
        return;

    std::shared_ptr<Clusters> builder;
    if (current && current->clusters)
    {
        builder = std::make_shared<Clusters>(*current->clusters);
        auto old_config = current->clusters_config;
        builder->mergeConfigClusters(*config, settings, config_name, old_config.get());
    }
    else
    {
        builder = std::make_shared<Clusters>(*config, settings, macros_snapshot, config_name);
    }

    registerCatalogClustersInto(*builder);

    const size_t new_version = (current ? current->clusters_version : 0) + 1;
    publishClustersSnapshotLocked(std::move(builder), config, new_version);
}

void ClusterFactory::reloadClustersConfig(ContextPtr context)
{
    if (!context)
        return;

    static constexpr std::string_view config_prefix = "remote_servers";

    ConfigurationPtr pinned_config;
    std::vector<String> config_cluster_names;
    {
        auto snap = clusters_state.get();
        if (snap)
        {
            pinned_config = snap->clusters_config;
            if (snap->clusters)
            {
                for (const auto & [name, cluster] : snap->clusters->getContainer())
                {
                    if (cluster && cluster->getDefinitionSource() == ClusterDefinitionSource::RemoteServersConfig)
                        config_cluster_names.push_back(name);
                }
            }
        }
    }

    const auto & config = pinned_config ? *pinned_config : context->getConfigRef();
    const auto & settings = context->getSettingsRef();

    std::vector<std::pair<String, std::shared_ptr<Cluster>>> rebuilt;
    rebuilt.reserve(config_cluster_names.size());
    for (const auto & name : config_cluster_names)
    {
        if (!config.has(String(config_prefix) + "." + name))
            continue;
        rebuilt.emplace_back(name, std::make_shared<Cluster>(config, settings, String(config_prefix), name));
    }

    std::lock_guard writer_lock(clusters_writer_mutex);
    auto current = clusters_state.get();
    if (!current || !current->clusters || current->clusters_config.get() != pinned_config.get())
        return;

    auto builder = std::make_shared<Clusters>(*current->clusters);

    for (auto & [name, cluster] : rebuilt)
    {
        auto existing = builder->getCluster(name);
        if (!existing || existing->getDefinitionSource() != ClusterDefinitionSource::RemoteServersConfig)
            continue;
        builder->addCluster(name, cluster);
    }

    publishClustersSnapshotLocked(std::move(builder), current->clusters_config, current->clusters_version);
}

}
