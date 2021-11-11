#include <Common/DNSResolver.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ZooKeeper/Types.h>
#include "base/logger_useful.h"
#include <Core/ServerUUID.h>

#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterDiscovery.h>
#include <Interpreters/Context.h>
#include <fmt/format.h>
#include <cstddef>
#include <vector>

namespace DB
{

ClusterDiscovery::ClusterDiscovery(
    const Poco::Util::AbstractConfiguration & config,
    ContextMutablePtr context_,
    const String & config_prefix)
    : context(context_)
    , node_name(toString(ServerUUID::get()))
    , server_port(context->getTCPPort())
    , log(&Poco::Logger::get("ClusterDiscovery"))
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_prefix, config_keys);

    for (const auto & key : config_keys)
    {
        String path = config.getString(config_prefix + "." + key + ".path");
        trimRight(path, '/');
        clusters[key] = path;
    }
}

Strings ClusterDiscovery::getNodes(zkutil::ZooKeeperPtr & zk, const String & zk_root, const Strings & nodes)
{
    Strings result;
    result.reserve(nodes.size());
    for (const auto & node : nodes)
    {
        /// TODO (vdimir@): use batch request?
        if (bool ok = zk->tryGet(zk_root + "/" + node, result.emplace_back()); !ok)
        {
            result.pop_back();
            LOG_WARNING(log, "Cluster configuration was changed during update, skip nonexisting node");
        }
    }
    return result;
}

void ClusterDiscovery::updateCluster(const String & cluster_name, const String & zk_root)
{
    auto zk = context->getZooKeeper();

    auto watch_callback = [this, cluster_name, zk_root](const Coordination::WatchResponse &)
    {
        this->updateCluster(cluster_name, zk_root);
    };

    const auto nodes = zk->getChildrenWatch(zk_root, nullptr, watch_callback);

    Strings replicas = getNodes(zk, zk_root, nodes);
    if (replicas.empty())
        return;

    std::vector<std::vector<String>> shards = {replicas};

    bool secure = false;
    auto maybe_secure_port = context->getTCPPortSecure();
    auto cluster = std::make_shared<Cluster>(
        context->getSettings(),
        shards,
        context->getUserName(),
        "",
        (secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort()),
        false /* treat_local_as_remote */,
        context->getApplicationType() == Context::ApplicationType::LOCAL /* treat_local_port_as_remote */,
        secure);

    context->setCluster(cluster_name, cluster);
}

void ClusterDiscovery::start()
{
    auto zk = context->getZooKeeper();
    for (const auto & [cluster_name, zk_root] : clusters)
    {
        String node_path = zk_root + "/" + node_name;
        zk->createAncestors(node_path);

        String info = DNSResolver::instance().getHostName() + ":" + toString(server_port);

        zk->createOrUpdate(node_path, info, zkutil::CreateMode::Ephemeral);

        LOG_DEBUG(log, "Current node {} registered in cluster {}", node_name, cluster_name);

        this->updateCluster(cluster_name, zk_root);
    }
}

}
