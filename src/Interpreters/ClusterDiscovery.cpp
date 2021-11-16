#include <base/getFQDNOrHostName.h>
#include <base/logger_useful.h>

#include <Common/DNSResolver.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/setThreadName.h>

#include <Core/ServerUUID.h>

#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterDiscovery.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace
{

constexpr size_t MAX_QUEUE_SIZE = 16;
constexpr UInt64 QUEUE_OP_TIMEOUT_MS = 1000;

fs::path getReplicasListPath(const String & zk_root)
{
    return fs::path(zk_root + "/replicas");
}

}

ClusterDiscovery::ClusterDiscovery(
    const Poco::Util::AbstractConfiguration & config,
    ContextMutablePtr context_,
    const String & config_prefix)
    : context(context_)
    , node_name(toString(ServerUUID::get()))
    , server_port(context->getTCPPort())
    , queue(std::make_shared<UpdateQueue>(MAX_QUEUE_SIZE))
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

Strings ClusterDiscovery::getNodeNames(zkutil::ZooKeeperPtr & zk,
                                       const String & zk_root,
                                       const String & cluster_name,
                                       int * version,
                                       bool set_callback)
{
    auto watch_callback = [cluster_name, queue=queue, log=log](const Coordination::WatchResponse &)
    {
        if (!queue->tryPush(cluster_name, QUEUE_OP_TIMEOUT_MS))
        {
            if (queue->isFinished())
                return;
            LOG_WARNING(log, "Cannot push update request for cluster '{}'", cluster_name);
        }
    };

    Coordination::Stat stat;
    Strings nodes = zk->getChildrenWatch(getReplicasListPath(zk_root), &stat, set_callback ? watch_callback : Coordination::WatchCallback{});
    if (version)
        *version = stat.cversion;
    return nodes;
}

Strings ClusterDiscovery::getNodes(zkutil::ZooKeeperPtr & zk, const String & zk_root, const Strings & nodes)
{
    Strings result;
    result.reserve(nodes.size());
    for (const auto & node : nodes)
    {
        bool ok = zk->tryGet(getReplicasListPath(zk_root) / node, result.emplace_back());
        if (!ok)
        {
            result.pop_back();
            LOG_WARNING(log, "Cluster configuration was changed during update, skip nonexisting node");
        }
    }
    return result;
}

bool ClusterDiscovery::updateCluster(const String & cluster_name, const String & zk_root)
{
    LOG_TRACE(log, "Updating cluster '{}'", cluster_name);

    auto zk = context->getZooKeeper();

    int start_version;
    Strings nodes = getNodeNames(zk, zk_root, cluster_name, &start_version, false);

    Strings replicas = getNodes(zk, zk_root, nodes);

    if (replicas.empty())
        return false;

    int current_version;
    getNodeNames(zk, zk_root, cluster_name, &current_version, true);

    if (current_version != start_version)
        return false;

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
    return true;
}

bool ClusterDiscovery::updateCluster(const String & cluster_name)
{
    return updateCluster(cluster_name, clusters[cluster_name]);
}

void ClusterDiscovery::start()
{
    auto zk = context->getZooKeeper();

    LOG_TRACE(log, "Starting working thread");
    main_thread = ThreadFromGlobalPool([this] { runMainThread(); });

    for (const auto & [cluster_name, zk_root] : clusters)
    {
        String node_path = getReplicasListPath(zk_root) / node_name;
        zk->createAncestors(node_path);

        String info = getFQDNOrHostName() + ":" + toString(server_port);

        zk->createOrUpdate(node_path, info, zkutil::CreateMode::Ephemeral);
        LOG_DEBUG(log, "Current node {} registered in cluster {}", node_name, cluster_name);

        updateCluster(cluster_name, zk_root);
    }
}

void ClusterDiscovery::runMainThread()
{
    // setThreadName("ClusterDiscovery");
    LOG_TRACE(log, "Worker thread started");

    while (!stop_flag)
    {
        std::string cluster_name;
        if (queue->tryPop(cluster_name, QUEUE_OP_TIMEOUT_MS))
        {
            bool ok = updateCluster(cluster_name);
            if (!ok)
            {
                LOG_WARNING(log, "Error on updating cluster '{}', configuration changed during update, will retry", cluster_name);
            }
        }
    }
    LOG_TRACE(log, "Worker thread stopped");
}

void ClusterDiscovery::shutdown()
{
    LOG_TRACE(log, "Shutting down");

    stop_flag.exchange(true);
    queue->clearAndFinish();
    if (main_thread.joinable())
        main_thread.join();
}

ClusterDiscovery::~ClusterDiscovery()
{
    ClusterDiscovery::shutdown();
}

}
