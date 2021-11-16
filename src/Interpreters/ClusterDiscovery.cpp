#include <algorithm>
#include <chrono>
#include <unordered_set>

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

fs::path getShardsListPath(const String & zk_root)
{
    return fs::path(zk_root + "/shards");
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
        clusters_info.emplace(key, ClusterInfo(key, path));
    }
}

/// List node in zookeper for cluster
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
    Strings nodes = zk->getChildrenWatch(getShardsListPath(zk_root), &stat, set_callback ? watch_callback : Coordination::WatchCallback{});
    if (version)
        *version = stat.cversion;
    return nodes;
}

/// Reads node information from specified zookeper nodes
ClusterDiscovery::NodesInfo ClusterDiscovery::getNodes(zkutil::ZooKeeperPtr & zk, const String & zk_root, const Strings & node_uuids)
{
    NodesInfo result;
    for (const auto & node_uuid : node_uuids)
    {
        String payload;
        bool ok = zk->tryGet(getShardsListPath(zk_root) / node_uuid, payload);
        if (!ok)
        {
            LOG_WARNING(log, "Cluster configuration was changed during update, found nonexisting node");
            return {};
        }
        result.emplace(node_uuid, NodeInfo(payload));
    }
    return result;
}

/// Checks if cluster nodes set is changed.
/// Returns true if update required.
/// It performs only shallow check (set of nodes' uuids).
/// So, if node's hostname are changed, then cluster won't be updated.
bool ClusterDiscovery::needUpdate(const Strings & node_uuids, const NodesInfo & nodes)
{
    bool has_difference = node_uuids.size() != nodes.size() ||
                          std::any_of(node_uuids.begin(), node_uuids.end(), [&nodes] (auto u) { return !nodes.contains(u); });

    return has_difference;
}

ClusterPtr ClusterDiscovery::getCluster(const ClusterInfo & cluster_info)
{
    Strings replica_adresses;
    replica_adresses.reserve(cluster_info.nodes_info.size());
    for (const auto & [_, node] : cluster_info.nodes_info)
        replica_adresses.emplace_back(node.address);

    std::vector<std::vector<String>> shards = {replica_adresses};

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
    return cluster;
}

/// Reads data from zookeeper and tries to update cluster.
/// Returns true on success (or no update required).
bool ClusterDiscovery::updateCluster(ClusterInfo & cluster_info)
{
    LOG_TRACE(log, "Updating cluster '{}'", cluster_info.name);

    auto zk = context->getZooKeeper();

    int start_version;
    Strings node_uuids = getNodeNames(zk, cluster_info.zk_root, cluster_info.name, &start_version, false);

    if (node_uuids.empty())
    {
        LOG_ERROR(log, "Can't find any node in cluster '{}', will register again", cluster_info.name);
        registerInZk(zk, cluster_info);
        return false;
    }

    auto & nodes_info = cluster_info.nodes_info;
    if (!needUpdate(node_uuids, nodes_info))
    {
        LOG_TRACE(log, "No update required for cluster '{}'", cluster_info.name);
        return true;
    }

    nodes_info = getNodes(zk, cluster_info.zk_root, node_uuids);
    if (nodes_info.empty())
        return false;

    int current_version;
    getNodeNames(zk, cluster_info.zk_root, cluster_info.name, &current_version, true);

    if (current_version != start_version)
    {
        nodes_info.clear();
        return false;
    }

    auto cluster = getCluster(cluster_info);
    context->setCluster(cluster_info.name, cluster);
    return true;
}

bool ClusterDiscovery::updateCluster(const String & cluster_name)
{
    auto cluster_info = clusters_info.find(cluster_name);
    if (cluster_info == clusters_info.end())
    {
        LOG_ERROR(log, "Unknown cluster '{}'", cluster_name);
        return false;
    }
    return updateCluster(cluster_info->second);
}

void ClusterDiscovery::registerInZk(zkutil::ZooKeeperPtr & zk, ClusterInfo & info)
{
    String node_path = getShardsListPath(info.zk_root) / node_name;
    zk->createAncestors(node_path);

    String payload = getFQDNOrHostName() + ":" + toString(server_port);

    zk->createOrUpdate(node_path, payload, zkutil::CreateMode::Ephemeral);
    LOG_DEBUG(log, "Current node {} registered in cluster {}", node_name, info.name);
}

void ClusterDiscovery::start()
{
    auto zk = context->getZooKeeper();

    LOG_TRACE(log, "Starting working thread");
    main_thread = ThreadFromGlobalPool([this] { runMainThread(); });

    for (auto & [_, info] : clusters_info)
    {
        registerInZk(zk, info);
        if (!updateCluster(info))
            LOG_WARNING(log, "Error on updating cluster '{}'", info.name);
    }
}

void ClusterDiscovery::runMainThread()
{
    LOG_TRACE(log, "Worker thread started");
    // setThreadName("ClusterDiscovery");

    using namespace std::chrono_literals;
    constexpr UInt64 full_update_interval = std::chrono::milliseconds(5min).count();

    std::unordered_map<String, Stopwatch> last_cluster_update;
    for (const auto & [cluster_name, _] : clusters_info)
        last_cluster_update.emplace(cluster_name, Stopwatch());
    Stopwatch last_full_update;

    pcg64 rng(randomSeed());

    while (!stop_flag)
    {
        {
            String cluster_name;
            if (queue->tryPop(cluster_name, QUEUE_OP_TIMEOUT_MS))
            {
                if (updateCluster(cluster_name))
                    last_cluster_update[cluster_name].restart();
                else
                    LOG_WARNING(log, "Error on updating cluster '{}', configuration changed during update, will retry", cluster_name);
            }
        }

        auto jitter = std::uniform_real_distribution<>(1.0, 2.0)(rng);
        if (last_full_update.elapsedMilliseconds() > UInt64(full_update_interval * jitter))
        {
            for (const auto & lastupd : last_cluster_update)
            {
                if (lastupd.second.elapsedMilliseconds() > full_update_interval)
                {
                    if (updateCluster(lastupd.first))
                        last_cluster_update[lastupd.first].restart();
                    else
                        LOG_WARNING(log, "Error on updating cluster '{}'", lastupd.first);
                }
            }
            last_full_update.restart();
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
