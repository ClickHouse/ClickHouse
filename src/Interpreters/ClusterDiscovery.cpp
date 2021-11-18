#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <unordered_map>
#include <unordered_set>

#include <base/getFQDNOrHostName.h>
#include <base/logger_useful.h>

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/setThreadName.h>

#include <Core/ServerUUID.h>

#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterDiscovery.h>
#include <Interpreters/Context.h>

#include <Poco/Exception.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

fs::path getShardsListPath(const String & zk_root)
{
    return fs::path(zk_root + "/shards");
}

}

/*
 * Holds boolean flags for fixed set of keys.
 * Flags can be concurrently set from different threads, and consumer can wait for it.
 */
template <typename T>
class ClusterDiscovery::ConcurrentFlags
{
public:
    template <typename It>
    ConcurrentFlags(It begin, It end)
    {
        for (auto it = begin; it != end; ++it)
            flags.emplace(*it, false);
    }

    void set(const T & key)
    {
        setFlag(key, true);
        cv.notify_one();
    }

    void unset(const T & key) { setFlag(key, false); }

    void wait(std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lk(mu);
        cv.wait_for(lk, timeout);
    }

    const std::unordered_map<T, std::atomic_bool> & get() { return flags; }

private:

    void setFlag(const T & key, bool value)
    {
        auto it = flags.find(key);
        if (it == flags.end())
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Unknown value '{}'", key);
        it->second = value;
    }

    std::condition_variable cv;
    std::mutex mu;

    std::unordered_map<T, std::atomic_bool> flags;
};

ClusterDiscovery::ClusterDiscovery(
    const Poco::Util::AbstractConfiguration & config,
    ContextPtr context_,
    const String & config_prefix)
    : context(Context::createCopy(context_))
    , node_name(toString(ServerUUID::get()))
    , server_port(context->getTCPPort())
    , log(&Poco::Logger::get("ClusterDiscovery"))
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_prefix, config_keys);

    for (const auto & key : config_keys)
    {
        String prefix = config_prefix + "." + key + ".discovery";
        if (!config.has(prefix))
            continue;
        String path = config.getString(prefix + ".path");
        trimRight(path, '/');
        clusters_info.emplace(key, ClusterInfo(key, path));
    }
    clusters_to_update = std::make_shared<UpdateFlags>(config_keys.begin(), config_keys.end());
}

/// List node in zookeper for cluster
Strings ClusterDiscovery::getNodeNames(zkutil::ZooKeeperPtr & zk,
                                       const String & zk_root,
                                       const String & cluster_name,
                                       int * version,
                                       bool set_callback)
{
    auto watch_callback = [cluster_name, clusters_to_update=clusters_to_update](auto) { clusters_to_update->set(cluster_name); };

    Coordination::Stat stat;
    Strings nodes = zk->getChildrenWatch(getShardsListPath(zk_root), &stat, set_callback ? watch_callback : Coordination::WatchCallback{});
    if (version)
        *version = stat.cversion;
    return nodes;
}

/// Reads node information from specified zookeeper nodes
/// On error returns empty result
ClusterDiscovery::NodesInfo ClusterDiscovery::getNodes(zkutil::ZooKeeperPtr & zk, const String & zk_root, const Strings & node_uuids)
{
    NodesInfo result;
    for (const auto & node_uuid : node_uuids)
    {
        String payload;
        if (!zk->tryGet(getShardsListPath(zk_root) / node_uuid, payload))
        {
            LOG_WARNING(log, "Error getting data from node '{}' in '{}'", node_uuid, zk_root);
            return {};
        }
        if (!NodeInfo::parse(payload, result[node_uuid]))
        {
            LOG_WARNING(log, "Error parsing data from node '{}' in '{}'", node_uuid, zk_root);
            return {};
        }
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
    UNUSED(log);
    #if defined(NDEBUG) || defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) || defined(MEMORY_SANITIZER) || defined(UNDEFINED_BEHAVIOR_SANITIZER)
    {
        /// Just to log updated nodes, suboptimal, but should be ok for expected update sizes.
        /// Disabled on release build.
        std::set<String> new_names(node_uuids.begin(), node_uuids.end());
        std::set<String> old_names;
        for (const auto & [name, _] : nodes)
            old_names.emplace(name);

        auto format_cluster_update = [](const std::set<String> & s1, const std::set<String> & s2)
        {
            std::vector<String> diff;
            std::set_difference(s1.begin(), s1.end(), s2.begin(), s2.end(), std::back_inserter(diff));

            constexpr size_t max_to_show = 3;
            size_t sz = diff.size();
            if (sz > max_to_show)
                diff.resize(max_to_show);
            return fmt::format("{} nodes ({}{})", sz, fmt::join(diff, ", "), diff.size() == sz ? "" : ",...");
        };

        LOG_DEBUG(log, "Cluster update: added: {}, removed: {}",
            format_cluster_update(new_names, old_names),
            format_cluster_update(old_names, new_names));

    }
    #endif
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

    NodeInfo self_node(getFQDNOrHostName() + ":" + toString(server_port));

    zk->createOrUpdate(node_path, self_node.serialize(), zkutil::CreateMode::Ephemeral);
    LOG_DEBUG(log, "Current node {} registered in cluster {}", node_name, info.name);
}

void ClusterDiscovery::start()
{
    if (clusters_info.empty())
    {
        LOG_DEBUG(log, "No defined clusters for discovery");
        return;
    }
    LOG_TRACE(log, "Starting working thread");
    main_thread = ThreadFromGlobalPool([this] { runMainThread(); });
}

void ClusterDiscovery::runMainThread()
{
    LOG_DEBUG(log, "Worker thread started");
    // setThreadName("ClusterDiscovery");

    {
        auto zk = context->getZooKeeper();
        for (auto & [_, info] : clusters_info)
        {
            registerInZk(zk, info);
            if (!updateCluster(info))
            {
                LOG_WARNING(log, "Error on updating cluster '{}', will retry", info.name);
                clusters_to_update->set(info.name);
            }
        }
    }

    using namespace std::chrono_literals;

    while (!stop_flag)
    {
        /// if some cluster update was ended with error on previous iteration, we will retry after timeout
        clusters_to_update->wait(5s);
        for (const auto & [cluster_name, need_update] : clusters_to_update->get())
        {
            if (!need_update)
                continue;
            bool ok = updateCluster(cluster_name);
            if (ok)
                clusters_to_update->unset(cluster_name);
        }
    }
    LOG_DEBUG(log, "Worker thread stopped");
}

void ClusterDiscovery::shutdown()
{
    LOG_TRACE(log, "Shutting down");

    stop_flag.exchange(true);
    if (main_thread.joinable())
        main_thread.join();
}

ClusterDiscovery::~ClusterDiscovery()
{
    ClusterDiscovery::shutdown();
}

bool ClusterDiscovery::NodeInfo::parse(const String & data, NodeInfo & result)
{
    try
    {
        Poco::JSON::Parser parser;
        auto json = parser.parse(data).extract<Poco::JSON::Object::Ptr>();

        result.address = json->getValue<std::string>("address");
    }
    catch (Poco::Exception & e)
    {
        LOG_WARNING(
            &Poco::Logger::get("ClusterDiscovery"),
            "Can't parse '{}' from node: {}",
            data.size() < 1024 ? data : "[data too long]", e.displayText());
        return false;
    }
    return true;
}

String ClusterDiscovery::NodeInfo::serialize() const
{
    Poco::JSON::Object json;
    json.set("address", address);

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

}
