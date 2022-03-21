#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
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
        auto it = flags.find(key);
        if (it == flags.end())
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Unknown value '{}'", key);
        it->second = true;
        any_need_update = true;
        cv.notify_one();
    }

    /// waits unit at least one flag is set
    /// caller should handle all set flags (or set it again manually)
    /// note: keys of returen map should not be changed!
    /// @param finished - output parameter indicates that stop() was called
    std::unordered_map<T, std::atomic_bool> & wait(std::chrono::milliseconds timeout, bool & finished)
    {
        std::unique_lock<std::mutex> lk(mu);
        cv.wait_for(lk, timeout, [this]() -> bool { return any_need_update || stop_flag; });
        finished = stop_flag;

        /// all set flags expected to be handled by caller
        any_need_update = false;
        return flags;
    }

    void stop()
    {
        std::unique_lock<std::mutex> lk(mu);
        stop_flag = true;
        cv.notify_one();
    }

private:
    std::condition_variable cv;
    std::mutex mu;

    /// flag indicates that update is required
    std::unordered_map<T, std::atomic_bool> flags;
    std::atomic_bool any_need_update = true;
    bool stop_flag = false;
};

ClusterDiscovery::ClusterDiscovery(
    const Poco::Util::AbstractConfiguration & config,
    ContextPtr context_,
    const String & config_prefix)
    : context(Context::createCopy(context_))
    , current_node_name(toString(ServerUUID::get()))
    , log(&Poco::Logger::get("ClusterDiscovery"))
{
    LOG_DEBUG(log, "Cluster discovery is enabled");

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_prefix, config_keys);

    for (const auto & key : config_keys)
    {
        String prefix = config_prefix + "." + key + ".discovery";
        if (!config.has(prefix))
            continue;

        clusters_info.emplace(
            key,
            ClusterInfo(
                /* name_= */ key,
                /* zk_root_= */ config.getString(prefix + ".path"),
                /* port= */ context->getTCPPort(),
                /* secure= */ config.getBool(prefix + ".secure", false),
                /* shard_id= */ config.getUInt(prefix + ".shard", 0)
            )
        );
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
        bool ok = zk->tryGet(getShardsListPath(zk_root) / node_uuid, payload) &&
                  NodeInfo::parse(payload, result[node_uuid]);
        if (!ok)
        {
            LOG_WARNING(log, "Can't get data from node '{}' in '{}'", node_uuid, zk_root);
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
    {
        /// Just to log updated nodes, suboptimal, but should be ok for expected update sizes
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
            bool need_crop = sz > max_to_show;
            if (need_crop)
                diff.resize(max_to_show);

            if (sz == 0)
                return fmt::format("{} nodes", sz);
            return fmt::format("{} node{} [{}{}]", sz, sz != 1 ? "s" : "", fmt::join(diff, ", "), need_crop ? ",..." : "");
        };

        LOG_DEBUG(log, "Cluster update: added {}, removed {}",
            format_cluster_update(new_names, old_names),
            format_cluster_update(old_names, new_names));
    }
    return has_difference;
}

ClusterPtr ClusterDiscovery::makeCluster(const ClusterInfo & cluster_info)
{
    std::vector<std::vector<String>> shards;
    {
        std::map<size_t, Strings> replica_adresses;

        for (const auto & [_, node] : cluster_info.nodes_info)
        {
            if (cluster_info.current_node.secure != node.secure)
            {
                LOG_WARNING(log, "Node '{}' in cluster '{}' has different 'secure' value, skipping it", node.address, cluster_info.name);
                continue;
            }
            replica_adresses[node.shard_id].emplace_back(node.address);
        }

        shards.reserve(replica_adresses.size());
        for (auto & [_, replicas] : replica_adresses)
            shards.emplace_back(std::move(replicas));
    }

    bool secure = cluster_info.current_node.secure;
    auto cluster = std::make_shared<Cluster>(
        context->getSettingsRef(),
        shards,
        /* username= */ context->getUserName(),
        /* password= */ "",
        /* clickhouse_port= */ secure ? context->getTCPPortSecure().value_or(DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort(),
        /* treat_local_as_remote= */ false,
        /* treat_local_port_as_remote= */ context->getApplicationType() == Context::ApplicationType::LOCAL,
        /* secure= */ secure);
    return cluster;
}

/// Reads data from zookeeper and tries to update cluster.
/// Returns true on success (or no update required).
bool ClusterDiscovery::updateCluster(ClusterInfo & cluster_info)
{
    LOG_DEBUG(log, "Updating cluster '{}'", cluster_info.name);

    auto zk = context->getZooKeeper();

    int start_version;
    Strings node_uuids = getNodeNames(zk, cluster_info.zk_root, cluster_info.name, &start_version, false);
    auto & nodes_info = cluster_info.nodes_info;

    if (std::find(node_uuids.begin(), node_uuids.end(), current_node_name) == node_uuids.end())
    {
        LOG_ERROR(log, "Can't find current node in cluster '{}', will register again", cluster_info.name);
        registerInZk(zk, cluster_info);
        nodes_info.clear();
        return false;
    }

    if (!needUpdate(node_uuids, nodes_info))
    {
        LOG_DEBUG(log, "No update required for cluster '{}'", cluster_info.name);
        return true;
    }

    nodes_info = getNodes(zk, cluster_info.zk_root, node_uuids);
    if (nodes_info.empty())
    {
        LOG_WARNING(log, "Can't get nodes info for '{}'", cluster_info.name);
        return false;
    }

    int current_version;
    getNodeNames(zk, cluster_info.zk_root, cluster_info.name, &current_version, true);

    if (current_version != start_version)
    {
        LOG_DEBUG(log, "Cluster '{}' configuration changed during update", cluster_info.name);
        nodes_info.clear();
        return false;
    }

    LOG_DEBUG(log, "Updating system.clusters record for '{}' with {} nodes", cluster_info.name, cluster_info.nodes_info.size());

    auto cluster = makeCluster(cluster_info);
    context->setCluster(cluster_info.name, cluster);
    return true;
}

void ClusterDiscovery::registerInZk(zkutil::ZooKeeperPtr & zk, ClusterInfo & info)
{
    LOG_DEBUG(log, "Registering current node {} in cluster {}", current_node_name, info.name);

    String node_path = getShardsListPath(info.zk_root) / current_node_name;
    zk->createAncestors(node_path);

    zk->createOrUpdate(node_path, info.current_node.serialize(), zkutil::CreateMode::Ephemeral);
    LOG_DEBUG(log, "Current node {} registered in cluster {}", current_node_name, info.name);
}

void ClusterDiscovery::initialUpdate()
{
    auto zk = context->getZooKeeper();
    for (auto & [_, info] : clusters_info)
    {
        registerInZk(zk, info);
        if (!updateCluster(info))
        {
            LOG_WARNING(log, "Error on initial cluster '{}' update, will retry in background", info.name);
            clusters_to_update->set(info.name);
        }
    }
}

void ClusterDiscovery::start()
{
    if (clusters_info.empty())
    {
        LOG_DEBUG(log, "No defined clusters for discovery");
        return;
    }

    try
    {
        initialUpdate();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Caught exception in cluster discovery initialization");
    }

    using namespace std::chrono_literals;
    constexpr static std::chrono::milliseconds DEFAULT_BACKOFF_TIMEOUT = 10ms;

    LOG_DEBUG(log, "Starting working thread");
    main_thread = ThreadFromGlobalPool([this]
    {
        std::chrono::milliseconds backoff_timeout = DEFAULT_BACKOFF_TIMEOUT;

        bool finish = false;
        while (!finish)
        {
            try
            {
                finish = runMainThread([&backoff_timeout] { backoff_timeout = DEFAULT_BACKOFF_TIMEOUT; });
            }
            catch (...)
            {
                /*
                 * it can be zk error (will take new session) or other retriable error,
                 * should not stop discovery forever
                 */
                tryLogCurrentException(log, "Caught exception in cluster discovery runMainThread");
            }
            std::this_thread::sleep_for(backoff_timeout);
            backoff_timeout = std::min(backoff_timeout * 2, std::chrono::milliseconds(3min));
        }
    });
}

/// Returns `true` on graceful shutdown (no restart required)
bool ClusterDiscovery::runMainThread(std::function<void()> up_to_date_callback)
{
    setThreadName("ClusterDiscover");
    LOG_DEBUG(log, "Worker thread started");

    using namespace std::chrono_literals;

    constexpr auto force_update_interval = 2min;
    bool finished = false;
    while (!finished)
    {
        bool all_up_to_date = true;
        auto & clusters = clusters_to_update->wait(5s, finished);
        for (auto & [cluster_name, need_update] : clusters)
        {
            auto cluster_info_it = clusters_info.find(cluster_name);
            if (cluster_info_it == clusters_info.end())
            {
                LOG_ERROR(log, "Unknown cluster '{}'", cluster_name);
                continue;
            }
            auto & cluster_info = cluster_info_it->second;

            if (!need_update.exchange(false))
            {
                /// force updating periodically
                bool force_update = cluster_info.watch.elapsedSeconds() > std::chrono::seconds(force_update_interval).count();
                if (!force_update)
                    continue;
            }

            if (updateCluster(cluster_info))
            {
                cluster_info.watch.restart();
                LOG_DEBUG(log, "Cluster '{}' updated successfully", cluster_name);
            }
            else
            {
                all_up_to_date = false;
                /// no need to trigger convar, will retry after timeout in `wait`
                need_update = true;
                LOG_WARNING(log, "Cluster '{}' wasn't updated, will retry", cluster_name);
            }
        }

        if (all_up_to_date)
        {
            up_to_date_callback();
        }
    }
    LOG_DEBUG(log, "Worker thread stopped");
    return finished;
}

void ClusterDiscovery::shutdown()
{
    LOG_DEBUG(log, "Shutting down");
    clusters_to_update->stop();

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

        size_t ver = json->optValue<size_t>("version", data_ver);
        if (ver == data_ver)
        {
            result.address = json->getValue<std::string>("address");
            result.secure = json->optValue<bool>("secure", false);
            result.shard_id = json->optValue<size_t>("shard_id", 0);
        }
        else
        {
            LOG_ERROR(
                &Poco::Logger::get("ClusterDiscovery"),
                "Unsupported version '{}' of data in zk node '{}'",
                ver, data.size() < 1024 ? data : "[data too long]");
        }
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
    json.set("version", data_ver);
    json.set("address", address);
    json.set("shard_id", shard_id);

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

}
