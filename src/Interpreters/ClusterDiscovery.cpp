#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include <base/getFQDNOrHostName.h>

#include <Common/Config/ConfigHelper.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/StringUtils.h>
#include <Common/thread_local_rng.h>
#include <Common/ZooKeeper/Types.h>

#include <Core/ServerUUID.h>

#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterDiscovery.h>
#include <Interpreters/Context.h>

#include <IO/WriteHelpers.h>

#include <Poco/Exception.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <fmt/format.h>
#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int KEEPER_EXCEPTION;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}

namespace FailPoints
{
    extern const char cluster_discovery_faults[];
}

namespace
{

fs::path getShardsListPath(const String & zk_root)
{
    return fs::path(zk_root + "/shards");
}

}

ClusterDiscovery::ClusterInfo::ClusterInfo(const String & name_,
    const String & zk_name_,
    const String & zk_root_,
    const String & host_name,
    const String & username_,
    const String & password_,
    const String & cluster_secret_,
    UInt16 port,
    bool secure,
    size_t shard_id,
    bool observer_mode,
    bool invisible,
    size_t zk_root_index_
    )
    : name(name_)
    , zk_name(zk_name_)
    , zk_root(zk_root_)
    , current_node(host_name + ":" + toString(port), secure, shard_id)
    , current_node_is_observer(observer_mode)
    , current_cluster_is_invisible(invisible)
    , is_secure_connection(secure)
    , username(username_)
    , password(password_)
    , cluster_secret(cluster_secret_)
    , zk_root_index(zk_root_index_)
{
}

/*
 * Holds boolean flags for set of keys.
 * Keys can be added and removed.
 * Flags can be set from different threads, and consumer can wait for it.
 */
template <typename T>
class ClusterDiscovery::Flags
{
public:
    template <typename It>
    Flags(It begin, It end)
    {
        for (auto it = begin; it != end; ++it)
            flags.emplace(*it, false);
    }

    void set(const T & key, bool value = true)
    {
        std::unique_lock<std::mutex> lk(mu);
        if (stop_flag)
            return;
        flags[key] = value;
        any_need_update |= value;
        cv.notify_one();
    }

    /// Just notify the condition variable.
    void set()
    {
        std::unique_lock<std::mutex> lk(mu);
        if (stop_flag)
            return;
        any_need_update = true;
        cv.notify_one();
    }

    void remove(const T & key)
    {
        std::unique_lock<std::mutex> lk(mu);
        if (!stop_flag)
            flags.erase(key);
    }

    std::unordered_map<T, bool> wait(bool & finished)
    {
        std::unique_lock<std::mutex> lk(mu);
        cv.wait(lk, [this]() -> bool { return any_need_update || stop_flag; });
        finished = stop_flag;

        any_need_update = false;
        auto res = flags;
        for (auto & f : flags)
            f.second = false;
        return res;
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
    std::unordered_map<T, bool> flags;
    bool any_need_update = true;
    bool stop_flag = false;
};

ClusterDiscovery::ClusterDiscovery(
    const Poco::Util::AbstractConfiguration & config,
    ContextPtr context_,
    MultiVersion<Macros>::Version macros_,
    const String & config_prefix)
    : context(Context::createCopy(context_))
    , current_node_name(toString(ServerUUID::get()))
    , log(getLogger("ClusterDiscovery"))
    , macros(macros_)
{
    LOG_DEBUG(log, "Cluster discovery is enabled");

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_prefix, config_keys);

    for (const auto & key : config_keys)
    {
        String cluster_config_prefix = config_prefix + "." + key + ".discovery";
        if (!config.has(cluster_config_prefix))
            continue;

        String zk_name_and_root = config.getString(cluster_config_prefix + ".path", "");
        String zk_multicluster_name_and_root = config.getString(cluster_config_prefix + ".multicluster_root_path", "");
        bool is_observer = ConfigHelper::getBool(config, cluster_config_prefix + ".observer");

        const auto & password = config.getString(cluster_config_prefix + ".password", "");
        const auto & cluster_secret = config.getString(cluster_config_prefix + ".secret", "");
        if (!password.empty() && !cluster_secret.empty())
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Both 'password' and 'secret' are specified for cluster '{}', only one option can be used at the same time", key);

        if (!zk_multicluster_name_and_root.empty())
        {
            if (!zk_name_and_root.empty())
                throw Exception(
                    ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
                    "Autodiscovery cluster node {} has 'path' and 'multicluster_root_path' subnodes simultaneously",
                    key);
            if (!is_observer)
                throw Exception(
                    ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                    "Autodiscovery cluster node {} must be in observer mode",
                    key);

            String zk_root = zkutil::extractZooKeeperPath(zk_multicluster_name_and_root, true);
            String zk_name = zkutil::extractZooKeeperName(zk_multicluster_name_and_root);

            MulticlusterDiscovery mcd(
                /* zk_name */ zk_name,
                /* zk_path */ zk_root,
                /* is_secure_connection */ config.getBool(cluster_config_prefix + ".secure", false),
                /* username */ config.getString(cluster_config_prefix + ".user", context->getUserName()),
                /* password */ password,
                /* cluster_secret */ cluster_secret
            );

            multicluster_discovery_paths.push_back(std::move(mcd));
            continue;
        }

        if (zk_name_and_root.empty())
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "ZooKeeper path for cluster '{}' is empty", key);

        String zk_root = zkutil::extractZooKeeperPath(zk_name_and_root, true);
        String zk_name = zkutil::extractZooKeeperName(zk_name_and_root);

        clusters_info.emplace(
            key,
            ClusterInfo(
                /* name_= */ key,
                /* zk_name_= */ zk_name,
                /* zk_root_= */ zk_root,
                /* host_name= */ config.getString(cluster_config_prefix + ".my_hostname", getFQDNOrHostName()),
                /* username= */ config.getString(cluster_config_prefix + ".user", context->getUserName()),
                /* password= */ password,
                /* cluster_secret= */ cluster_secret,
                /* port= */ context->getTCPPort(),
                /* secure= */ config.getBool(cluster_config_prefix + ".secure", false),
                /* shard_id= */ config.getUInt(cluster_config_prefix + ".shard", 0),
                /* observer_mode= */ is_observer,
                /* invisible= */ ConfigHelper::getBool(config, cluster_config_prefix + ".invisible")
            )
        );
    }

    std::vector<String> clusters_info_names;
    clusters_info_names.reserve(clusters_info.size());
    for (const auto & e : clusters_info)
        clusters_info_names.emplace_back(e.first);

    LOG_TRACE(log, "Clusters in discovery mode: {}", fmt::join(clusters_info_names, ", "));
    clusters_to_update = std::make_shared<UpdateFlags>(clusters_info_names.begin(), clusters_info_names.end());

    /// Init get_nodes_callbacks after init clusters_to_update.
    for (const auto & e : clusters_info)
        get_nodes_callbacks[e.first] = std::make_shared<Coordination::WatchCallback>(
            [cluster_name = e.first, my_clusters_to_update = clusters_to_update](auto)
            {
                my_clusters_to_update->set(cluster_name);
            });

    for (auto & path : multicluster_discovery_paths)
    {
        path.watch_callback = std::make_shared<Coordination::WatchCallback>(
            [my_need_update = path.need_update, my_flag = clusters_to_update](auto)
            {
                my_need_update->store(true);
                my_flag->set();
            }
        );
    }
}

/// List node in zookeper for cluster
Strings ClusterDiscovery::getNodeNames(zkutil::ZooKeeperPtr & zk,
                                       const String & zk_root,
                                       const String & cluster_name,
                                       int * version,
                                       bool set_callback,
                                       size_t zk_root_index)
{
    Coordination::Stat stat;
    Strings nodes;

    if (set_callback)
    {
        auto callback = get_nodes_callbacks.find(cluster_name);
        if (callback == get_nodes_callbacks.end())
        {
            auto watch_dynamic_callback = std::make_shared<Coordination::WatchCallback>([
                cluster_name,
                my_clusters_to_update = clusters_to_update,
                my_discovery_paths_need_update = multicluster_discovery_paths[zk_root_index - 1].need_update
                ](auto)
                {
                    my_discovery_paths_need_update->store(true);
                    my_clusters_to_update->set(cluster_name);
                });
            auto res = get_nodes_callbacks.insert(std::make_pair(cluster_name, watch_dynamic_callback));
            callback = res.first;
        }
        nodes = zk->getChildrenWatch(getShardsListPath(zk_root), &stat, callback->second);
    }
    else
        nodes = zk->getChildren(getShardsListPath(zk_root), &stat);

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
    std::vector<Strings> shards;
    {
        std::map<size_t, Strings> replica_addresses;

        for (const auto & [_, node] : cluster_info.nodes_info)
        {
            if (cluster_info.current_node.secure != node.secure)
            {
                LOG_WARNING(log, "Node '{}' in cluster '{}' has different 'secure' value, skipping it", node.address, cluster_info.name);
                continue;
            }
            replica_addresses[node.shard_id].emplace_back(node.address);
        }

        shards.reserve(replica_addresses.size());
        for (auto & [_, replicas] : replica_addresses)
            shards.emplace_back(std::move(replicas));
    }

    bool secure = cluster_info.current_node.secure;
    ClusterConnectionParameters params{
        /* username= */ cluster_info.username,
        /* password= */ cluster_info.password,
        /* clickhouse_port= */ secure ? context->getTCPPortSecure().value_or(DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort(),
        /* treat_local_as_remote= */ false,
        /* treat_local_port_as_remote= */ false, /// should be set only for clickhouse-local, but cluster discovery is not used there
        /* secure= */ secure,
        /* bind_host= */ "",
        /* priority= */ Priority{1},
        /* cluster_name= */ cluster_info.name,
        /* cluster_secret= */ cluster_info.cluster_secret};
    auto cluster = std::make_shared<Cluster>(
        context->getSettingsRef(),
        shards,
        params);
    return cluster;
}

static bool contains(const Strings & list, const String & value)
{
    return std::find(list.begin(), list.end(), value) != list.end();
}

/// Reads data from zookeeper and tries to update cluster.
/// Returns true on success (or no update required).
/// Is the record about cluster did not existed before, creates it.
bool ClusterDiscovery::upsertCluster(ClusterInfo & cluster_info)
{
    LOG_DEBUG(log, "Updating cluster '{}'", cluster_info.name);

    auto zk = context->getDefaultOrAuxiliaryZooKeeper(cluster_info.zk_name);

    int start_version;
    Strings node_uuids = getNodeNames(zk, cluster_info.zk_root, cluster_info.name, &start_version, false, cluster_info.zk_root_index);
    auto & nodes_info = cluster_info.nodes_info;
    auto on_exit = [this, start_version, &zk, &cluster_info, &nodes_info]()
    {
        /// in case of successful update we still need to check if configuration of cluster still valid and also set watch callback
        int current_version;
        getNodeNames(zk, cluster_info.zk_root, cluster_info.name, &current_version, true, cluster_info.zk_root_index);

        if (current_version != start_version)
        {
            LOG_DEBUG(log, "Cluster '{}' configuration changed during update", cluster_info.name);
            nodes_info.clear();
            return false;
        }
        return true;
    };

    if (!cluster_info.current_node_is_observer && !contains(node_uuids, current_node_name))
    {
        LOG_ERROR(log, "Can't find current node in cluster '{}', will register again", cluster_info.name);
        registerInZk(zk, cluster_info);
        nodes_info.clear();
        return false;
    }

    if (cluster_info.current_cluster_is_invisible)
    {
        LOG_DEBUG(log, "Cluster '{}' is invisible.", cluster_info.name);
        return true;
    }

    if (!needUpdate(node_uuids, nodes_info))
    {
        LOG_DEBUG(log, "No update required for cluster '{}'", cluster_info.name);
        return on_exit();
    }

    nodes_info = getNodes(zk, cluster_info.zk_root, node_uuids);

    if (bool ok = on_exit(); !ok)
        return false;

    LOG_DEBUG(log, "Updating system.clusters record for '{}' with {} nodes", cluster_info.name, cluster_info.nodes_info.size());

    if (nodes_info.empty())
    {
        String name = cluster_info.name;
        /// cluster_info removed inside removeCluster, can't use reference to name.
        removeCluster(name);
        return true;
    }

    auto cluster = makeCluster(cluster_info);
    std::lock_guard lock(mutex);
    cluster_impls[cluster_info.name] = cluster;

    return true;
}

void ClusterDiscovery::removeCluster(const String & name)
{
    {
        std::lock_guard lock(mutex);
        cluster_impls.erase(name);
    }
    clusters_to_update->remove(name);
    get_nodes_callbacks.erase(name);
    LOG_DEBUG(log, "Dynamic cluster '{}' removed successfully", name);
}

void ClusterDiscovery::registerInZk(zkutil::ZooKeeperPtr & zk, ClusterInfo & info)
{
    /// Create root node in observer mode not to get 'No node' error
    String node_path = getShardsListPath(info.zk_root) / current_node_name;
    zk->createAncestors(node_path);

    if (info.current_node_is_observer)
    {
        LOG_DEBUG(log, "Current node {} is observer of cluster {}", current_node_name, info.name);
        return;
    }

    LOG_DEBUG(log, "Registering current node {} in cluster {}", current_node_name, info.name);

    zk->createOrUpdate(node_path, info.current_node.serialize(), zkutil::CreateMode::Ephemeral);
    LOG_DEBUG(log, "Current node {} registered in cluster {}", current_node_name, info.name);
}

void ClusterDiscovery::initialUpdate()
{
    LOG_DEBUG(log, "Initializing");

    fiu_do_on(FailPoints::cluster_discovery_faults,
    {
        constexpr UInt8 success_chance = 4;
        static size_t fail_count = 0;
        fail_count++;
        /// strict limit on fail count to avoid flaky tests
        auto is_failed = fail_count < success_chance && std::uniform_int_distribution<>(0, success_chance)(thread_local_rng) != 0;
        if (is_failed)
            throw Exception(ErrorCodes::KEEPER_EXCEPTION, "Failpoint cluster_discovery_faults is triggered");
    });

    for (const auto & path : multicluster_discovery_paths)
    {
        auto zk = context->getDefaultOrAuxiliaryZooKeeper(path.zk_name);

        zk->createAncestors(path.zk_path);
        zk->createIfNotExists(path.zk_path, "");
    }

    findDynamicClusters(clusters_info);

    for (auto & [_, info] : clusters_info)
    {
        auto zk = context->getDefaultOrAuxiliaryZooKeeper(info.zk_name);
        registerInZk(zk, info);
        if (!upsertCluster(info))
        {
            LOG_WARNING(log, "Error on initial cluster '{}' update, will retry in background", info.name);
            clusters_to_update->set(info.name);
        }
        else if (info.zk_root_index)
            clusters_to_update->set(info.name, false);
    }

    LOG_DEBUG(log, "Initialized");
    is_initialized = true;
}

void ClusterDiscovery::findDynamicClusters(
    std::unordered_map<String, ClusterDiscovery::ClusterInfo> & info,
    std::unordered_set<size_t> * unchanged_roots)
{
    using namespace std::chrono_literals;

    constexpr auto force_update_interval = 2min;

    size_t zk_root_index = 0;

    for (const auto & path : multicluster_discovery_paths)
    {
        ++zk_root_index;

        if (unchanged_roots)
        {
            if (!path.need_update->exchange(false))
            {
                /// force updating periodically
                bool force_update = path.watch.elapsedSeconds() > std::chrono::seconds(force_update_interval).count();
                if (!force_update)
                {
                    unchanged_roots->insert(zk_root_index);
                    continue;
                }
            }
        }

        auto zk = context->getDefaultOrAuxiliaryZooKeeper(path.zk_name);

        auto clusters = zk->getChildrenWatch(path.zk_path, nullptr, path.watch_callback);

        for (const auto & cluster : clusters)
        {
            auto p = clusters_info.find(cluster);
            if (p != clusters_info.end() && !p->second.zk_root_index)
            {
                /// Not a warning - node can register itsefs in one cluster and discover other clusters
                LOG_TRACE(log, "Found dynamic duplicate of cluster '{}' in config and Keeper, skipped", cluster);
                continue;
            }

            if (info.contains(cluster))
            {
                /// Possible with several root paths, it's a configuration error
                LOG_WARNING(log, "Found dynamic duplicate of cluster '{}' in Keeper, skipped record by path {}:{}",
                    cluster, path.zk_name, path.zk_path);
                continue;
            }

            info.emplace(
                cluster,
                ClusterInfo(
                    /* name_= */ cluster,
                    /* zk_name_= */ path.zk_name,
                    /* zk_root_= */ path.zk_path + "/" + cluster,
                    /* host_name= */ "",
                    /* username= */ path.username,
                    /* password= */ path.password,
                    /* cluster_secret= */ path.cluster_secret,
                    /* port= */ context->getTCPPort(),
                    /* secure= */ path.is_secure_connection,
                    /* shard_id= */ 0,
                    /* observer_mode= */ true,
                    /* invisible= */ false,
                    /* zk_root_index= */ zk_root_index
                )
            );
        }

        path.watch.restart();
    }
}

void ClusterDiscovery::start()
{
    if (clusters_info.empty() && multicluster_discovery_paths.empty())
    {
        LOG_DEBUG(log, "No defined clusters for discovery");
        return;
    }

    try
    {
        auto component_guard = Coordination::setCurrentComponent("ClusterDiscovery::start");
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
    DB::setThreadName(ThreadName::CLUSTER_DISCOVERY);
    LOG_DEBUG(log, "Worker thread started");

    auto component_guard = Coordination::setCurrentComponent("ClusterDiscovery::runMainThread");

    using namespace std::chrono_literals;

    constexpr auto force_update_interval = 2min;

    if (!is_initialized)
        initialUpdate();

    bool finished = false;
    while (!finished)
    {
        bool all_up_to_date = true;
        auto clusters = clusters_to_update->wait(finished);
        if (finished)
            break;

        std::unordered_map<String, ClusterInfo> new_dynamic_clusters_info;
        std::unordered_set<size_t> unchanged_roots;
        findDynamicClusters(new_dynamic_clusters_info, &unchanged_roots);

        std::unordered_set<String> clusters_to_insert;
        std::unordered_set<String> clusters_to_remove;

        /// Remove clusters that are not found in new_dynamic_clusters_info
        for (const auto & [cluster_name, info] : clusters_info)
        {
            if (!info.zk_root_index)
                continue;
            if (!new_dynamic_clusters_info.erase(cluster_name)
                && !unchanged_roots.contains(info.zk_root_index))
                clusters_to_remove.insert(cluster_name);
        }
        /// new_dynamic_clusters_info now contains only new clusters
        for (const auto & [cluster_name, _] : new_dynamic_clusters_info)
            clusters_to_insert.insert(cluster_name);

        for (const auto & cluster_name : clusters_to_remove)
            removeCluster(cluster_name);

        clusters_info.merge(new_dynamic_clusters_info);

        for (const auto & [cluster_name, need_update] : clusters)
        {
            auto cluster_info_it = clusters_info.find(cluster_name);
            if (cluster_info_it == clusters_info.end())
            {
                LOG_ERROR(log, "Unknown cluster '{}'", cluster_name);
                continue;
            }

            auto & cluster_info = cluster_info_it->second;
            if (!need_update)
            {
                /// force updating periodically
                bool force_update = cluster_info.watch.elapsedSeconds() > std::chrono::seconds(force_update_interval).count();
                if (!force_update)
                    continue;
            }

            if (upsertCluster(cluster_info))
            {
                cluster_info.watch.restart();
                LOG_DEBUG(log, "Cluster '{}' updated successfully", cluster_name);
            }
            else
            {
                all_up_to_date = false;
                /// no need to trigger convar, will retry after timeout in `wait`
                clusters_to_update->set(cluster_name);
                LOG_WARNING(log, "Cluster '{}' wasn't updated, will retry", cluster_name);
            }
        }

        for (const auto & cluster_name : clusters_to_insert)
        {
            auto cluster_info_it = clusters_info.find(cluster_name);
            if (cluster_info_it == clusters_info.end())
            {
                LOG_ERROR(log, "Unknown dynamic cluster '{}'", cluster_name);
                continue;
            }
            auto & cluster_info = cluster_info_it->second;
            if (upsertCluster(cluster_info))
            {
                cluster_info.watch.restart();
                LOG_DEBUG(log, "Dynamic cluster '{}' inserted successfully", cluster_name);
            }
            else
            {
                all_up_to_date = false;
                /// no need to trigger convar, will retry after timeout in `wait`
                clusters_to_update->set(cluster_name);
                LOG_WARNING(log, "Dynamic cluster '{}' wasn't inserted, will retry", cluster_name);
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

ClusterPtr ClusterDiscovery::getCluster(const String & cluster_name) const
{
    std::lock_guard lock(mutex);
    auto expanded_cluster_name = macros->expand(cluster_name);
    auto it = cluster_impls.find(expanded_cluster_name);
    if (it == cluster_impls.end())
        return nullptr;
    return it->second;
}

std::unordered_map<String, ClusterPtr> ClusterDiscovery::getClusters() const
{
    std::lock_guard lock(mutex);
    return cluster_impls;
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
    try
    {
        ClusterDiscovery::shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error on ClusterDiscovery shutdown");
    }
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
                getLogger("ClusterDiscovery"),
                "Unsupported version '{}' of data in zk node '{}'",
                ver, data.size() < 1024 ? data : "[data too long]");
        }
    }
    catch (Poco::Exception & e)
    {
        LOG_WARNING(
            getLogger("ClusterDiscovery"),
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
