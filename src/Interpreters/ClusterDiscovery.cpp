#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
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


namespace ProfileEvents
{
    extern const Event ZooKeeperWatchTriggeredClusterDiscovery;
}

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
    extern const char cluster_discovery_unregister_fail[];
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
    const String & multicluster_full_path_
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
    , multicluster_full_path(multicluster_full_path_)
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
    Flags() = default;

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

    bool isStopped() const
    {
        std::unique_lock<std::mutex> lk(mu);
        return stop_flag;
    }

private:
    mutable std::condition_variable cv;
    mutable std::mutex mu;

    /// flag indicates that update is required
    std::unordered_map<T, bool> flags;
    bool any_need_update = true;
    bool stop_flag = false;
};

ClusterDiscovery::ParsedDiscoveryConfig ClusterDiscovery::parseDiscoveryConfig(
    const Poco::Util::AbstractConfiguration & config,
    ContextPtr context,
    const String & config_prefix)
{
    ParsedDiscoveryConfig result;

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

            result.multicluster_roots.push_back(ParsedMulticlusterDiscovery{
                .zk_name = zk_name,
                .zk_path = zk_root,
                .is_secure_connection = config.getBool(cluster_config_prefix + ".secure", false),
                .username = config.getString(cluster_config_prefix + ".user", context->getUserName()),
                .password = password,
                .cluster_secret = cluster_secret,
            });
            continue;
        }

        if (zk_name_and_root.empty())
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "ZooKeeper path for cluster '{}' is empty", key);

        String zk_root = zkutil::extractZooKeeperPath(zk_name_and_root, true);
        String zk_name = zkutil::extractZooKeeperName(zk_name_and_root);

        result.static_clusters.push_back(ParsedStaticDiscovery{
            .name = key,
            .zk_name = zk_name,
            .zk_root = zk_root,
            .host_name = config.getString(cluster_config_prefix + ".my_hostname", getFQDNOrHostName()),
            .username = config.getString(cluster_config_prefix + ".user", context->getUserName()),
            .password = password,
            .cluster_secret = cluster_secret,
            .secure = config.getBool(cluster_config_prefix + ".secure", false),
            .shard_id = config.getUInt(cluster_config_prefix + ".shard", 0),
            .observer = is_observer,
            .invisible = ConfigHelper::getBool(config, cluster_config_prefix + ".invisible"),
        });
    }

    return result;
}

void ClusterDiscovery::validateConfig(
    const Poco::Util::AbstractConfiguration & config,
    ContextPtr context,
    const String & config_prefix)
{
    /// Discard result; throws on invalid discovery subtrees.
    parseDiscoveryConfig(config, context, config_prefix);
}

ClusterDiscovery::ClusterDiscovery(
    const Poco::Util::AbstractConfiguration & config,
    ContextPtr context_,
    MultiVersion<Macros>::Version macros_,
    const String & config_prefix)
    : context(Context::createCopy(context_))
    , current_node_name(toString(ServerUUID::get()))
    , clusters_to_update(std::make_shared<UpdateFlags>())
    , log(getLogger("ClusterDiscovery"))
    , macros(macros_)
{
    LOG_DEBUG(log, "Cluster discovery is enabled");

    auto parsed = parseDiscoveryConfig(config, context, config_prefix);

    for (auto & static_cluster : parsed.static_clusters)
        addStaticCluster(std::move(static_cluster));

    for (auto & root : parsed.multicluster_roots)
        addMulticlusterRoot(std::move(root));

    std::vector<String> clusters_info_names;
    clusters_info_names.reserve(clusters_info.size());
    for (const auto & e : clusters_info)
        clusters_info_names.emplace_back(e.first);

    LOG_TRACE(log, "Clusters in discovery mode: {}", fmt::join(clusters_info_names, ", "));
}

void ClusterDiscovery::addStaticCluster(ParsedStaticDiscovery && parsed)
{
    const String name = parsed.name;

    if (auto existing = clusters_info.find(name); existing != clusters_info.end())
    {
        if (existing->second.isDynamic())
        {
            /// Static config wins over multicluster discovery (same as findDynamicClusters).
            removeDynamicCluster(name);
        }
        else
        {
            LOG_DEBUG(log, "Static discovery cluster '{}' already exists, skip add", name);
            return;
        }
    }

    clusters_info.emplace(
        name,
        ClusterInfo(
            /* name_= */ parsed.name,
            /* zk_name_= */ parsed.zk_name,
            /* zk_root_= */ parsed.zk_root,
            /* host_name= */ parsed.host_name,
            /* username= */ parsed.username,
            /* password= */ parsed.password,
            /* cluster_secret= */ parsed.cluster_secret,
            /* port= */ context->getTCPPort(),
            /* secure= */ parsed.secure,
            /* shard_id= */ parsed.shard_id,
            /* observer_mode= */ parsed.observer,
            /* invisible= */ parsed.invisible));

    /// Re-adding a participant on the same path must not let a stale pending unregister
    /// delete the ephemeral after registerInZk (or before the first upsert).
    if (!parsed.observer)
        cancelPendingUnregister(parsed.zk_name, parsed.zk_root);

    get_nodes_callbacks[name] = std::make_shared<Coordination::WatchCallback>(
        [cluster_name = name, my_clusters_to_update = clusters_to_update](auto)
        {
            my_clusters_to_update->set(cluster_name);
        });

    clusters_to_update->set(name);
}

void ClusterDiscovery::removeStaticCluster(const String & name)
{
    auto it = clusters_info.find(name);
    if (it == clusters_info.end() || it->second.isDynamic())
        return;

    /// Drop local tracking even if Keeper remove fails: config already removed the cluster.
    /// Keep enough identity to retry ephemeral cleanup so peers stop seeing this node.
    if (!unregisterFromZk(it->second))
    {
        pending_zk_unregisters.push_back(PendingZkUnregister{
            .zk_name = it->second.zk_name,
            .zk_root = it->second.zk_root,
            .cluster_name = name,
        });
        LOG_WARNING(
            log,
            "Failed to unregister current node from cluster '{}' on config remove; will retry",
            name);
    }

    clusters_to_update->remove(name);
    get_nodes_callbacks.erase(name);
    clusters_info.erase(it);

    {
        std::lock_guard lock(mutex);
        cluster_impls.erase(name);
    }

    LOG_DEBUG(log, "Static discovery cluster '{}' removed due to config change", name);
}

void ClusterDiscovery::removeDynamicCluster(const String & name)
{
    auto it = clusters_info.find(name);
    if (it == clusters_info.end() || !it->second.isDynamic())
        return;

    removeCluster(name, /* is_dynamic */ true);
    clusters_info.erase(name);
    LOG_DEBUG(log, "Dynamic discovery cluster '{}' removed to make way for static config", name);
}

bool ClusterDiscovery::updateStaticClusterFields(ClusterInfo & info, const ParsedStaticDiscovery & parsed)
{
    bool identity_changed = info.zk_name != parsed.zk_name || info.zk_root != parsed.zk_root;
    if (identity_changed)
        return false;

    bool credentials_changed
        = info.username != parsed.username
        || info.password != parsed.password
        || info.cluster_secret != parsed.cluster_secret
        || info.is_secure_connection != parsed.secure;

    String expected_address = parsed.host_name + ":" + toString(context->getTCPPort());
    bool registration_changed
        = info.current_node_is_observer != parsed.observer
        || info.current_node.shard_id != parsed.shard_id
        || info.current_node.address != expected_address
        || info.current_node.secure != parsed.secure;

    bool invisible_changed = info.current_cluster_is_invisible != parsed.invisible;

    if (!credentials_changed && !registration_changed && !invisible_changed)
        return true;

    info.username = parsed.username;
    info.password = parsed.password;
    info.cluster_secret = parsed.cluster_secret;
    info.is_secure_connection = parsed.secure;
    info.current_node_is_observer = parsed.observer;
    info.current_cluster_is_invisible = parsed.invisible;
    info.current_node = NodeInfo(expected_address, parsed.secure, parsed.shard_id);

    if (registration_changed || invisible_changed)
    {
        /// Force upsertCluster to re-read ZK payloads; membership UUIDs alone do not change
        /// when only address/shard/secure are updated.
        if (registration_changed)
            info.nodes_info.clear();
        clusters_to_update->set(info.name);
    }
    else
        rebuildClusterObject(info);

    return true;
}

void ClusterDiscovery::addMulticlusterRoot(ParsedMulticlusterDiscovery && parsed)
{
    const String full_path = parsed.getFullPath();
    if (multicluster_discovery_paths.contains(full_path))
        return;

    MulticlusterDiscovery mcd(
        /* zk_name */ parsed.zk_name,
        /* zk_path */ parsed.zk_path,
        /* is_secure_connection */ parsed.is_secure_connection,
        /* username */ parsed.username,
        /* password */ parsed.password,
        /* cluster_secret */ parsed.cluster_secret);

    mcd.watch_callback = std::make_shared<Coordination::WatchCallback>(
        [my_need_update = mcd.need_update, my_flag = clusters_to_update](auto)
        {
            my_need_update->store(true);
            my_flag->set();
        });

    multicluster_discovery_paths.emplace(full_path, std::move(mcd));
    clusters_to_update->set();

    LOG_DEBUG(log, "Added multicluster discovery root '{}'", full_path);
}

void ClusterDiscovery::removeMulticlusterRoot(const String & full_path)
{
    if (!multicluster_discovery_paths.erase(full_path))
        return;

    std::vector<String> dynamic_clusters;
    for (const auto & [name, info] : clusters_info)
    {
        if (info.multicluster_full_path == full_path)
            dynamic_clusters.push_back(name);
    }

    for (const auto & name : dynamic_clusters)
        removeDynamicCluster(name);

    clusters_to_update->set();

    LOG_DEBUG(log, "Removed multicluster discovery root '{}'", full_path);
}

bool ClusterDiscovery::updateMulticlusterRootFields(MulticlusterDiscovery & path, const ParsedMulticlusterDiscovery & parsed)
{
    bool credentials_changed
        = path.username != parsed.username
        || path.password != parsed.password
        || path.cluster_secret != parsed.cluster_secret
        || path.is_secure_connection != parsed.is_secure_connection;

    if (!credentials_changed)
        return true;

    path.username = parsed.username;
    path.password = parsed.password;
    path.cluster_secret = parsed.cluster_secret;
    path.is_secure_connection = parsed.is_secure_connection;

    const String full_path = path.getFullPath();
    for (auto & [_, info] : clusters_info)
    {
        if (info.multicluster_full_path != full_path)
            continue;
        info.username = parsed.username;
        info.password = parsed.password;
        info.cluster_secret = parsed.cluster_secret;
        info.is_secure_connection = parsed.is_secure_connection;
        info.current_node.secure = parsed.is_secure_connection;
        rebuildClusterObject(info);
    }

    return true;
}

void ClusterDiscovery::rebuildClusterObject(const ClusterInfo & info)
{
    if (info.current_cluster_is_invisible || info.nodes_info.empty())
    {
        std::lock_guard lock(mutex);
        cluster_impls.erase(info.name);
        return;
    }

    auto cluster = makeCluster(info);
    std::lock_guard lock(mutex);
    cluster_impls[info.name] = std::move(cluster);
}

void ClusterDiscovery::applyParsedConfig(ParsedDiscoveryConfig && parsed)
{
    std::unordered_map<String, ParsedStaticDiscovery> desired_static;
    for (auto & c : parsed.static_clusters)
        desired_static.emplace(c.name, std::move(c));

    std::unordered_map<String, ParsedMulticlusterDiscovery> desired_multi;
    for (auto & r : parsed.multicluster_roots)
        desired_multi.emplace(r.getFullPath(), std::move(r));

    std::vector<String> static_to_remove;
    std::vector<String> static_identity_replace;
    std::vector<ParsedStaticDiscovery> static_to_add;
    std::vector<String> multi_to_remove;
    std::vector<ParsedMulticlusterDiscovery> multi_to_add;

    for (auto & [name, info] : clusters_info)
    {
        if (info.isDynamic())
            continue;

        auto it = desired_static.find(name);
        if (it == desired_static.end())
        {
            static_to_remove.push_back(name);
            continue;
        }

        if (!updateStaticClusterFields(info, it->second))
            static_identity_replace.push_back(name);
        else
            desired_static.erase(it);
    }

    for (auto & [full_path, path] : multicluster_discovery_paths)
    {
        auto it = desired_multi.find(full_path);
        if (it == desired_multi.end())
        {
            multi_to_remove.push_back(full_path);
            continue;
        }

        updateMulticlusterRootFields(path, it->second);
        desired_multi.erase(it);
    }

    for (const auto & name : static_identity_replace)
    {
        auto it = desired_static.find(name);
        if (it != desired_static.end())
        {
            static_to_add.push_back(std::move(it->second));
            desired_static.erase(it);
        }
        static_to_remove.push_back(name);
    }

    for (auto & [_, c] : desired_static)
        static_to_add.push_back(std::move(c));
    for (auto & [_, r] : desired_multi)
        multi_to_add.push_back(std::move(r));

    for (const auto & name : static_to_remove)
        removeStaticCluster(name);
    for (auto & c : static_to_add)
        addStaticCluster(std::move(c));

    for (const auto & full_path : multi_to_remove)
        removeMulticlusterRoot(full_path);
    for (auto & r : multi_to_add)
        addMulticlusterRoot(std::move(r));
}

void ClusterDiscovery::updateFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
{
    LOG_DEBUG(log, "Scheduling cluster discovery config update");
    auto parsed = parseDiscoveryConfig(config, context, config_prefix);
    {
        std::lock_guard lock(pending_config_mutex);
        pending_config_update = std::move(parsed);
    }
    ensureWorkerStarted();
    clusters_to_update->set();
}

bool ClusterDiscovery::consumePendingConfigUpdate()
{
    std::optional<ParsedDiscoveryConfig> pending;
    {
        std::lock_guard lock(pending_config_mutex);
        pending.swap(pending_config_update);
    }
    if (!pending)
        return false;

    LOG_DEBUG(log, "Applying pending cluster discovery config update");
    applyParsedConfig(std::move(*pending));
    return true;
}

void ClusterDiscovery::ensureWorkerStarted()
{
    std::lock_guard lock(start_mutex);
    if (main_thread.joinable())
        return;

    if (clusters_info.empty() && multicluster_discovery_paths.empty())
    {
        std::lock_guard pending_lock(pending_config_mutex);
        if (!pending_config_update)
            return;
        /// Pending update may add the first discovery path; apply it before start().
    }

    /// If worker is not running yet, apply pending config inline so startImpl() sees new clusters.
    consumePendingConfigUpdate();
    startImpl();
}

/// List node in zookeper for cluster
Strings ClusterDiscovery::getNodeNames(zkutil::ZooKeeperPtr & zk,
                                       const String & zk_root,
                                       const String & cluster_name,
                                       int * version,
                                       bool set_callback,
                                       const String & multicluster_full_path)
{
    Coordination::Stat stat;
    Strings nodes;

    if (set_callback)
    {
        auto callback = get_nodes_callbacks.find(cluster_name);
        if (callback == get_nodes_callbacks.end())
        {
            std::shared_ptr<std::atomic_bool> need_update;
            if (!multicluster_full_path.empty())
            {
                auto path_it = multicluster_discovery_paths.find(multicluster_full_path);
                if (path_it != multicluster_discovery_paths.end())
                    need_update = path_it->second.need_update;
            }

            auto watch_dynamic_callback = std::make_shared<Coordination::WatchCallback>([
                cluster_name,
                my_clusters_to_update = clusters_to_update,
                my_discovery_paths_need_update = need_update
                ](auto)
                {
                    if (my_discovery_paths_need_update)
                        my_discovery_paths_need_update->store(true);
                    my_clusters_to_update->set(cluster_name);
                });
            auto res = get_nodes_callbacks.insert(std::make_pair(cluster_name, watch_dynamic_callback));
            callback = res.first;
        }
        nodes = zk->getChildrenWatch(
            getShardsListPath(zk_root),
            &stat,
            Coordination::WatchCallbackPtrOrEventPtr{callback->second, ProfileEvents::ZooKeeperWatchTriggeredClusterDiscovery});
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

/// Checks if cluster membership (set of node UUIDs) changed.
/// Used for logging; payload refresh is decided separately in upsertCluster.
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
    registerInZk(zk, cluster_info);

    int start_version = 0;
    Strings node_uuids = getNodeNames(
        zk, cluster_info.zk_root, cluster_info.name, &start_version, false, cluster_info.multicluster_full_path);
    auto & nodes_info = cluster_info.nodes_info;
    auto on_exit = [this, start_version, &zk, &cluster_info, &nodes_info]()
    {
        /// in case of successful update we still need to check if configuration of cluster still valid and also set watch callback
        int current_version = 0;
        getNodeNames(
            zk, cluster_info.zk_root, cluster_info.name, &current_version, true, cluster_info.multicluster_full_path);

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
        std::lock_guard lock(mutex);
        cluster_impls.erase(cluster_info.name);
        return true;
    }

    if (!needUpdate(node_uuids, nodes_info))
        LOG_DEBUG(log, "Membership unchanged for cluster '{}', refreshing node payloads", cluster_info.name);

    /// Always re-read ephemeral payloads so hostname/shard/secure updates propagate even when
    /// the UUID set is unchanged (createOrUpdate does not fire children watches by itself;
    /// registerInZk recreates the node when data changes to notify peers).
    nodes_info = getNodes(zk, cluster_info.zk_root, node_uuids);

    if (bool ok = on_exit(); !ok)
        return false;

    LOG_DEBUG(log, "Updating system.clusters record for '{}' with {} nodes", cluster_info.name, cluster_info.nodes_info.size());

    if (nodes_info.empty())
    {
        String name = cluster_info.name;
        if (cluster_info.isDynamic())
            removeDynamicCluster(name);
        else
            removeCluster(name, /* is_dynamic */ false);
        return true;
    }

    rebuildClusterObject(cluster_info);
    return true;
}

void ClusterDiscovery::removeCluster(const String & name, bool is_dynamic)
{
    {
        std::lock_guard lock(mutex);
        cluster_impls.erase(name);
    }
    /// For static clusters (defined in config), `clusters_to_update` and `get_nodes_callbacks`
    /// are initialized once at startup and must persist so the cluster can be re-registered after
    /// a ZooKeeper session loss. Dynamic clusters own their entries and must clean them up.
    if (is_dynamic)
    {
        clusters_to_update->remove(name);
        get_nodes_callbacks.erase(name);
        LOG_DEBUG(log, "Dynamic cluster '{}' removed successfully", name);
    }
}

void ClusterDiscovery::registerInZk(zkutil::ZooKeeperPtr & zk, ClusterInfo & info)
{
    /// Create root node in observer mode not to get 'No node' error
    String node_path = getShardsListPath(info.zk_root) / current_node_name;
    zk->createAncestors(node_path);

    if (info.current_node_is_observer)
    {
        /// Drop leftover ephemeral registration when transitioning from participant to observer
        /// (or if a stale node remained). ZNONODE means already absent.
        auto code = zk->tryRemove(node_path);
        if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
            throw Exception(
                ErrorCodes::KEEPER_EXCEPTION,
                "Cannot remove discovery registration for observer node {}: {}",
                node_path,
                Coordination::errorMessage(code));
        LOG_DEBUG(log, "Current node {} is observer of cluster {}", current_node_name, info.name);
        return;
    }

    LOG_DEBUG(log, "Registering current node {} in cluster {}", current_node_name, info.name);

    const String payload = info.current_node.serialize();
    String existing;
    if (zk->tryGet(node_path, existing))
    {
        if (existing == payload)
        {
            LOG_DEBUG(log, "Current node {} already registered in cluster {} with up-to-date data", current_node_name, info.name);
            cancelPendingUnregister(info.zk_name, info.zk_root);
            return;
        }
        /// Recreate ephemeral so children watches fire; setData alone does not notify peers.
        zk->tryRemove(node_path);
    }

    zk->create(node_path, payload, zkutil::CreateMode::Ephemeral);
    cancelPendingUnregister(info.zk_name, info.zk_root);
    LOG_DEBUG(log, "Current node {} registered in cluster {}", current_node_name, info.name);
}

bool ClusterDiscovery::tryUnregisterPath(const String & zk_name, const String & zk_root, const String & cluster_name_for_log)
{
    fiu_do_on(FailPoints::cluster_discovery_unregister_fail,
    {
        throw Exception(
            ErrorCodes::KEEPER_EXCEPTION,
            "Failpoint cluster_discovery_unregister_fail is triggered for cluster '{}'",
            cluster_name_for_log);
    });

    auto zk = context->getDefaultOrAuxiliaryZooKeeper(zk_name);
    String node_path = getShardsListPath(zk_root) / current_node_name;
    auto code = zk->tryRemove(node_path);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
    {
        LOG_WARNING(
            log,
            "Cannot unregister current node {} from cluster '{}': {}",
            current_node_name,
            cluster_name_for_log,
            Coordination::errorMessage(code));
        return false;
    }

    LOG_DEBUG(log, "Current node {} unregistered from cluster {}", current_node_name, cluster_name_for_log);
    return true;
}

bool ClusterDiscovery::unregisterFromZk(const ClusterInfo & info)
{
    try
    {
        return tryUnregisterPath(info.zk_name, info.zk_root, info.name);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error while unregistering node from cluster '" + info.name + "'");
        return false;
    }
}

void ClusterDiscovery::cancelPendingUnregister(const String & zk_name, const String & zk_root)
{
    std::erase_if(
        pending_zk_unregisters,
        [&](const PendingZkUnregister & pending)
        {
            return pending.zk_name == zk_name && pending.zk_root == zk_root;
        });
}

bool ClusterDiscovery::hasActiveParticipantOnPath(const String & zk_name, const String & zk_root) const
{
    for (const auto & [_, info] : clusters_info)
    {
        if (!info.current_node_is_observer && info.zk_name == zk_name && info.zk_root == zk_root)
            return true;
    }
    return false;
}

bool ClusterDiscovery::retryPendingUnregisters()
{
    if (pending_zk_unregisters.empty())
        return true;

    std::vector<PendingZkUnregister> still_pending;
    still_pending.reserve(pending_zk_unregisters.size());

    for (const auto & pending : pending_zk_unregisters)
    {
        /// Cluster was re-added on this path; do not delete its live ephemeral.
        if (hasActiveParticipantOnPath(pending.zk_name, pending.zk_root))
        {
            LOG_DEBUG(
                log,
                "Skipping pending unregister for cluster '{}' because a participant is active on the same path",
                pending.cluster_name);
            continue;
        }

        bool ok = false;
        try
        {
            ok = tryUnregisterPath(pending.zk_name, pending.zk_root, pending.cluster_name);
        }
        catch (...)
        {
            tryLogCurrentException(
                log,
                "Error while retrying unregister from cluster '" + pending.cluster_name + "'");
        }

        if (!ok)
            still_pending.push_back(pending);
    }

    pending_zk_unregisters = std::move(still_pending);
    return pending_zk_unregisters.empty();
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

    for (const auto & [_, path] : multicluster_discovery_paths)
    {
        auto zk = context->getDefaultOrAuxiliaryZooKeeper(path.zk_name);

        zk->createAncestors(path.zk_path);
        zk->createIfNotExists(path.zk_path, "");
    }

    findDynamicClusters(clusters_info);

    std::vector<String> cluster_names;
    cluster_names.reserve(clusters_info.size());
    for (const auto & [name, _] : clusters_info)
        cluster_names.push_back(name);

    for (const auto & name : cluster_names)
    {
        auto it = clusters_info.find(name);
        if (it == clusters_info.end())
            continue;

        auto & info = it->second;
        auto zk = context->getDefaultOrAuxiliaryZooKeeper(info.zk_name);
        registerInZk(zk, info);
        if (!upsertCluster(info))
        {
            LOG_WARNING(log, "Error on initial cluster '{}' update, will retry in background", name);
            clusters_to_update->set(name);
        }
        else if (auto after = clusters_info.find(name); after != clusters_info.end() && after->second.isDynamic())
            clusters_to_update->set(name, false);
    }

    LOG_DEBUG(log, "Initialized");
    is_initialized = true;
}

void ClusterDiscovery::findDynamicClusters(
    std::unordered_map<String, ClusterDiscovery::ClusterInfo> & info,
    std::unordered_set<String> * unchanged_roots)
{
    using namespace std::chrono_literals;

    constexpr auto force_update_interval = 2min;

    for (auto & [full_path, path] : multicluster_discovery_paths)
    {
        if (unchanged_roots)
        {
            if (!path.need_update->exchange(false))
            {
                /// force updating periodically
                bool force_update = path.watch.elapsedSeconds() > std::chrono::seconds(force_update_interval).count();
                if (!force_update)
                {
                    unchanged_roots->insert(full_path);
                    continue;
                }
            }
        }

        auto zk = context->getDefaultOrAuxiliaryZooKeeper(path.zk_name);
        zk->createAncestors(path.zk_path);
        zk->createIfNotExists(path.zk_path, "");

        auto clusters = zk->getChildrenWatch(
            path.zk_path,
            nullptr,
            Coordination::WatchCallbackPtrOrEventPtr{path.watch_callback, ProfileEvents::ZooKeeperWatchTriggeredClusterDiscovery});

        for (const auto & cluster : clusters)
        {
            auto p = clusters_info.find(cluster);
            if (p != clusters_info.end() && !p->second.isDynamic())
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
                    /* multicluster_full_path_= */ full_path
                )
            );
        }

        path.watch.restart();
    }
}

void ClusterDiscovery::start()
{
    std::lock_guard lock(start_mutex);
    startImpl();
}

void ClusterDiscovery::startImpl()
{
    if (main_thread.joinable())
        return;

    if (clusters_info.empty() && multicluster_discovery_paths.empty())
    {
        LOG_DEBUG(log, "No defined clusters for discovery");
        return;
    }

    try
    {
        auto component_guard = Coordination::setCurrentComponent("ClusterDiscovery::start");
        /// Apply any queued reload before the first init attempt (same rationale as runMainThread).
        consumePendingConfigUpdate();
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
                if (clusters_to_update->isStopped())
                    break;
            }
            if (finish || clusters_to_update->isStopped())
                break;

            /// Interruptible backoff so shutdown does not wait for a long sleep after errors.
            for (auto remaining = backoff_timeout; remaining.count() > 0 && !clusters_to_update->isStopped();)
            {
                constexpr auto slice = std::chrono::milliseconds(50);
                auto step = remaining < slice ? remaining : slice;
                std::this_thread::sleep_for(step);
                remaining -= step;
            }
            if (clusters_to_update->isStopped())
                break;
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

    /// Pending reloads must be applied before retrying init. Otherwise a worker that keeps
    /// failing initialUpdate (bad Keeper path, etc.) never reaches the loop body consumer and
    /// updateFromConfig stays stuck while ensureWorkerStarted no-ops on the running thread.
    consumePendingConfigUpdate();
    retryPendingUnregisters();

    if (!is_initialized)
        initialUpdate();

    bool finished = false;
    while (!finished)
    {
        bool all_up_to_date = true;
        auto clusters = clusters_to_update->wait(finished);
        if (finished)
            break;

        consumePendingConfigUpdate();

        if (!retryPendingUnregisters())
        {
            /// Keep waking the loop with a short interruptible backoff so ephemeral cleanup
            /// retries without failing / rolling back the already-applied config update.
            using namespace std::chrono_literals;
            for (auto remaining = std::chrono::milliseconds(1000);
                 remaining.count() > 0 && !clusters_to_update->isStopped();)
            {
                constexpr auto slice = std::chrono::milliseconds(50);
                auto step = remaining < slice ? remaining : slice;
                std::this_thread::sleep_for(step);
                remaining -= step;
            }
            if (!clusters_to_update->isStopped())
                clusters_to_update->set();
        }

        std::unordered_map<String, ClusterInfo> new_dynamic_clusters_info;
        std::unordered_set<String> unchanged_roots;
        findDynamicClusters(new_dynamic_clusters_info, &unchanged_roots);

        std::unordered_set<String> clusters_to_insert;
        std::unordered_set<String> clusters_to_remove;

        /// Remove clusters that are not found in new_dynamic_clusters_info
        for (const auto & [cluster_name, info] : clusters_info)
        {
            if (!info.isDynamic())
                continue;
            if (!new_dynamic_clusters_info.erase(cluster_name)
                && !unchanged_roots.contains(info.multicluster_full_path))
                clusters_to_remove.insert(cluster_name);
        }
        /// new_dynamic_clusters_info now contains only new clusters
        for (const auto & [cluster_name, _] : new_dynamic_clusters_info)
            clusters_to_insert.insert(cluster_name);

        for (const auto & cluster_name : clusters_to_remove)
            removeDynamicCluster(cluster_name);

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

            String name = cluster_name;
            if (upsertCluster(cluster_info))
            {
                cluster_info_it = clusters_info.find(name);
                if (cluster_info_it != clusters_info.end())
                    cluster_info_it->second.watch.restart();
                LOG_DEBUG(log, "Cluster '{}' updated successfully", name);
            }
            else
            {
                all_up_to_date = false;
                /// no need to trigger convar, will retry after timeout in `wait`
                clusters_to_update->set(name);
                LOG_WARNING(log, "Cluster '{}' wasn't updated, will retry", name);
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
            String name = cluster_name;
            if (upsertCluster(cluster_info))
            {
                cluster_info_it = clusters_info.find(name);
                if (cluster_info_it != clusters_info.end())
                    cluster_info_it->second.watch.restart();
                LOG_DEBUG(log, "Dynamic cluster '{}' inserted successfully", name);
            }
            else
            {
                all_up_to_date = false;
                /// no need to trigger convar, will retry after timeout in `wait`
                clusters_to_update->set(name);
                LOG_WARNING(log, "Dynamic cluster '{}' wasn't inserted, will retry", name);
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
    if (clusters_to_update)
        clusters_to_update->stop();

    /// Wait for any in-flight startImpl() before joining so we do not race ThreadFromGlobalPool assign.
    std::lock_guard lock(start_mutex);
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
