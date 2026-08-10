#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/Common.h>
#include <Interpreters/Cluster.h>

#include <Poco/Logger.h>

#include <base/defines.h>

#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

/*
 * Discover cluster nodes.
 *
 * Each node adds ephemernal node into specified path in zookeeper (each cluster have own path).
 * Also node subscribed for updates for these paths, and at each child node chanhe cluster updated.
 * When node goes down ephemernal node are destroyed, cluster configuration is updated on other node and gone node is removed from cluster.
 */
class ClusterDiscovery
{

public:
    ClusterDiscovery(
        const Poco::Util::AbstractConfiguration & config,
        ContextPtr context_,
        MultiVersion<Macros>::Version macros_,
        const String & config_prefix = "remote_servers");

    void start();

    /// Apply changes from reloaded remote_servers config (credentials, add/remove discovery paths).
    /// Safe to call from the config-reloader thread; the update is applied on the discovery worker.
    void updateFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix = "remote_servers");

    /// Throws if discovery subtrees under `config_prefix` are invalid. No side effects.
    /// Call before committing Clusters / clusters_config so a bad reload cannot partially apply.
    static void validateConfig(
        const Poco::Util::AbstractConfiguration & config,
        ContextPtr context,
        const String & config_prefix = "remote_servers");

    ClusterPtr getCluster(const String & cluster_name) const;
    std::unordered_map<String, ClusterPtr> getClusters() const;

    ~ClusterDiscovery();

private:
    struct NodeInfo
    {
        /// versioning for format of data stored in zk
        static constexpr size_t data_ver = 1;

        /// host:port
        String address;
        /// is secure tcp port user
        bool secure = false;
        /// shard number
        size_t shard_id = 0;

        NodeInfo() = default;
        explicit NodeInfo(const String & address_, bool secure_, size_t shard_id_)
            : address(address_)
            , secure(secure_)
            , shard_id(shard_id_)
        {}

        static bool parse(const String & data, NodeInfo & result);
        String serialize() const;
    };

    // node uuid -> address ("host:port")
    using NodesInfo = std::unordered_map<String, NodeInfo>;

    struct ClusterInfo
    {
        const String name;
        const String zk_name;
        const String zk_root;
        NodesInfo nodes_info;

        /// Track last update time
        Stopwatch watch;

        NodeInfo current_node;
        /// Current node may not belong to cluster, to be just an observer.
        bool current_node_is_observer = false;

        /// For internal management need.
        /// Is it designed that when deploying multiple compute groups,
        /// they are mutually invisible to each other.
        bool current_cluster_is_invisible = false;

        bool is_secure_connection = false;
        String username;
        String password;
        String cluster_secret;

        /// For dynamic clusters: MulticlusterDiscovery::getFullPath() where cluster was found.
        /// Empty for static clusters defined with <path>.
        String multicluster_full_path;

        bool isDynamic() const { return !multicluster_full_path.empty(); }

        ClusterInfo(const String & name_,
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
                    const String & multicluster_full_path_ = {}
                    );
    };

    struct ParsedStaticDiscovery
    {
        String name;
        String zk_name;
        String zk_root;
        String host_name;
        String username;
        String password;
        String cluster_secret;
        bool secure = false;
        size_t shard_id = 0;
        bool observer = false;
        bool invisible = false;
    };

    struct ParsedMulticlusterDiscovery
    {
        String zk_name;
        String zk_path;
        bool is_secure_connection = false;
        String username;
        String password;
        String cluster_secret;

        String getFullPath() const { return zk_name + ":" + zk_path; }
    };

    struct ParsedDiscoveryConfig
    {
        std::vector<ParsedStaticDiscovery> static_clusters;
        std::vector<ParsedMulticlusterDiscovery> multicluster_roots;
    };

    struct MulticlusterDiscovery
    {
        const String zk_name;
        const String zk_path;
        bool is_secure_connection;
        String username;
        String password;
        String cluster_secret;

        mutable Stopwatch watch;
        mutable std::shared_ptr<std::atomic_bool> need_update;
        Coordination::WatchCallbackPtr watch_callback;

        MulticlusterDiscovery(const String & zk_name_,
                              const String & zk_path_,
                              bool is_secure_connection_,
                              const String & username_,
                              const String & password_,
                              const String & cluster_secret_)
            : zk_name(zk_name_)
            , zk_path(zk_path_)
            , is_secure_connection(is_secure_connection_)
            , username(username_)
            , password(password_)
            , cluster_secret(cluster_secret_)
            , need_update(std::make_shared<std::atomic_bool>(true))
        {}

        String getFullPath() const { return zk_name + ":" + zk_path; }
    };

    static ParsedDiscoveryConfig parseDiscoveryConfig(
        const Poco::Util::AbstractConfiguration & config,
        ContextPtr context,
        const String & config_prefix);

    void applyParsedConfig(ParsedDiscoveryConfig && parsed);
    void addStaticCluster(ParsedStaticDiscovery && parsed);
    void removeStaticCluster(const String & name);
    /// Remove a multicluster-discovered cluster so a static config entry can take its name.
    void removeDynamicCluster(const String & name);
    bool updateStaticClusterFields(ClusterInfo & info, const ParsedStaticDiscovery & parsed);
    void addMulticlusterRoot(ParsedMulticlusterDiscovery && parsed);
    void removeMulticlusterRoot(const String & full_path);
    bool updateMulticlusterRootFields(MulticlusterDiscovery & path, const ParsedMulticlusterDiscovery & parsed);

    void rebuildClusterObject(const ClusterInfo & info);
    void ensureWorkerStarted();
    bool consumePendingConfigUpdate();

    /// Assumes start_mutex is held. Starts the worker at most once.
    void startImpl();

    void initialUpdate();

    void registerInZk(zkutil::ZooKeeperPtr & zk, ClusterInfo & info);

    struct PendingZkUnregister
    {
        String zk_name;
        String zk_root;
        String cluster_name;
    };

    /// Returns false if Keeper remove failed; caller should queue a retry.
    bool unregisterFromZk(const ClusterInfo & info);
    bool tryUnregisterPath(const String & zk_name, const String & zk_root, const String & cluster_name_for_log);
    /// Retries failed unregisters. Returns true when the queue is empty.
    bool retryPendingUnregisters();

    Strings getNodeNames(zkutil::ZooKeeperPtr & zk,
                         const String & zk_root,
                         const String & cluster_name,
                         int * version,
                         bool set_callback,
                         const String & multicluster_full_path);

    NodesInfo getNodes(zkutil::ZooKeeperPtr & zk, const String & zk_root, const Strings & node_uuids);

    ClusterPtr makeCluster(const ClusterInfo & cluster_info);

    bool needUpdate(const Strings & node_uuids, const NodesInfo & nodes);
    bool upsertCluster(ClusterInfo & cluster_info);
    void removeCluster(const String & name, bool is_dynamic);

    bool runMainThread(std::function<void()> up_to_date_callback);
    void shutdown();

    void findDynamicClusters(
        std::unordered_map<String, ClusterInfo> & info,
        std::unordered_set<String> * unchanged_roots = nullptr);

    /// cluster name -> cluster info (zk root, set of nodes)
    /// Mutated only from constructor (before start) and the discovery worker thread.
    std::unordered_map<String, ClusterInfo> clusters_info;

    ContextMutablePtr context;

    String current_node_name;

    template <typename T> class Flags;
    using UpdateFlags = Flags<std::string>;

    /// Cluster names to update.
    /// The `shared_ptr` is used because it's passed to watch callback.
    /// It prevents accessing to invalid object after ClusterDiscovery is destroyed.
    std::shared_ptr<UpdateFlags> clusters_to_update;

    /// Hold the callback pointers of each cluster.
    /// To avoid registering callbacks for the same path multiple times.
    std::unordered_map<String, Coordination::WatchCallbackPtr> get_nodes_callbacks;

    mutable std::mutex mutex;
    std::unordered_map<String, ClusterPtr> cluster_impls;

    bool is_initialized = false;

    /// Serializes start() / ensureWorkerStarted() so concurrent config reload and
    /// startClusterDiscovery cannot double-assign main_thread (ThreadFromGlobalPool aborts).
    /// Lock order: start_mutex before pending_config_mutex.
    mutable std::mutex start_mutex;
    ThreadFromGlobalPool main_thread;

    LoggerPtr log;

    /// Keyed by MulticlusterDiscovery::getFullPath()
    std::unordered_map<String, MulticlusterDiscovery> multicluster_discovery_paths;

    /// Config reload posts parsed config here; worker applies it.
    /// Never take this lock while a caller without start_mutex may later take start_mutex
    /// while holding this one: lock order is start_mutex -> pending_config_mutex.
    mutable std::mutex pending_config_mutex;
    std::optional<ParsedDiscoveryConfig> pending_config_update;

    /// Ephemeral registrations that failed to remove during config apply.
    /// Local cluster state is already dropped; retry from the worker thread.
    /// Accessed only from the discovery worker (same as clusters_info).
    std::vector<PendingZkUnregister> pending_zk_unregisters;

    MultiVersion<Macros>::Version macros;
};

}
