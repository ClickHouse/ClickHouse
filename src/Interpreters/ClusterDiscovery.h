#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/Common.h>
#include <Interpreters/Cluster.h>

#include <Poco/Logger.h>

#include <base/defines.h>

#include <unordered_map>

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
        const String & config_prefix = "remote_servers");

    void start();

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

        /// For dynamic clusters, index+1 in multicluster_discovery_paths where cluster was found
        /// 0 for static ckusters
        size_t zk_root_index;

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
                    size_t zk_root_index_ = 0
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
    };

    void initialUpdate();

    void registerInZk(zkutil::ZooKeeperPtr & zk, ClusterInfo & info);

    Strings getNodeNames(zkutil::ZooKeeperPtr & zk,
                         const String & zk_root,
                         const String & cluster_name,
                         int * version,
                         bool set_callback,
                         size_t zk_root_index);

    NodesInfo getNodes(zkutil::ZooKeeperPtr & zk, const String & zk_root, const Strings & node_uuids);

    ClusterPtr makeCluster(const ClusterInfo & cluster_info);

    bool needUpdate(const Strings & node_uuids, const NodesInfo & nodes);
    bool upsertCluster(ClusterInfo & cluster_info);
    void removeCluster(const String & name);

    bool runMainThread(std::function<void()> up_to_date_callback);
    void shutdown();

    void findDynamicClusters(std::unordered_map<String, ClusterInfo> & info, std::unordered_set<size_t> * unchanged_roots = nullptr);

    /// cluster name -> cluster info (zk root, set of nodes)
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
    ThreadFromGlobalPool main_thread;

    LoggerPtr log;

    struct MulticlusterDiscovery
    {
        const String zk_name;
        const String zk_path;
        bool is_secure_connection;
        String username;
        String password;
        String cluster_secret;

        Stopwatch watch;
        mutable std::atomic_bool need_update;

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
        {
            need_update = true;
        }

        String getFullPath() const { return zk_name + ":" + zk_path; }
    };

    std::shared_ptr<std::vector<std::shared_ptr<MulticlusterDiscovery>>> multicluster_discovery_paths;
};

}
