#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/Common.h>
#include <base/getFQDNOrHostName.h>
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
        const String zk_root;
        NodesInfo nodes_info;

        /// Track last update time
        Stopwatch watch;

        NodeInfo current_node;

        explicit ClusterInfo(const String & name_, const String & zk_root_, UInt16 port, bool secure, size_t shard_id)
            : name(name_)
            , zk_root(zk_root_)
            , current_node(getFQDNOrHostName() + ":" + toString(port), secure, shard_id)
        {
        }
    };

    void initialUpdate();

    void registerInZk(zkutil::ZooKeeperPtr & zk, ClusterInfo & info);

    Strings getNodeNames(zkutil::ZooKeeperPtr & zk,
                         const String & zk_root,
                         const String & cluster_name,
                         int * version = nullptr,
                         bool set_callback = true);

    NodesInfo getNodes(zkutil::ZooKeeperPtr & zk, const String & zk_root, const Strings & node_uuids);

    ClusterPtr makeCluster(const ClusterInfo & cluster_info);

    bool needUpdate(const Strings & node_uuids, const NodesInfo & nodes);
    bool updateCluster(ClusterInfo & cluster_info);

    bool runMainThread(std::function<void()> up_to_date_callback);
    void shutdown();

    /// cluster name -> cluster info (zk root, set of nodes)
    std::unordered_map<String, ClusterInfo> clusters_info;

    ContextMutablePtr context;

    String current_node_name;

    template <typename T> class ConcurrentFlags;
    using UpdateFlags = ConcurrentFlags<std::string>;

    /// Cluster names to update.
    /// The `shared_ptr` is used because it's passed to watch callback.
    /// It prevents accessing to invalid object after ClusterDiscovery is destroyed.
    std::shared_ptr<UpdateFlags> clusters_to_update;

    ThreadFromGlobalPool main_thread;

    Poco::Logger * log;
};

}
