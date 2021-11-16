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
 * Also node subscribed for updates for theese paths, and at each child node chanhe cluster updated.
 * When node goes down ephemernal node are destroyed, cluster configuration is updated on other node and gone node is removed from cluster.
 */
class ClusterDiscovery
{

public:
    ClusterDiscovery(
        const Poco::Util::AbstractConfiguration & config,
        ContextMutablePtr context_,
        const String & config_prefix = "remote_servers_discovery");

    void start();

    ~ClusterDiscovery();

private:
    // node uuid -> address ("host:port")
    using NodesInfo = std::unordered_map<String, String>;

    struct ClusterInfo
    {
        const String name;
        const String zk_root;
        NodesInfo nodes_info;

        explicit ClusterInfo(const String & name_, const String & zk_root_) : name(name_), zk_root(zk_root_) {}
    };

    void registerInZk(zkutil::ZooKeeperPtr & zk, ClusterInfo & info);

    Strings getNodeNames(zkutil::ZooKeeperPtr & zk,
                         const String & zk_root,
                         const String & cluster_name,
                         int * version = nullptr,
                         bool set_callback = true);

    NodesInfo getNodes(zkutil::ZooKeeperPtr & zk, const String & zk_root, const Strings & nodes);

    ClusterPtr getCluster(const ClusterInfo & cluster_info);

    static bool needUpdate(const Strings & node_uuids, const NodesInfo & nodes);
    bool updateCluster(const String & cluster_name);
    bool updateCluster(ClusterInfo & cluster_info);

    void runMainThread();
    void shutdown();

    /// cluster name -> cluster info (zk root, set of nodes)
    std::unordered_map<String, ClusterInfo> clusters_info;

    ContextMutablePtr context;

    String node_name;
    UInt16 server_port;

    /// Cluster names to update
    using UpdateQueue = ConcurrentBoundedQueue<std::string>;

    /// shared_ptr is used because it's passed to watch callback
    /// it prevents accessing to invalid queue after ClusterDiscovery is destroyed
    std::shared_ptr<UpdateQueue> queue;

    std::atomic<bool> stop_flag = false;
    ThreadFromGlobalPool main_thread;

    Poco::Logger * log;
};

}
