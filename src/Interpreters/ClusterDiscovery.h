#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/Common.h>

#include <Poco/Logger.h>

#include <base/defines.h>

#include <unordered_map>

namespace DB
{

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
    Strings getNodeNames(zkutil::ZooKeeperPtr & zk, const String & zk_root, const String & cluster_name);
    Strings getNodes(zkutil::ZooKeeperPtr & zk, const String & zk_root, const Strings & nodes);

    void updateCluster(const String & cluster_name);
    void updateCluster(const String & cluster_name, const String & zk_root);

    void runMainThread();
    void shutdown();

    /// cluster name -> path in zk
    std::unordered_map<String, String> clusters;

    ContextMutablePtr context;

    String node_name;
    UInt16 server_port;

    using UpdateQueue = ConcurrentBoundedQueue<std::string>;
    std::shared_ptr<UpdateQueue> queue;
    std::atomic<bool> stop_flag = false;
    ThreadFromGlobalPool main_thread;

    Poco::Logger * log;
};

}
