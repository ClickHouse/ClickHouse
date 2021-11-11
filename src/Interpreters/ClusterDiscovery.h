#pragma once

#include <unordered_map>
#include <base/defines.h>
#include <Poco/Logger.h>
#include <Common/ZooKeeper/Common.h>


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


private:
    Strings getNodes(zkutil::ZooKeeperPtr & zk, const String & zk_root, const Strings & nodes);
    void updateCluster(const String & cluster_name, const String & zk_root);

    /// cluster name -> path in zk
    std::unordered_map<String, String> clusters;

    ContextMutablePtr context;

    String node_name;
    UInt16 server_port;

    Poco::Logger * log;
};

}
