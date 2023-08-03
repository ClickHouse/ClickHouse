#pragma once

#include <unordered_map>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <Common/ZooKeeper/ZooKeeper.h>


namespace DB
{

struct Settings;

struct ReplicatedMergeTreeClusterReplica : public ReplicatedMergeTreeAddress
{
    String name;
    bool is_lost = true;
    int is_lost_version = -1;
    size_t log_pointer = 0;

    String toStringForLog() const;

    /// To avoid ambiguity
    String toString() = delete;
    void fromString(const String &) = delete;

    void fromCoordinator(const zkutil::ZooKeeperPtr & zookeeper, const String & zookeeper_path, const String & name_);

    Cluster::ShardInfo makeShardInfo(const Settings & settings, bool treat_local_port_as_remote) const;
    Cluster::ShardInfo makeShardInfo(const ContextPtr & context) const;
};
using ReplicatedMergeTreeClusterReplicas = std::unordered_map<String, ReplicatedMergeTreeClusterReplica>;

}
