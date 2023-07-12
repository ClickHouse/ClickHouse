#pragma once

#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct Settings;

struct ReplicatedMergeTreeClusterReplica : public ReplicatedMergeTreeAddress
{
    String name;

    String toStringForLog() const;
    /// To avoid ambiguity
    String toString() = delete;

    Cluster::ShardInfo makeShardInfo(const Settings & settings, bool treat_local_port_as_remote) const;
    Cluster::ShardInfo makeShardInfo(const ContextPtr & context) const;
};
using ReplicatedMergeTreeClusterReplicas = std::vector<ReplicatedMergeTreeClusterReplica>;

}
