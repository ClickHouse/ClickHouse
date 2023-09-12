#include <Storages/MergeTree/ReplicatedMergeTreeClusterReplica.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Interpreters/Context.h>

namespace DB
{

String ReplicatedMergeTreeClusterReplica::toStringForLog() const
{
    return fmt::format("{}({}:{}#{}.{})", name, host, queries_port, database, table);
}
void ReplicatedMergeTreeClusterReplica::fromCoordinator(const zkutil::ZooKeeperPtr & zookeeper, const String & zookeeper_path, const String & name_)
try
{
    const String & replica_path = fs::path(zookeeper_path) / "replicas" / name_;

    ReplicatedMergeTreeAddress::fromString(zookeeper->get(fs::path(replica_path) / "host"));

    ///
    /// Additional fields of ReplicatedMergeTreeClusterReplica
    ///
    name = name_;

    bool is_new_replica;
    is_lost = StorageReplicatedMergeTree::isReplicaLost(zookeeper, replica_path, is_new_replica, is_lost_version);

    String log_pointer_res;
    if (zookeeper->tryGet(fs::path(replica_path) / "log_pointer", log_pointer_res) && !log_pointer_res.empty())
        log_pointer = parse<UInt64>(log_pointer_res);
}
catch (...)
{
    /// NOTE: The error should be ignored, since some may replicas may be
    /// removed, and we should not fail updating the map in this case.
    tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("while resolving {}", name_));
}

Cluster::ShardInfo ReplicatedMergeTreeClusterReplica::makeShardInfo(const Settings & settings, bool treat_local_port_as_remote) const
{
    /// TODO(cluster): implement:
    /// - username
    /// - password
    /// - secure
    ClusterConnectionParameters connection_params{
        .username = "",
        .password = "",
        .clickhouse_port = queries_port,
        .treat_local_as_remote = false,
        .treat_local_port_as_remote = treat_local_port_as_remote,
        .secure = false,
        .priority = Priority{1},
        .cluster_name = "",
        .cluster_secret = "",
    };

    DatabaseReplicaInfo replica_info{
        .hostname = host,
        .shard_name = "",
        .replica_name = "",
    };

    Cluster::Address address(
        replica_info,
        connection_params,
        /* shard_index_= */ 0,
        /* replica_index_= */ 0);

    Cluster::ShardInfo shard_info;
    {
        shard_info.shard_num = 0;
        if (address.is_local)
            shard_info.local_addresses.push_back(address);
        auto pool = ConnectionPoolFactory::instance().get(
            static_cast<unsigned>(settings.distributed_connections_pool_size),
            address.host_name, address.port,
            address.default_database, address.user, address.password, address.quota_key,
            address.cluster, address.cluster_secret,
            "server", address.compression,
            address.secure, address.priority);
        shard_info.pool = std::make_shared<ConnectionPoolWithFailover>(
            ConnectionPoolPtrs{pool}, settings.load_balancing);
        shard_info.per_replica_pools = {std::move(pool)};
    }

    return shard_info;
}

Cluster::ShardInfo ReplicatedMergeTreeClusterReplica::makeShardInfo(const ContextPtr & context) const
{
    bool treat_local_port_as_remote = context->getApplicationType() == Context::ApplicationType::LOCAL;
    return makeShardInfo(context->getSettingsRef(), treat_local_port_as_remote);
}

}
