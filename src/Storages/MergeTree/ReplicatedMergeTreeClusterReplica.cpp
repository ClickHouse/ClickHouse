#include <Storages/MergeTree/ReplicatedMergeTreeClusterReplica.h>
#include <Interpreters/Context.h>

namespace DB
{

String ReplicatedMergeTreeClusterReplica::toStringForLog() const
{
    return fmt::format("{}({}:{}#{}.{})", name, host, queries_port, database, table);
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
