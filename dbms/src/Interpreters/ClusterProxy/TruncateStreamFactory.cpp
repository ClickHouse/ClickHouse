#include <Interpreters/ClusterProxy/TruncateStreamFactory.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Interpreters/Cluster.h>
#include <ext/range.h>

namespace DB
{

namespace ClusterProxy
{

TruncateStreamFactory::TruncateStreamFactory(ClusterPtr & cluster_) : cluster(cluster_)
{
}

void TruncateStreamFactory::createForShard(
    const Cluster::ShardInfo & shard_info,
    const String & query, const ASTPtr & query_ast,
    const ThrottlerPtr & throttler, Context & context,
    BlockInputStreams & res)
{
    if (shard_info.isLocal())
    {
        InterpreterDropQuery drop_query{query_ast, context};
        BlockIO drop_res = drop_query.execute();

        if (drop_res.in)
            res.emplace_back(std::move(drop_res.in));
    }

    if (!shard_info.hasInternalReplication() || !shard_info.isLocal())
    {
        Cluster::Addresses replicas = getShardReplicasAddresses(shard_info);

        for (size_t replica_index : ext::range(0, replicas.size()))
        {
            if (!replicas[replica_index].is_local)
            {
                if (const auto & connection_pool = shard_info.per_replica_pools.at(replica_index))
                {
                    auto entry = connection_pool->get(&context.getSettingsRef());
                    auto remote_stream = std::make_shared<RemoteBlockInputStream>(*entry, query, Block{}, context, nullptr, throttler);
                    remote_stream->setPoolMode(PoolMode::GET_ONE);
                    remote_stream->appendExtraInfo();
                    res.emplace_back(std::move(remote_stream));

                    if (shard_info.hasInternalReplication())
                        break;

                    continue;
                }

                throw Exception("Connection pool for replica " + replicas[replica_index].readableString() + " does not exist", ErrorCodes::LOGICAL_ERROR);
            }
        }
    }
}

Cluster::Addresses TruncateStreamFactory::getShardReplicasAddresses(const Cluster::ShardInfo & info)
{
    const auto addresses_with_failovers = cluster->getShardsAddresses();
    return addresses_with_failovers[info.shard_num - 1];
}

}
}

