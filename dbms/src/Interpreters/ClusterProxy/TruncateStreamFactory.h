#pragma once

#include <Interpreters/ClusterProxy/IStreamFactory.h>

namespace DB
{

namespace ClusterProxy
{

class TruncateStreamFactory final : public IStreamFactory
{
public:

    TruncateStreamFactory(ClusterPtr & cluster);

    void createForShard(
        const Cluster::ShardInfo & shard_info,
        const String & query, const ASTPtr & query_ast,
        const ThrottlerPtr & throttler, Context & context,
        BlockInputStreams & res) override;

private:
    ClusterPtr & cluster;

    Cluster::Addresses getShardReplicasAddresses(const Cluster::ShardInfo &info);
};

}

}
