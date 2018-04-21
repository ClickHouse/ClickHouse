#pragma once

#include <Interpreters/ClusterProxy/IStreamFactory.h>

namespace DB
{

namespace ClusterProxy
{

class TruncateStreamFactory final : public IStreamFactory
{
public:

    TruncateStreamFactory(ClusterPtr & cluster, String & storage_path);

    void createForShard(
        const Cluster::ShardInfo & shard_info,
        const String & query, const ASTPtr & query_ast,
        const ThrottlerPtr & throttler, Context & context,
        BlockInputStreams & res) override;

    void removeTemporaryDir(const Cluster::ShardInfo &shard_info) const;

private:
    ClusterPtr cluster;
    String & storage_path;

    Cluster::Addresses getShardReplicasAddresses(const Cluster::ShardInfo &info);
};

}

}
