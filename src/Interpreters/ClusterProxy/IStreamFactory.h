#pragma once

#include <Client/ConnectionPool.h>
#include <Interpreters/Cluster.h>
#include <Parsers/IAST.h>

namespace DB
{

struct Settings;
class Cluster;
class Throttler;
struct SelectQueryInfo;

class Pipe;
using Pipes = std::vector<Pipe>;

class QueryPlan;
using QueryPlanPtr = std::unique_ptr<QueryPlan>;

struct StorageID;

namespace ClusterProxy
{

/// Base class for the implementation of the details of distributed query
/// execution that are specific to the query type.
class IStreamFactory
{
public:
    virtual ~IStreamFactory() = default;

    struct Shard
    {
        /// Query and header may be changed depending on shard.
        ASTPtr query;
        Block header;

        size_t shard_num = 0;
        size_t num_replicas = 0;
        ConnectionPoolWithFailoverPtr pool;
        ConnectionPoolPtrs per_replica_pools;

        /// If we connect to replicas lazily.
        /// (When there is a local replica with big delay).
        bool lazy = false;
        UInt32 local_delay = 0;
    };

    using Shards = std::vector<Shard>;

    virtual void createForShard(
            const Cluster::ShardInfo & shard_info,
            const ASTPtr & query_ast,
            const StorageID & main_table,
            const ASTPtr & table_func_ptr,
            ContextPtr context,
            std::vector<QueryPlanPtr> & local_plans,
            Shards & remote_shards,
            UInt32 shard_count) = 0;
};

}

}
