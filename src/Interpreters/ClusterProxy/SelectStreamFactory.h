#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageSnapshot.h>
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


using ColumnsDescriptionByShardNum = std::unordered_map<UInt32, ColumnsDescription>;

class SelectStreamFactory
{
public:

    struct Shard
    {
        /// Query and header may be changed depending on shard.
        ASTPtr query;
        Block header;

        Cluster::ShardInfo shard_info;

        /// If we connect to replicas lazily.
        /// (When there is a local replica with big delay).
        bool lazy = false;
        UInt32 local_delay = 0;
    };

    using Shards = std::vector<Shard>;

    SelectStreamFactory(
        const Block & header_,
        const ColumnsDescriptionByShardNum & objects_by_shard_,
        const StorageSnapshotPtr & storage_snapshot_,
        QueryProcessingStage::Enum processed_stage_);

    void createForShard(
        const Cluster::ShardInfo & shard_info,
        const ASTPtr & query_ast,
        const StorageID & main_table,
        const ASTPtr & table_func_ptr,
        ContextPtr context,
        std::vector<QueryPlanPtr> & local_plans,
        Shards & remote_shards,
        UInt32 shard_count);

    struct ShardPlans
    {
        /// If a shard has local replicas this won't be nullptr
        std::unique_ptr<QueryPlan> local_plan;

        /// Contains several steps to read from all remote replicas
        std::unique_ptr<QueryPlan> remote_plan;
    };

    ShardPlans createForShardWithParallelReplicas(
        const Cluster::ShardInfo & shard_info,
        const ASTPtr & query_ast,
        const StorageID & main_table,
        const ASTPtr & table_function_ptr,
        const ThrottlerPtr & throttler,
        ContextPtr context,
        UInt32 shard_count,
        const std::shared_ptr<const StorageLimitsList> & storage_limits
    );

private:
    const Block header;
    const ColumnsDescriptionByShardNum objects_by_shard;
    const StorageSnapshotPtr storage_snapshot;
    QueryProcessingStage::Enum processed_stage;
};

}

}
