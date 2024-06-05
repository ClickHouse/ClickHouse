#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Client/ConnectionPool.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageSnapshot.h>

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

class PreparedSets;
using PreparedSetsPtr = std::shared_ptr<PreparedSets>;
namespace ClusterProxy
{

/// select query has database, table and table function names as AST pointers
/// Creates a copy of query, changes database, table and table function names.
ASTPtr rewriteSelectQuery(
    ContextPtr context,
    const ASTPtr & query,
    const std::string & remote_database,
    const std::string & remote_table,
    ASTPtr table_function_ptr = nullptr);

using ColumnsDescriptionByShardNum = std::unordered_map<UInt32, ColumnsDescription>;
using AdditionalShardFilterGenerator = std::function<ASTPtr(uint64_t)>;

class SelectStreamFactory
{
public:

    struct Shard
    {
        /// Query and header may be changed depending on shard.
        ASTPtr query;
        QueryTreeNodePtr query_tree;

        /// Used to check the table existence on remote node
        StorageID main_table;
        Block header;

        bool has_missing_objects = false;

        Cluster::ShardInfo shard_info;

        /// If we connect to replicas lazily.
        /// (When there is a local replica with big delay).
        bool lazy = false;
        time_t local_delay = 0;
        AdditionalShardFilterGenerator shard_filter_generator{};
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
        UInt32 shard_count,
        bool parallel_replicas_enabled,
        AdditionalShardFilterGenerator shard_filter_generator);

    void createForShard(
        const Cluster::ShardInfo & shard_info,
        const QueryTreeNodePtr & query_tree,
        const StorageID & main_table,
        const ASTPtr & table_func_ptr,
        ContextPtr context,
        std::vector<QueryPlanPtr> & local_plans,
        Shards & remote_shards,
        UInt32 shard_count,
        bool parallel_replicas_enabled,
        AdditionalShardFilterGenerator shard_filter_generator);

    const Block header;
    const ColumnsDescriptionByShardNum objects_by_shard;
    const StorageSnapshotPtr storage_snapshot;
    QueryProcessingStage::Enum processed_stage;

private:
    void createForShardImpl(
        const Cluster::ShardInfo & shard_info,
        const ASTPtr & query_ast,
        const QueryTreeNodePtr & query_tree,
        const StorageID & main_table,
        const ASTPtr & table_func_ptr,
        ContextPtr context,
        std::vector<QueryPlanPtr> & local_plans,
        Shards & remote_shards,
        UInt32 shard_count,
        bool parallel_replicas_enabled,
        AdditionalShardFilterGenerator shard_filter_generator,
        bool has_missing_objects = false);
};

}

}
