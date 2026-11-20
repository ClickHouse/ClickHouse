#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Client/ConnectionPool.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/UnavailableShardTracker.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageSnapshot.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{


struct Settings;
class Cluster;
struct SelectQueryInfo;

class Pipe;
using Pipes = std::vector<Pipe>;

class QueryPlan;
using QueryPlanPtr = std::unique_ptr<QueryPlan>;
using QueryPlanPtrs = VectorWithMemoryTracking<QueryPlanPtr>;

struct StorageID;

class PreparedSets;
using PreparedSetsPtr = std::shared_ptr<PreparedSets>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

namespace ClusterProxy
{

/// select query has database, table and table function names as AST pointers
/// Creates a copy of query, changes database, table and table function names.
using ColumnsDescriptionByShardNum = std::unordered_map<UInt32, ColumnsDescription>;
using AdditionalShardFilterGenerator = std::function<ASTPtr(uint64_t)>;

class SelectStreamFactory
{
public:

    struct Shard
    {
        /// Query and header may be changed depending on shard.
        ASTPtr query;

        /// These can be shared between shards.
        QueryTreeNodePtr query_tree;
        PlannerContextPtr planner_context;

        std::shared_ptr<QueryPlan> query_plan;

        /// Used to check the table existence on remote node
        StorageID main_table;
        SharedHeader header;

        Cluster::ShardInfo shard_info;

        /// If we connect to replicas lazily.
        /// (When there is a local replica with big delay).
        bool lazy = false;
        AdditionalShardFilterGenerator shard_filter_generator{};
    };

    using Shards = std::vector<Shard>;

    SelectStreamFactory(
        SharedHeader header_,
        const StorageSnapshotPtr & storage_snapshot_,
        QueryProcessingStage::Enum processed_stage_);

    void createForShard(
        const Cluster::ShardInfo & shard_info,
        const QueryTreeNodePtr & query_tree,
        const StorageID & main_table,
        const ASTPtr & table_func_ptr,
        ContextPtr context,
        QueryPlanPtrs & local_plans,
        Shards & remote_shards,
        UInt32 shard_count,
        bool parallel_replicas_enabled,
        AdditionalShardFilterGenerator shard_filter_generator,
        const UnavailableShardTrackerPtr & unavailable_shard_tracker);

    SharedHeader header;
    const StorageSnapshotPtr storage_snapshot;
    QueryProcessingStage::Enum processed_stage;

    /// Whether the inner table function comes from a definition read back from the metadata rather than
    /// from a fresh query. A stored `ENGINE = Remote('...', f(...))` sets it, and so does a `remote()` or
    /// `cluster()` call that replays one, so a combination the function refuses while a definition is
    /// written still loads; a fresh `remote()`/`cluster()` call leaves it false, so the same checks run
    /// there as for the function written directly. `ENGINE = Distributed` cannot carry a function target:
    /// its third argument is evaluated to a literal.
    bool is_replayed_definition = false;

private:
    /// The query of the previous shard, which is usually the query of the next one too.
    struct Query
    {
        QueryTreeNodePtr tree;
        ASTPtr ast;

        /// Calculated when a remote shard needs them.
        SharedHeader header;
        PlannerContextPtr planner_context;
    };

    Query query;
};

}

}
