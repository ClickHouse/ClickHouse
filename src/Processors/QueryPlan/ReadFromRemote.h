#pragma once

#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Core/QueryProcessingStage.h>
#include <Client/IConnections.h>
#include <Common/GetPriorityForLoadBalancing.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Core/UUID.h>

namespace DB
{
class IThrottler;
using ThrottlerPtr = std::shared_ptr<IThrottler>;

struct UnavailableShardTracker;
using UnavailableShardTrackerPtr = std::shared_ptr<UnavailableShardTracker>;

class ParallelReplicasReadingCoordinator;
using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

/// Whether `parallel_replicas_filter_pushdown` would really splice `pushed_down_filters` into the query
/// shipped to the remote replicas - every reason it can decline, asked ahead of the splice itself, and
/// every setting read from that query's own context rather than the ambient one:
///  - the setting itself must be on for the shipped query;
///  - the query must read a single table, or the predicate has no side to be attributed to;
///  - it must be one `PredicateRewriteVisitor` rewrites at all, so no `FINAL`, no `LIMIT`, and no window
///    function in the `SELECT` list;
///  - the predicate must be expressible against that query's projection.
///
/// Plan optimization asks it because a condition that ends up in the initiator's local plan without
/// reaching the replicas may not change how the local fragment reads. Pass a null `pushed_down_filters`
/// to ask only what the query itself decides, before there is a predicate to push.
///
/// An `IN` set's temporary table is not registered here, so a predicate needing one is answered no; that
/// is the safe direction, and such a predicate reaches the local plan by the other route anyway.
/// The context the shipped query carries its own SETTINGS in - a jointly scoped subquery gets one of
/// its own, and its values, not the ambient ones, govern what the replicas run.
ContextPtr getShippedQueryContext(const QueryTreeNodePtr & query_tree, const ContextPtr & fallback);

bool canAddFiltersToShippedQuery(
    const ASTPtr & query_ast,
    const QueryTreeNodePtr & query_tree,
    const PlannerContextPtr & planner_context,
    ContextMutablePtr context,
    const ActionsDAG * pushed_down_filters);

/// Reading step from remote servers.
/// Unite query results from several shards.
class ReadFromRemote final : public SourceStepWithFilterBase
{
public:
    /// @param main_table_ if Shards contains main_table then this parameter will be ignored
    ReadFromRemote(
        ClusterProxy::SelectStreamFactory::Shards shards_,
        SharedHeader header_,
        QueryProcessingStage::Enum stage_,
        StorageID main_table_,
        ASTPtr table_func_ptr_,
        ContextMutablePtr context_,
        ThrottlerPtr throttler_,
        Scalars scalars_,
        Tables external_tables_,
        LoggerPtr log_,
        UInt32 shard_count_,
        std::shared_ptr<const StorageLimitsList> storage_limits_,
        const String & cluster_name_,
        UnavailableShardTrackerPtr unavailable_shard_tracker_ = nullptr);

    String getName() const override { return "ReadFromRemote"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeDistributedPlan(FormatSettings & settings, const ExplainPlanOptions & options) override;
    void describeDistributedPipeline(FormatSettings & settings, bool distributed) override;

    void enableMemoryBoundMerging();
    void enforceAggregationInOrder(const SortDescription & sort_description);

    bool hasSerializedPlan() const;

private:
    ClusterProxy::SelectStreamFactory::Shards shards;
    QueryProcessingStage::Enum stage;
    StorageID main_table;
    ASTPtr table_func_ptr;
    ContextMutablePtr context;
    ThrottlerPtr throttler;
    Scalars scalars;
    Tables external_tables;
    std::shared_ptr<const StorageLimitsList> storage_limits;
    LoggerPtr log;
    UInt32 shard_count;
    const String cluster_name;
    UnavailableShardTrackerPtr unavailable_shard_tracker;
    std::optional<GetPriorityForLoadBalancing> priority_func_factory;

    Pipes addPipes(const ClusterProxy::SelectStreamFactory::Shards & used_shards, const SharedHeader & out_header);

    void addLazyPipe(
        Pipes & pipes,
        const ClusterProxy::SelectStreamFactory::Shard & shard,
        const SharedHeader & out_header,
        size_t parallel_marshalling_threads);

    void addPipe(
        Pipes & pipes,
        const ClusterProxy::SelectStreamFactory::Shard & shard,
        const SharedHeader & out_header,
        size_t parallel_marshalling_threads);
};


class ReadFromParallelRemoteReplicasStep : public SourceStepWithFilterBase
{
public:
    ReadFromParallelRemoteReplicasStep(
        ASTPtr query_ast_,
        const QueryTreeNodePtr & query_tree_,
        const PlannerContextPtr & planner_context,
        ClusterPtr cluster_,
        const StorageID & storage_id_,
        ParallelReplicasReadingCoordinatorPtr coordinator_,
        SharedHeader header_,
        QueryProcessingStage::Enum stage_,
        ContextMutablePtr context_,
        ThrottlerPtr throttler_,
        Scalars scalars_,
        Tables external_tables_,
        LoggerPtr log_,
        std::shared_ptr<const StorageLimitsList> storage_limits_,
        std::vector<ConnectionPoolPtr> pools_to_use,
        std::optional<size_t> exclude_pool_index_ = std::nullopt,
        ConnectionPoolWithFailoverPtr connection_pool_with_failover_ = nullptr,
        std::shared_ptr<const QueryPlan> query_plan_ = nullptr);

    String getName() const override { return "ReadFromRemoteParallelReplicas"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeDistributedPlan(FormatSettings & settings, const ExplainPlanOptions & options) override;
    void describeDistributedPipeline(FormatSettings & settings, bool distributed) override;

    void enableMemoryBoundMerging();
    void enforceAggregationInOrder(const SortDescription & sort_description);

    StorageID getStorageID() const { return storage_id; }
    ParallelReplicasReadingCoordinatorPtr getCoordinator() const { return coordinator; }

    /// The connection pools (sized to the coordinator's replica count) and the local replica's index
    /// within them. Captured before this step is dropped from the local INSERT SELECT plan so the
    /// remote-pool pass can reuse the exact same replica set the coordinator was created with.
    const std::vector<ConnectionPoolPtr> & getPools() const { return pools_to_use; }
    std::optional<size_t> getExcludePoolIndex() const { return exclude_pool_index; }

private:
    Pipes addPipes(ASTPtr ast, const SharedHeader & out_header);

    Pipe createPipeForSingeReplica(const ConnectionPoolPtr & pool, ASTPtr ast, IConnections::ReplicaInfo replica_info, const SharedHeader & out_header,
                                   size_t parallel_marshalling_threads);

    ClusterPtr cluster;
    ASTPtr query_ast;
    QueryTreeNodePtr query_tree;
    PlannerContextPtr planner_context;
    StorageID storage_id;
    ParallelReplicasReadingCoordinatorPtr coordinator;
    QueryProcessingStage::Enum stage;
    ContextMutablePtr context;
    ThrottlerPtr throttler;
    Scalars scalars;
    Tables external_tables;
    std::shared_ptr<const StorageLimitsList> storage_limits;
    LoggerPtr log;
    std::vector<ConnectionPoolPtr> pools_to_use;
    std::optional<size_t> exclude_pool_index;
    ConnectionPoolWithFailoverPtr connection_pool_with_failover;
    std::shared_ptr<const QueryPlan> query_plan;
};

ASTPtr tryBuildAdditionalFilterAST(
    const ActionsDAG & dag,
    const std::unordered_set<std::string> & projection_names,
    const std::unordered_map<std::string, QueryTreeNodePtr> & execution_name_to_projection_query_tree,
    Tables * external_tables,
    ContextMutablePtr & context);

}
