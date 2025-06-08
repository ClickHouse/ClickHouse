#pragma once

#include <stack>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Core/QueryProcessingStage.h>
#include <Client/IConnections.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Core/UUID.h>

namespace DB
{
class Throttler;
using ThrottlerPtr = std::shared_ptr<Throttler>;

class ParallelReplicasReadingCoordinator;
using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

/// Reading step from remote servers.
/// Unite query results from several shards.
class ReadFromRemote final : public SourceStepWithFilterBase
{
public:
    /// @param main_table_ if Shards contains main_table then this parameter will be ignored
    ReadFromRemote(
        ClusterProxy::SelectStreamFactory::Shards shards_,
        Block header_,
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
        const String & cluster_name_);

    String getName() const override { return "ReadFromRemote"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeDistributedPlan(FormatSettings & settings, const ExplainPlanOptions & options) override;

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
    std::optional<GetPriorityForLoadBalancing> priority_func_factory;

    Pipes addPipes(const ClusterProxy::SelectStreamFactory::Shards & used_shards, const Header & out_header);
    void addLazyPipe(Pipes & pipes, const ClusterProxy::SelectStreamFactory::Shard & shard, const Header & out_header);
    void addPipe(Pipes & pipes, const ClusterProxy::SelectStreamFactory::Shard & shard, const Header & out_header);
};


class ReadFromParallelRemoteReplicasStep : public ISourceStep
{
public:
    ReadFromParallelRemoteReplicasStep(
        ASTPtr query_ast_,
        ClusterPtr cluster_,
        const StorageID & storage_id_,
        ParallelReplicasReadingCoordinatorPtr coordinator_,
        Block header_,
        QueryProcessingStage::Enum stage_,
        ContextMutablePtr context_,
        ThrottlerPtr throttler_,
        Scalars scalars_,
        Tables external_tables_,
        LoggerPtr log_,
        std::shared_ptr<const StorageLimitsList> storage_limits_,
        std::vector<ConnectionPoolPtr> pools_to_use,
        std::optional<size_t> exclude_pool_index_ = std::nullopt,
        ConnectionPoolWithFailoverPtr connection_pool_with_failover_ = nullptr);

    String getName() const override { return "ReadFromRemoteParallelReplicas"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeDistributedPlan(FormatSettings & settings, const ExplainPlanOptions & options) override;

    void enableMemoryBoundMerging();
    void enforceAggregationInOrder(const SortDescription & sort_description);

    StorageID getStorageID() const { return storage_id; }
    ParallelReplicasReadingCoordinatorPtr getCoordinator() const { return coordinator; }

private:
    Pipes addPipes(ASTPtr ast, const Header & out_header);
    Pipe createPipeForSingeReplica(const ConnectionPoolPtr & pool, ASTPtr ast, IConnections::ReplicaInfo replica_info, const Header & out_header);

    ClusterPtr cluster;
    ASTPtr query_ast;
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
};

}
