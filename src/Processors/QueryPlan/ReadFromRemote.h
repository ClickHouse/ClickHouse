#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
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
class ReadFromRemote final : public ISourceStep
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

    void enableMemoryBoundMerging();
    void enforceAggregationInOrder();

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

    void addLazyPipe(Pipes & pipes, const ClusterProxy::SelectStreamFactory::Shard & shard);
    void addPipe(Pipes & pipes, const ClusterProxy::SelectStreamFactory::Shard & shard);
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
        std::optional<size_t> exclude_pool_index_ = std::nullopt);

    String getName() const override { return "ReadFromRemoteParallelReplicas"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void enableMemoryBoundMerging();
    void enforceAggregationInOrder();

private:
    void addPipeForSingeReplica(Pipes & pipes, const ConnectionPoolPtr & pool, IConnections::ReplicaInfo replica_info);

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
};

}
