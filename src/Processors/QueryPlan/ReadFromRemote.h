#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Core/QueryProcessingStage.h>
#include <Client/IConnections.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

namespace DB
{

class ConnectionPoolWithFailover;
using ConnectionPoolWithFailoverPtr = std::shared_ptr<ConnectionPoolWithFailover>;

class Throttler;
using ThrottlerPtr = std::shared_ptr<Throttler>;

/// Reading step from remote servers.
/// Unite query results from several shards.
class ReadFromRemote final : public ISourceStep
{
public:
    ReadFromRemote(
        ClusterProxy::SelectStreamFactory::Shards shards_,
        Block header_,
        QueryProcessingStage::Enum stage_,
        StorageID main_table_,
        ASTPtr table_func_ptr_,
        ContextPtr context_,
        ThrottlerPtr throttler_,
        Scalars scalars_,
        Tables external_tables_,
        Poco::Logger * log_,
        UInt32 shard_count_,
        std::shared_ptr<const StorageLimitsList> storage_limits_);

    String getName() const override { return "ReadFromRemote"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    enum class Mode
    {
        PerReplica,
        PerShard
    };

    ClusterProxy::SelectStreamFactory::Shards shards;
    QueryProcessingStage::Enum stage;

    StorageID main_table;
    ASTPtr table_func_ptr;

    ContextPtr context;

    ThrottlerPtr throttler;
    Scalars scalars;
    Tables external_tables;

    std::shared_ptr<const StorageLimitsList> storage_limits;

    Poco::Logger * log;

    UInt32 shard_count;
    void addLazyPipe(Pipes & pipes, const ClusterProxy::SelectStreamFactory::Shard & shard);
    void addPipe(Pipes & pipes, const ClusterProxy::SelectStreamFactory::Shard & shard);
};


class ReadFromParallelRemoteReplicasStep : public ISourceStep
{
public:
    ReadFromParallelRemoteReplicasStep(
        ParallelReplicasReadingCoordinatorPtr coordinator_,
        ClusterProxy::SelectStreamFactory::Shard shard,
        Block header_,
        QueryProcessingStage::Enum stage_,
        StorageID main_table_,
        ASTPtr table_func_ptr_,
        ContextPtr context_,
        ThrottlerPtr throttler_,
        Scalars scalars_,
        Tables external_tables_,
        Poco::Logger * log_,
        std::shared_ptr<const StorageLimitsList> storage_limits_);

    String getName() const override { return "ReadFromRemoteParallelReplicas"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:

    void addPipeForSingeReplica(Pipes & pipes, std::shared_ptr<ConnectionPoolWithFailover> pool, IConnections::ReplicaInfo replica_info);

    ParallelReplicasReadingCoordinatorPtr coordinator;
    ClusterProxy::SelectStreamFactory::Shard shard;
    QueryProcessingStage::Enum stage;

    StorageID main_table;
    ASTPtr table_func_ptr;

    ContextPtr context;

    ThrottlerPtr throttler;
    Scalars scalars;
    Tables external_tables;

    std::shared_ptr<const StorageLimitsList> storage_limits;

    Poco::Logger * log;
};

}
