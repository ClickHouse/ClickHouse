#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Core/QueryProcessingStage.h>
#include <Client/IConnections.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/ClusterProxy/IStreamFactory.h>
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
        ClusterProxy::IStreamFactory::Shards shards_,
        Block header_,
        QueryProcessingStage::Enum stage_,
        StorageID main_table_,
        ASTPtr table_func_ptr_,
        ContextPtr context_,
        ThrottlerPtr throttler_,
        Scalars scalars_,
        Tables external_tables_,
        Poco::Logger * log_,
        UInt32 shard_count_);

    String getName() const override { return "ReadFromRemote"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    enum class Mode
    {
        PerReplica,
        PerShard
    };

    ClusterProxy::IStreamFactory::Shards shards;
    QueryProcessingStage::Enum stage;

    StorageID main_table;
    ASTPtr table_func_ptr;

    ContextPtr context;

    ThrottlerPtr throttler;
    Scalars scalars;
    Tables external_tables;

    Poco::Logger * log;

    UInt32 shard_count;
    void addLazyPipe(Pipes & pipes, const ClusterProxy::IStreamFactory::Shard & shard,
        std::shared_ptr<ParallelReplicasReadingCoordinator> coordinator,
        std::shared_ptr<ConnectionPoolWithFailover> pool,
        std::optional<IConnections::ReplicaInfo> replica_info);
    void addPipe(Pipes & pipes, const ClusterProxy::IStreamFactory::Shard & shard,
        std::shared_ptr<ParallelReplicasReadingCoordinator> coordinator,
        std::shared_ptr<ConnectionPoolWithFailover> pool,
        std::optional<IConnections::ReplicaInfo> replica_info);

    void addPipeForReplica();
};

}
