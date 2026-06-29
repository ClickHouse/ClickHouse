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

class ParallelReplicasReadingCoordinator;
using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

class ReadFromParallelReplicasStep : public SourceStepWithFilterBase
{
public:
    ReadFromParallelReplicasStep(
        ClusterPtr cluster_,
        const StorageID & storage_id_,
        ParallelReplicasReadingCoordinatorPtr coordinator_,
        SharedHeader header_,
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

    String getName() const override { return "ReadFromParallelReplicas"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeDistributedPlan(FormatSettings & settings, const ExplainPlanOptions & options) override;
    void describeDistributedPipeline(FormatSettings & settings, bool distributed) override;

    StorageID getStorageID() const { return storage_id; }
    ParallelReplicasReadingCoordinatorPtr getCoordinator() const { return coordinator; }

private:
    Pipes addPipes(const SharedHeader & out_header);

    Pipe createPipeForSingeReplica(
        const ConnectionPoolPtr & pool,
        IConnections::ReplicaInfo replica_info,
        const SharedHeader & out_header,
        size_t parallel_marshalling_threads);

    ClusterPtr cluster;
    StorageID storage_id;
    ParallelReplicasReadingCoordinatorPtr coordinator;
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

}
