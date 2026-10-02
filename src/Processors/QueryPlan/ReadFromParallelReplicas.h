#pragma once

#include <Client/IConnections.h>
#include <Core/QueryProcessingStage.h>
#include <Core/UUID.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/StorageID.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/IStorage_fwd.h>
#include <Common/GetPriorityForLoadBalancing.h>

namespace DB
{
class IThrottler;
using ThrottlerPtr = std::shared_ptr<IThrottler>;

class ParallelReplicasReadingCoordinator;
using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

class ReadFromParallelReplicasStep : public ISourceStep
{
public:
    ReadFromParallelReplicasStep(
        std::shared_ptr<const QueryPlan> query_plan_,
        ClusterPtr cluster_,
        ParallelReplicasReadingCoordinatorPtr coordinator_,
        ContextPtr context_,
        std::vector<ConnectionPoolPtr> pools_to_use,
        std::optional<size_t> exclude_pool_index_ = std::nullopt,
        ConnectionPoolWithFailoverPtr connection_pool_with_failover_ = nullptr);

    String getName() const override { return "ReadFromParallelReplicas"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeDistributedPlan(FormatSettings & settings, const ExplainPlanOptions & options) override;
    void describeDistributedPipeline(FormatSettings & settings, bool distributed) override;

    ParallelReplicasReadingCoordinatorPtr getCoordinator() const { return coordinator; }

private:
    Pipes addPipes(const SharedHeader & out_header);

    Pipe createPipeForSingeReplica(
        const ConnectionPoolPtr & pool,
        IConnections::ReplicaInfo replica_info,
        const SharedHeader & out_header,
        size_t parallel_marshalling_threads);

    std::shared_ptr<const QueryPlan> query_plan;
    ClusterPtr cluster;
    ParallelReplicasReadingCoordinatorPtr coordinator;
    ContextPtr context;
    LoggerPtr log;
    std::vector<ConnectionPoolPtr> pools_to_use;
    std::optional<size_t> exclude_pool_index;
    ConnectionPoolWithFailoverPtr connection_pool_with_failover;
};

}
