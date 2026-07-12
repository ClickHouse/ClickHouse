#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/StreamLocalLimits.h>

#include <optional>

namespace DB
{

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

class LimitStep;

class ReadFromRemotePlanStep : public ISourceStep
{
public:
    struct DistributedTopKCoordination
    {
        UInt64 limit;
        SortDescription sort_description;
    };

    ReadFromRemotePlanStep(
        QueryPlanPtr inner_plan_,
        ClusterPtr cluster_,
        String cluster_name_,
        ContextMutablePtr remote_context_,
        StorageID main_table_,
        ASTPtr table_func_ptr_,
        ASTPtr query_for_logging_,
        std::shared_ptr<const StorageLimitsList> storage_limits_,
        LoggerPtr log_);

    String getName() const override { return "ReadFromRemotePlan"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    bool isSerializable() const override { return false; }

    void absorbStep(QueryPlanStepPtr step);

    void absorbLimitCopy(const LimitStep & limit_step);

    bool isLimitCopied() const { return limit_copied; }

    void setDistributedTopKCoordination(UInt64 limit, SortDescription sort_description);
    const std::optional<DistributedTopKCoordination> & getDistributedTopKCoordination() const
    {
        return distributed_top_k_coordination;
    }

    QueryPlanRawPtrs getChildPlans() override;

    QueryPlanPtr extractInnerPlan();

    const ClusterPtr & getCluster() const { return cluster; }
    const String & getClusterName() const { return cluster_name; }
    ContextMutablePtr getRemoteContext() const { return remote_context; }
    const StorageID & getMainTable() const { return main_table; }
    const ASTPtr & getTableFunctionPtr() const { return table_func_ptr; }
    const ASTPtr & getQueryForLogging() const { return query_for_logging; }
    std::shared_ptr<const StorageLimitsList> getStorageLimits() const { return storage_limits; }
    LoggerPtr getLog() const { return log; }

private:
    QueryPlanPtr inner_plan;

    ClusterPtr cluster;
    String cluster_name;
    ContextMutablePtr remote_context;

    StorageID main_table;
    ASTPtr table_func_ptr;
    ASTPtr query_for_logging;

    std::shared_ptr<const StorageLimitsList> storage_limits;
    LoggerPtr log;

    bool limit_copied = false;
    std::optional<DistributedTopKCoordination> distributed_top_k_coordination;
};

}
