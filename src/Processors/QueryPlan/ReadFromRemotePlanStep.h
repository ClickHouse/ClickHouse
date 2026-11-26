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

    [[nodiscard]] String getName() const override { return "ReadFromRemotePlan"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    [[nodiscard]] bool isSerializable() const override { return false; }

    void absorbStep(QueryPlanStepPtr step);

    void absorbLimitCopy(const LimitStep & limit_step);

    [[nodiscard]] bool isLimitCopied() const { return limit_copied; }

    void setDistributedTopKCoordination(UInt64 limit, SortDescription sort_description);
    [[nodiscard]] const std::optional<DistributedTopKCoordination> & getDistributedTopKCoordination() const
    {
        return distributed_top_k_coordination;
    }

    QueryPlanRawPtrs getChildPlans(bool for_explain) override;

    QueryPlanPtr extractInnerPlan();

    [[nodiscard]] const ClusterPtr & getCluster() const { return cluster; }
    [[nodiscard]] const String & getClusterName() const { return cluster_name; }
    [[nodiscard]] ContextMutablePtr getRemoteContext() const { return remote_context; }
    [[nodiscard]] const StorageID & getMainTable() const { return main_table; }
    [[nodiscard]] const ASTPtr & getTableFunctionPtr() const { return table_func_ptr; }
    [[nodiscard]] const ASTPtr & getQueryForLogging() const { return query_for_logging; }
    [[nodiscard]] std::shared_ptr<const StorageLimitsList> getStorageLimits() const { return storage_limits; }
    [[nodiscard]] LoggerPtr getLog() const { return log; }

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
