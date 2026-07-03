#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/StreamLocalLimits.h>

namespace DB
{

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

class LimitStep;

/// Placeholder for a distributed read, created by `StorageDistributed::read` when `make_distributed_plan`
/// is enabled. Holds the cluster info and an inner *logical* per-shard plan whose leaf is a bare read
/// from the remote table (`ReadFromTableStep`). Query plan optimizations may push steps of the outer
/// plan down into the inner plan (`absorbStep` / `absorbLimitCopy`). At the end of the second
/// optimization pass the placeholder is replaced with a regular `ReadFromRemote` step whose shards
/// carry the inner plan (see `finalizeReadFromRemotePlan`).
class ReadFromRemotePlanStep : public ISourceStep
{
public:
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

    /// The placeholder is always replaced with `ReadFromRemote` during plan optimization,
    /// so this is never executed directly (safety net).
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    bool isSerializable() const override { return false; }

    /// Move a step of the outer plan into the inner (per-shard) plan.
    /// The caller is responsible for removing the step from the outer plan.
    void absorbStep(QueryPlanStepPtr step);

    /// Add a copy of an outer `LimitStep` to the inner plan: per-shard `LIMIT limit + offset`.
    /// The outer step must stay in place, because a per-shard LIMIT is not a global LIMIT.
    void absorbLimitCopy(const LimitStep & limit_step);

    /// Whether a `LimitStep` copy has already been pushed into the inner plan. Used by the
    /// pushdown rule to avoid copying the same outer limit twice.
    bool isLimitCopied() const { return limit_copied; }

    /// Expose the inner plan to EXPLAIN and debug dumps.
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

    /// The optimized cluster (after skipping unused shards).
    ClusterPtr cluster;
    /// The name of the non-optimized cluster.
    String cluster_name;
    /// Context with settings and client info updated for the distributed query.
    /// Must stay mutable: `ReadFromRemote::addPipe` mutates it.
    ContextMutablePtr remote_context;

    StorageID main_table;
    ASTPtr table_func_ptr;
    /// The original SELECT query. The shard never parses it, but its text lands
    /// in the shard's `system.query_log` and `system.processes`.
    ASTPtr query_for_logging;

    std::shared_ptr<const StorageLimitsList> storage_limits;
    LoggerPtr log;

    /// Set once `absorbLimitCopy` has pushed a per-shard `LimitStep` into the inner plan.
    bool limit_copied = false;
};

}
