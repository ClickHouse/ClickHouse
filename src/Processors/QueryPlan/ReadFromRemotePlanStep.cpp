#include <Processors/QueryPlan/ReadFromRemotePlanStep.h>

#include <Core/Settings.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/LimitStep.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsUInt64 query_plan_max_step_description_length;
}

ReadFromRemotePlanStep::ReadFromRemotePlanStep(
    QueryPlanPtr inner_plan_,
    ClusterPtr cluster_,
    String cluster_name_,
    ContextMutablePtr remote_context_,
    StorageID main_table_,
    ASTPtr table_func_ptr_,
    ASTPtr query_for_logging_,
    std::shared_ptr<const StorageLimitsList> storage_limits_,
    LoggerPtr log_)
    : ISourceStep(inner_plan_->getCurrentHeader())
    , inner_plan(std::move(inner_plan_))
    , cluster(std::move(cluster_))
    , cluster_name(std::move(cluster_name_))
    , remote_context(std::move(remote_context_))
    , main_table(std::move(main_table_))
    , table_func_ptr(std::move(table_func_ptr_))
    , query_for_logging(std::move(query_for_logging_))
    , storage_limits(std::move(storage_limits_))
    , log(log_)
{
    setStepDescription(
        fmt::format("Cluster: {}, shards: {}", cluster_name, cluster->getShardCount()),
        remote_context->getSettingsRef()[Setting::query_plan_max_step_description_length]);
}

void ReadFromRemotePlanStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} shouldn't be called", __PRETTY_FUNCTION__);
}

void ReadFromRemotePlanStep::absorbStep(QueryPlanStepPtr step)
{
    inner_plan->addStep(std::move(step));
    output_header = inner_plan->getCurrentHeader();
}

void ReadFromRemotePlanStep::absorbLimitCopy(const LimitStep & limit_step)
{
    /// Per-shard `LIMIT n` is not a global `LIMIT n`, so the outer step stays and the inner plan
    /// gets `LIMIT limit + offset` with zero offset (the same semantics as `distributed_push_down_limit`).
    /// `getLimitForSorting` computes `limit + offset` with overflow protection; the caller must have
    /// already checked that it is non-zero (see `tryPushDownToRemotePlan`).
    const size_t shard_limit = limit_step.getLimitForSorting();
    chassert(shard_limit != 0);
    inner_plan->addStep(std::make_unique<LimitStep>(
        inner_plan->getCurrentHeader(), shard_limit, /*offset_=*/0));
    /// `LimitStep` does not change the header, no need to update `output_header`.
    limit_copied = true;
}

QueryPlanRawPtrs ReadFromRemotePlanStep::getChildPlans()
{
    if (!inner_plan)
        return {};

    return {inner_plan.get()};
}

QueryPlanPtr ReadFromRemotePlanStep::extractInnerPlan()
{
    chassert(inner_plan);

    auto plan = std::move(inner_plan);
    inner_plan.reset();
    return plan;
}

}
