#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>

#include "config_version.h"
#include <Common/checkStackSize.h>
#include <Core/ProtocolDefines.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Processors/QueryPlan/ExpressionStep.h>

namespace DB
{

namespace
{

void addConvertingActions(QueryPlan & plan, const Block & header)
{
    if (blocksHaveEqualStructure(plan.getCurrentDataStream().header, header))
        return;

    auto get_converting_dag = [](const Block & block_, const Block & header_)
    {
        /// Convert header structure to expected.
        /// Also we ignore constants from result and replace it with constants from header.
        /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
        return ActionsDAG::makeConvertingActions(
            block_.getColumnsWithTypeAndName(),
            header_.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            true);
    };

    auto convert_actions_dag = get_converting_dag(plan.getCurrentDataStream().header, header);
    auto converting = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), convert_actions_dag);
    plan.addStep(std::move(converting));
}

}

std::unique_ptr<QueryPlan> createLocalPlan(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t shard_num,
    size_t shard_count,
    size_t replica_num,
    size_t replica_count,
    std::shared_ptr<ParallelReplicasReadingCoordinator> coordinator,
    UUID group_uuid)
{
    checkStackSize();

    auto query_plan = std::make_unique<QueryPlan>();
    auto new_context = Context::createCopy(context);

    /// Do not push down limit to local plan, as it will break `rows_before_limit_at_least` counter.
    if (processed_stage == QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit)
        processed_stage = QueryProcessingStage::WithMergeableStateAfterAggregation;

    /// Do not apply AST optimizations, because query
    /// is already optimized and some optimizations
    /// can be applied only for non-distributed tables
    /// and we can produce query, inconsistent with remote plans.
    auto select_query_options = SelectQueryOptions(processed_stage)
        .setShardInfo(static_cast<UInt32>(shard_num), static_cast<UInt32>(shard_count))
        .ignoreASTOptimizations();

    /// There are much things that are needed for coordination
    /// during reading with parallel replicas
    if (coordinator)
    {
        new_context->parallel_reading_coordinator = coordinator;
        new_context->setClientInterface(ClientInfo::Interface::LOCAL);
        new_context->setQueryKind(ClientInfo::QueryKind::SECONDARY_QUERY);
        new_context->setReplicaInfo(true, replica_count, replica_num);
        new_context->setConnectionClientVersion(DBMS_VERSION_MAJOR, DBMS_VERSION_MINOR, DBMS_VERSION_PATCH, DBMS_TCP_PROTOCOL_VERSION);
        new_context->setParallelReplicasGroupUUID(group_uuid);
        new_context->setMergeTreeAllRangesCallback([coordinator](InitialAllRangesAnnouncement announcement)
        {
            coordinator->handleInitialAllRangesAnnouncement(announcement);
        });
        new_context->setMergeTreeReadTaskCallback([coordinator](ParallelReadRequest request) -> std::optional<ParallelReadResponse>
        {
            return coordinator->handleRequest(request);
        });
    }

    if (context->getSettingsRef().allow_experimental_analyzer)
    {
        auto interpreter = InterpreterSelectQueryAnalyzer(query_ast, new_context, select_query_options);
        query_plan = std::make_unique<QueryPlan>(std::move(interpreter).extractQueryPlan());
    }
    else
    {
        auto interpreter = InterpreterSelectQuery(query_ast, new_context, select_query_options);
        interpreter.buildQueryPlan(*query_plan);
    }

    addConvertingActions(*query_plan, header);
    return query_plan;
}

}
