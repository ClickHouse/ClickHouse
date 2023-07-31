#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>
#include <Common/checkStackSize.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/InterpreterSelectQuery.h>

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
    UInt32 shard_num,
    UInt32 shard_count,
    size_t replica_num,
    size_t replica_count,
    std::shared_ptr<ParallelReplicasReadingCoordinator> coordinator)
{
    checkStackSize();

    auto query_plan = std::make_unique<QueryPlan>();
    /// Do not apply AST optimizations, because query
    /// is already optimized and some optimizations
    /// can be applied only for non-distributed tables
    /// and we can produce query, inconsistent with remote plans.
    auto interpreter = InterpreterSelectQuery(
        query_ast, context,
        SelectQueryOptions(processed_stage)
            .setShardInfo(shard_num, shard_count)
            .ignoreASTOptimizations());

    interpreter.setProperClientInfo(replica_num, replica_count);
    if (coordinator)
    {
        interpreter.setMergeTreeReadTaskCallbackAndClientInfo([coordinator](PartitionReadRequest request) -> std::optional<PartitionReadResponse>
        {
            return coordinator->handleRequest(request);
        });
    }

    interpreter.buildQueryPlan(*query_plan);
    addConvertingActions(*query_plan, header);
    return query_plan;
}

}
