#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>

#include <Common/checkStackSize.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Processors/QueryPlan/ExpressionStep.h>

namespace DB
{

namespace
{

void addConvertingActions(QueryPlan & plan, const Block & header, bool has_missing_objects)
{
    if (blocksHaveEqualStructure(plan.getCurrentDataStream().header, header))
        return;

    auto mode = has_missing_objects ? ActionsDAG::MatchColumnsMode::Position : ActionsDAG::MatchColumnsMode::Name;

    auto get_converting_dag = [mode](const Block & block_, const Block & header_)
    {
        /// Convert header structure to expected.
        /// Also we ignore constants from result and replace it with constants from header.
        /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
        return ActionsDAG::makeConvertingActions(
            block_.getColumnsWithTypeAndName(),
            header_.getColumnsWithTypeAndName(),
            mode,
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
    bool has_missing_objects)
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

    if (context->getSettingsRef().allow_experimental_analyzer)
    {
        /// For Analyzer, identifier in GROUP BY/ORDER BY/LIMIT BY lists has been resolved to
        /// ConstantNode in QueryTree if it is an alias of a constant, so we should not replace
        /// ConstantNode with ProjectionNode again(https://github.com/ClickHouse/ClickHouse/issues/62289).
        new_context->setSetting("enable_positional_arguments", Field(false));
        auto interpreter = InterpreterSelectQueryAnalyzer(query_ast, new_context, select_query_options);
        query_plan = std::make_unique<QueryPlan>(std::move(interpreter).extractQueryPlan());
    }
    else
    {
        auto interpreter = InterpreterSelectQuery(query_ast, new_context, select_query_options);
        interpreter.buildQueryPlan(*query_plan);
    }

    addConvertingActions(*query_plan, header, has_missing_objects);
    return query_plan;
}

}
