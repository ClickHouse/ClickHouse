#include <Processors/QueryPlan/Optimizations/Cascades/Optimizer.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromString.h>
#include <fmt/format.h>
#include <exception>
#include <memory>
#include <utility>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static String dumpQueryPlanShort(const QueryPlan & query_plan)
{
    WriteBufferFromOwnString out;
    query_plan.explainPlan(out, {.estimates = true});
    return out.str();
}

CascadesOptimizer::CascadesOptimizer(QueryPlan & query_plan_)
    : query_plan(query_plan_)
{}

void CascadesOptimizer::optimize()
{
    Stopwatch optimizer_timer;
    OptimizerStatisticsPtr statistics;
    /// FIXME: statistics stub for testing
    auto query_context = CurrentThread::get().getQueryContext();
    constexpr auto stats_hint_param_name = "_internal_join_table_stat_hints";
    if (query_context->getQueryParameters().contains(stats_hint_param_name))
        statistics = createStatisticsFromHint(query_context->getQueryParameters().at(stats_hint_param_name));
    else
        statistics = createEmptyStatistics();

    /// Extract cluster node count from query parameters (for testing) or fall back to a default.
    /// TODO: derive from actual cluster topology (e.g. Context::getCluster).
    size_t cluster_node_count = 4;
    constexpr auto cluster_node_count_param_name = "_internal_cascades_cluster_node_count";
    if (query_context->getQueryParameters().contains(cluster_node_count_param_name))
        cluster_node_count = std::stoull(query_context->getQueryParameters().at(cluster_node_count_param_name));

    CostConfig cost_config;
    constexpr auto cost_config_param_name = "_internal_cascades_cost_config";
    if (query_context->getQueryParameters().contains(cost_config_param_name))
    {
        try
        {
            cost_config = parseCostConfig(query_context->getQueryParameters().at(cost_config_param_name));
        }
        catch (const std::exception & e)
        {
            LOG_WARNING(&Poco::Logger::get("CascadesOptimizer"), "Failed to parse cost config: {} from parameter '{}'",
                e.what(), query_context->getQueryParameters().at(cost_config_param_name));
        }
    }

    OptimizerContext optimizer_context(*statistics, cluster_node_count, cost_config);

    LOG_TRACE(optimizer_context.log, "Cost config: {}", cost_config.dump());
    LOG_TEST(optimizer_context.log, "Initial query plan:\n{}", dumpQueryPlanShort(query_plan));

    auto [root_group_id, root_required_properties] = optimizer_context.addGroup(*query_plan.getRootNode());

    LOG_TEST(optimizer_context.log, "Initial memo:\n{}", optimizer_context.memo.dump());

    /// Add task to optimize root group
    CostLimit initial_cost_limit = std::numeric_limits<CostLimit>::infinity();
    optimizer_context.pushTask(std::make_shared<OptimizeGroupTask>(root_group_id, root_required_properties, initial_cost_limit));

    /// Limit the time in terms of optimization tasks instead of wall clock time. This is done for stability of generated plans regardless of system load.
    /// Guys from MS SQL Server describe this in Andy Pavlo's seminar: https://www.youtube.com/watch?v=pQe1LQJiXN0
    const size_t executed_tasks_limit = 100000;
    size_t executed_tasks_count = 0;
    for (; !optimizer_context.tasks.empty() && executed_tasks_count < executed_tasks_limit; ++executed_tasks_count)
    {
        auto task = optimizer_context.tasks.top();
        optimizer_context.tasks.pop();
        task->execute(optimizer_context);
    }

    LOG_TEST(optimizer_context.log, "Executed {} tasks, Memo after:\n{}", executed_tasks_count, optimizer_context.memo.dump());

    /// Get the best plan for the root group
    auto best_plan = buildBestPlan(root_group_id, root_required_properties, optimizer_context.memo);

    LOG_TEST(optimizer_context.log, "Optimized plan:\n{}", dumpQueryPlanShort(*best_plan));

    /// Update the original plan in-place because there might be references to the root node of the original plan
    query_plan.replaceNodeWithPlan(query_plan.getRootNode(), std::move(*best_plan));

    LOG_TRACE(optimizer_context.log, "Optimization took {} ms", optimizer_timer.elapsedMilliseconds());
}

/// Drop unused columns and reorder columns between steps if needed
void addConvertingExpression(QueryPlan & plan, const SharedHeader & expected_header)
{
    if (!blocksHaveEqualStructure(*plan.getCurrentHeader(), *expected_header))
    {
        auto actions_dag = ActionsDAG::makeConvertingActions(
                plan.getCurrentHeader()->getColumnsWithTypeAndName(),
                expected_header->getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name,
                nullptr);
        auto converting_step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(actions_dag));
        converting_step->setStepDescription("Convert column list");
        plan.addStep(std::move(converting_step));
    }
}

QueryPlanPtr CascadesOptimizer::buildBestPlan(GroupId subtree_root_group_id, ExpressionProperties required_properties, const Memo & memo)
{
    auto group = memo.getGroup(subtree_root_group_id);
    const auto & cost_config = memo.getCostConfig();
    auto group_best_expression = group->getBestImplementation(required_properties, cost_config).expression;
    if (!group_best_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cascades optimizer: no implementation found for group #{} satisfying required properties {}.\n"
            "Group state:\n{}",
            subtree_root_group_id, required_properties.dump(), group->dump());

    QueryPlanPtr plan_for_group;
    if (group_best_expression->inputs.empty())
    {
        auto leaf_plan = std::make_unique<QueryPlan>();
        leaf_plan->addStep(group_best_expression->getQueryPlanStep()->clone());
        plan_for_group = std::move(leaf_plan);
    }
    else if (group_best_expression->inputs.size() == 1)
    {
        const auto & input = group_best_expression->inputs.front();
        auto child_plan = buildBestPlan(input.group_id, input.required_properties, memo);
        auto step = group_best_expression->getQueryPlanStep()->clone();
        addConvertingExpression(*child_plan, step->getInputHeaders().at(0));
        child_plan->addStep(std::move(step));
        plan_for_group = std::move(child_plan);
    }
    else
    {
        std::vector<QueryPlanPtr> input_plans;
        input_plans.reserve(group_best_expression->inputs.size());
        auto step = group_best_expression->getQueryPlanStep()->clone();
        for (size_t i = 0; i < group_best_expression->inputs.size(); ++i)
        {
            const auto & input = group_best_expression->inputs[i];
            auto input_plan = buildBestPlan(input.group_id, input.required_properties, memo);
            addConvertingExpression(*input_plan, step->getInputHeaders().at(i));
            input_plans.push_back(std::move(input_plan));
        }
        auto united_plan = std::make_unique<QueryPlan>();
        united_plan->unitePlans(std::move(step), std::move(input_plans));
        plan_for_group = std::move(united_plan);
    }

    plan_for_group->getRootNode()->cost_estimation = CostEstimationInfo
        {
            .cost = group_best_expression->cost->subtree_cost.weighted_total(cost_config),
            .rows = group->statistics->estimated_row_count
        };

    LOG_TEST(getLogger("buildBestPlan"), "Plan for group #{}:\n{}", subtree_root_group_id, dumpQueryPlanShort(*plan_for_group));

    return plan_for_group;
}

}
