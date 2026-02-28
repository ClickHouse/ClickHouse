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
#include <unordered_set>
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
    const auto & cost_config = memo.getCostConfig();

    /// Track visited single-input expressions to detect cycles in the
    /// best-implementation chain. A cycle occurs when enforcers (e.g.
    /// ShuffleExchange) in the same group point back to the same group with
    /// relaxed properties that match the enforcer itself, because
    /// `isDistributionSatisfiedBy` treats empty required columns as "any
    /// distribution is fine". When a cycle is detected, we skip the cycling
    /// expression and pick the next-best alternative from the group.
    std::unordered_set<GroupExpression *> visited_expressions;

    /// Select the best expression for a group, with cycle detection for
    /// single-input enforcers.
    auto selectBest = [&](GroupId group_id, const ExpressionProperties & props) -> GroupExpressionPtr
    {
        auto group = memo.getGroup(group_id);
        auto best = group->getBestImplementation(props, cost_config).expression;
        if (!best)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cascades optimizer: no implementation found for group #{} satisfying required properties {}.\n"
                "Group state:\n{}",
                group_id, props.dump(), group->dump());

        while (best->inputs.size() == 1
               && visited_expressions.contains(best.get()))
        {
            GroupExpressionPtr alternative;
            for (const auto & candidate : group->best_implementations)
            {
                if (visited_expressions.contains(candidate.get()))
                    continue;
                if (!props.isSatisfiedBy(candidate->properties))
                    continue;
                if (!alternative
                    || alternative->cost->subtree_cost.weighted_total(cost_config)
                       > candidate->cost->subtree_cost.weighted_total(cost_config))
                    alternative = candidate;
            }
            if (!alternative)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Cascades optimizer: cycle detected in best-implementation chain at group #{} "
                    "with no acyclic alternative. Expression '{}', properties {}.\n"
                    "Group state:\n{}",
                    group_id, best->dump(), props.dump(), group->dump());
            best = alternative;
        }

        if (best->inputs.size() == 1)
            visited_expressions.insert(best.get());

        return best;
    };

    /// DFS stack frame — one per expression (leaf, single-input, or multi-input).
    struct Frame
    {
        GroupId group_id;
        GroupExpressionPtr expression;
        size_t next_child = 0;
        std::vector<QueryPlanPtr> child_plans;
    };

    std::vector<Frame> stack;
    QueryPlanPtr result;

    stack.push_back({subtree_root_group_id, selectBest(subtree_root_group_id, required_properties), 0, {}});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        /// Traverse children first (pre-order push).
        if (frame.next_child < frame.expression->inputs.size())
        {
            const auto & input = frame.expression->inputs[frame.next_child];
            ++frame.next_child;
            stack.push_back({input.group_id, selectBest(input.group_id, input.required_properties), 0, {}});
            continue;
        }

        /// All children processed — build this node's plan (post-order).
        if (frame.expression->inputs.empty())
        {
            result = std::make_unique<QueryPlan>();
            result->addStep(frame.expression->getQueryPlanStep()->clone());
        }
        else if (frame.expression->inputs.size() == 1)
        {
            result = std::move(frame.child_plans[0]);
            auto step = frame.expression->getQueryPlanStep()->clone();
            addConvertingExpression(*result, step->getInputHeaders().at(0));
            result->addStep(std::move(step));
        }
        else
        {
            auto step = frame.expression->getQueryPlanStep()->clone();
            for (size_t i = 0; i < frame.child_plans.size(); ++i)
                addConvertingExpression(*frame.child_plans[i], step->getInputHeaders().at(i));
            result = std::make_unique<QueryPlan>();
            result->unitePlans(std::move(step), std::move(frame.child_plans));
        }

        result->getRootNode()->cost_estimation = CostEstimationInfo
            {
                .cost = frame.expression->cost->subtree_cost.weighted_total(cost_config),
                .rows = memo.getGroup(frame.group_id)->statistics->estimated_row_count
            };
        LOG_TEST(getLogger("buildBestPlan"), "Plan for group #{}:\n{}", frame.group_id, dumpQueryPlanShort(*result));

        stack.pop_back();

        if (!stack.empty())
            stack.back().child_plans.push_back(std::move(result));
    }

    return result;
}

}
