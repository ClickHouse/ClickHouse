#include <Common/ThreadStatus.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Optimizer.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
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
    extern const int SUPPORT_IS_DISABLED;
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
    auto query_context = CurrentThread::get().tryGetQueryContext();
    if (!query_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No query context available");
    constexpr auto stats_hint_param_name = "_internal_join_table_stat_hints";
    if (query_context->getQueryParameters().contains(stats_hint_param_name))
        statistics = createStatisticsFromHint(query_context->getQueryParameters().at(stats_hint_param_name));
    else
        statistics = createEmptyStatistics();

    /// Parameter takes priority (for testing or to limit parallelism).
    /// Otherwise use the actual cluster size from config.
    size_t cluster_node_count = getCascadesClusterNodeCountParam(query_context);
    if (cluster_node_count == 0)
        cluster_node_count = std::max<size_t>(1, getDistributedWorkerCount(query_context));

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

    LOG_TRACE(optimizer_context.log, "Cost config: {}, cluster node count: {}", cost_config.dump(), cluster_node_count);
    LOG_TEST(optimizer_context.log, "Initial query plan:\n{}", dumpQueryPlanShort(query_plan));

    auto [root_group_id, root_required_properties] = optimizer_context.addGroup(*query_plan.getRootNode());

    LOG_TEST(optimizer_context.log, "Initial memo:\n{}", optimizer_context.memo.dump());

    /// Add task to optimize root group
    CostLimit initial_cost_limit = std::numeric_limits<CostLimit>::infinity();
    optimizer_context.pushTask(std::make_shared<OptimizeGroupTask>(root_group_id, root_required_properties, initial_cost_limit));

    /// Limit the time in terms of optimization tasks instead of wall clock time. This is done for stability of generated plans regardless of system load.
    /// Guys from MS SQL Server describe this in Andy Pavlo's seminar: https://www.youtube.com/watch?v=pQe1LQJiXN0
    const size_t executed_tasks_limit = getCascadesTaskLimitParam(query_context, 100000);
    size_t executed_tasks_count = 0;
    for (; !optimizer_context.tasks.empty() && executed_tasks_count < executed_tasks_limit; ++executed_tasks_count)
    {
        auto task = optimizer_context.tasks.top();
        optimizer_context.tasks.pop();
        task->execute(optimizer_context);
    }

    LOG_TEST(optimizer_context.log, "Executed {} tasks, Memo after:\n{}", executed_tasks_count, optimizer_context.memo.dump());

    /// Fail closed if the search did not finish within the task budget: building a plan from a
    /// partial memo can yield a non-minimal plan or a confusing failure deep inside buildBestPlan.
    /// Surface a clear error instead and point at the knob to raise the limit.
    if (!optimizer_context.tasks.empty())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Cascades optimizer did not finish within the task budget of {} tasks "
            "(root group #{}, required properties {}, {} groups in memo). "
            "The distributed Cascades optimizer is experimental; disable enable_cascades_optimizer "
            "or simplify the query.",
            executed_tasks_limit, root_group_id, root_required_properties.dump(),
            optimizer_context.memo.getGroupCount());

    /// Get the best plan for the root group
    auto best_plan = buildBestPlan(root_group_id, root_required_properties, optimizer_context.memo);

    LOG_TEST(optimizer_context.log, "Optimized plan:\n{}", dumpQueryPlanShort(*best_plan));

    /// Update the original plan in-place because there might be references to the root node of the original plan
    query_plan.replaceNodeWithPlan(query_plan.getRootNode(), std::move(*best_plan));

    LOG_TRACE(optimizer_context.log, "Optimization took {} ms", optimizer_timer.elapsedMilliseconds());
}

/// Drop unused columns and reorder columns between steps if needed
static void addConvertingExpression(QueryPlan & plan, const SharedHeader & expected_header)
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

/// Clone the shared immutable plan_step and apply the strategy-based description.
/// Join strategies share the logical step, so the description is formatted here.
/// Other strategies (aggregation, read) set descriptions during rule application.
static QueryPlanStepPtr cloneStepForBestPlan(const GroupExpression & expression)
{
    auto step = expression.getQueryPlanStep()->clone();
    if (const auto * join_strategy = dynamic_cast<const IJoinStrategy *>(expression.strategy.get()))
    {
        const auto & suffix = expression.description_suffix;
        const auto & original = expression.getQueryPlanStep()->getStepDescription();
        if (suffix.empty())
            step->setStepDescription(fmt::format("{} {}", join_strategy->getName(), original), 200);
        else
            step->setStepDescription(fmt::format("{} {} {}", join_strategy->getName(), suffix, original), 200);
    }
    return step;
}

QueryPlanPtr CascadesOptimizer::buildBestPlan(GroupId subtree_root_group_id, ExpressionProperties required_properties, const Memo & memo)
{
    const auto & cost_config = memo.getCostConfig();

    /// Single-input expressions on the current DFS path, used to break enforcer self-reference
    /// cycles. Path-local: added when a frame is pushed, removed when popped, so the same expression
    /// can still be reused in an independent sibling branch.
    std::unordered_set<GroupExpression *> active_path;

    /// Select the cheapest eligible (acyclic) implementation for a group. `input_is_self_referential`
    /// is true when this selection is for the self-referential input of a same-group enforcer.
    auto selectBest = [&](GroupId group_id, const ExpressionProperties & props, bool input_is_self_referential) -> GroupExpressionPtr
    {
        auto group = memo.getGroup(group_id);
        auto best = group->selectInputImplementation(props, cost_config, active_path, input_is_self_referential).expression;
        if (!best)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cascades optimizer: no acyclic implementation found for group #{} satisfying required properties {}.\n"
                "Group state:\n{}",
                group_id, props.dump(), group->dump(cost_config));
        return best;
    };

    /// DFS stack frame — one per expression (leaf, single-input, or multi-input).
    struct Frame
    {
        GroupId group_id;
        GroupExpressionPtr expression;
        size_t next_child = 0;
        std::vector<QueryPlanPtr> child_plans;
        bool on_active_path = false;  /// whether this frame's expression was added to `active_path`
    };

    std::vector<Frame> stack;
    QueryPlanPtr result;

    /// Push a frame for `expression`, recording it on the active path if it is single-input.
    auto pushFrame = [&](GroupId group_id, GroupExpressionPtr expression)
    {
        const bool on_active_path = expression->inputs.size() == 1;
        if (on_active_path)
            active_path.insert(expression.get());
        stack.push_back({group_id, std::move(expression), 0, {}, on_active_path});
    };

    pushFrame(subtree_root_group_id, selectBest(subtree_root_group_id, required_properties, /*input_is_self_referential=*/false));

    while (!stack.empty())
    {
        auto & frame = stack.back();

        /// Traverse children first (pre-order push).
        if (frame.next_child < frame.expression->inputs.size())
        {
            const auto & input = frame.expression->inputs[frame.next_child];
            ++frame.next_child;
            const bool input_is_self_referential = input.group_id == frame.group_id;
            auto child = selectBest(input.group_id, input.required_properties, input_is_self_referential);
            pushFrame(input.group_id, std::move(child));
            continue;
        }

        /// All children processed — build this node's plan (post-order).
        if (frame.expression->inputs.empty())
        {
            result = std::make_unique<QueryPlan>();
            result->addStep(cloneStepForBestPlan(*frame.expression));
        }
        else if (frame.expression->inputs.size() == 1)
        {
            result = std::move(frame.child_plans[0]);
            auto step = cloneStepForBestPlan(*frame.expression);
            addConvertingExpression(*result, step->getInputHeaders().at(0));
            result->addStep(std::move(step));
        }
        else
        {
            auto step = cloneStepForBestPlan(*frame.expression);
            for (size_t i = 0; i < frame.child_plans.size(); ++i)
                addConvertingExpression(*frame.child_plans[i], step->getInputHeaders().at(i));
            result = std::make_unique<QueryPlan>();
            result->unitePlans(std::move(step), std::move(frame.child_plans));
        }

        result->getRootNode()->cost_estimation = CostEstimationInfo
            {
                .cost = frame.expression->cost->subtree_cost.total(cost_config),
                .rows = memo.getGroup(frame.group_id)->statistics->estimated_row_count
            };
        LOG_TEST(getLogger("buildBestPlan"), "Plan for group #{}:\n{}", frame.group_id, dumpQueryPlanShort(*result));

        if (frame.on_active_path)
            active_path.erase(frame.expression.get());

        stack.pop_back();

        if (!stack.empty())
            stack.back().child_plans.push_back(std::move(result));
    }

    return result;
}

}
