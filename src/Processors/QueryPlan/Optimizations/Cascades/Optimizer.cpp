#include <Common/ThreadStatus.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Optimizer.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Processors/QueryPlan/Optimizations/Cascades/CascadesParams.h>
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

CascadesOptimizer::CascadesOptimizer(QueryPlan & query_plan_, const QueryPlanOptimizationSettings & optimization_settings_)
    : query_plan(query_plan_)
    , optimization_settings(optimization_settings_)
{}

/// Default task budget for one optimization; see the comment at the search loop.
static constexpr size_t DEFAULT_TASK_LIMIT = 100000;

/// Collects everything the search runs under: the cluster size (fail-closed when unknown), the
/// cost model, and the query settings the rules honor.
static OptimizationEnvironment buildEnvironment(const ContextPtr & query_context, const QueryPlanOptimizationSettings & optimization_settings)
{
    OptimizationEnvironment environment;

    /// Seed the sort settings from the query so any sort added by SortingEnforcer keeps the query's
    /// size limits and spill thresholds instead of arbitrary defaults.
    environment.sort_settings = SortingStep::Settings(query_context->getSettingsRef());

    /// Parameter takes priority (for testing or to limit parallelism); otherwise use the same worker
    /// source as the distributed executor.
    environment.cluster_node_count = getCascadesClusterNodeCountParam(query_context);
    if (environment.cluster_node_count == 0)
        environment.cluster_node_count = getCascadesPlanningNodeCount(query_context);
    /// Reject rather than silently plan for one node, which would skip every distributed alternative.
    if (environment.cluster_node_count == 0)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan with enable_cascades_optimizer cannot determine how many nodes will "
            "run the query. Configure a stateless worker cluster, or set `distributed_plan_workers_num` "
            "(also required for `distributed_plan_execute_locally` without a configured cluster).");

    /// If the cost-config override is set but invalid, let the error propagate instead of silently
    /// using the defaults, so a query that set it does not get a different cost model than it asked for.
    if (query_context->getQueryParameters().contains(CascadesParams::COST_CONFIG))
        environment.cost_config = parseCostConfig(query_context->getQueryParameters().at(CascadesParams::COST_CONFIG));

    environment.distributed_plan_execute_locally = optimization_settings.distributed_plan_execute_locally;
    environment.distributed_aggregation_memory_efficient = optimization_settings.distributed_aggregation_memory_efficient;
    environment.distributed_plan_force_shuffle_aggregation = optimization_settings.distributed_plan_force_shuffle_aggregation;
    environment.exact_rows_before_limit = optimization_settings.exact_rows_before_limit;

    return environment;
}

void CascadesOptimizer::optimize()
{
    Stopwatch optimizer_timer;
    auto query_context = CurrentThread::get().tryGetQueryContext();
    if (!query_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No query context available");

    /// Statistics come from a query-parameter hint when present, otherwise they are empty.
    /// Deriving them from real table statistics is not wired up yet.
    OptimizerStatisticsPtr statistics;
    if (query_context->getQueryParameters().contains(CascadesParams::STAT_HINTS))
        statistics = createStatisticsFromHint(query_context->getQueryParameters().at(CascadesParams::STAT_HINTS));
    else
        statistics = createEmptyStatistics();

    OptimizationEnvironment environment = buildEnvironment(query_context, optimization_settings);
    LOG_TRACE(getLogger("CascadesOptimizer"), "Cost config: {}, cluster node count: {}",
        environment.cost_config.dump(), environment.cluster_node_count);

    OptimizerContext optimizer_context(*statistics, std::move(environment));
    LOG_TEST(optimizer_context.log, "Initial query plan:\n{}", dumpQueryPlanShort(query_plan));

    auto [root_group_id, root_required_properties] = optimizer_context.addGroup(*query_plan.getRootNode());

    LOG_TEST(optimizer_context.log, "Initial memo:\n{}", optimizer_context.memo.dump());

    /// Add task to optimize root group
    optimizer_context.pushTask(std::make_shared<OptimizeGroupTask>(root_group_id, root_required_properties));

    /// Limit the time in terms of optimization tasks instead of wall clock time. This is done for stability of generated plans regardless of system load.
    /// Guys from MS SQL Server describe this in Andy Pavlo's seminar: https://www.youtube.com/watch?v=pQe1LQJiXN0
    const size_t executed_tasks_limit = getCascadesTaskLimitParam(query_context, DEFAULT_TASK_LIMIT);
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
            "(root group #{}, required properties {}, {} groups in memo, {} tasks left, next: {}). "
            "The distributed Cascades optimizer is experimental; set enable_cascades_optimizer = 0 "
            "or simplify the query.",
            executed_tasks_limit, root_group_id, root_required_properties.dump(),
            optimizer_context.memo.getGroupCount(), optimizer_context.tasks.size(),
            optimizer_context.tasks.top()->describe());

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
/// Join and replicated-subplan strategies share the logical step, so the description is
/// formatted here. Other strategies (aggregation, read) set descriptions during rule
/// application.
static QueryPlanStepPtr cloneStepForBestPlan(const GroupExpression & expression)
{
    auto step = expression.getQueryPlanStep()->clone();
    if (dynamic_cast<const IJoinStrategy *>(expression.strategy.get()) != nullptr
        || dynamic_cast<const ReplicatedSubplanStrategy *>(expression.strategy.get()) != nullptr)
    {
        const auto & suffix = expression.description_suffix;
        const auto & original = expression.getQueryPlanStep()->getStepDescription();
        if (suffix.empty())
            step->setStepDescription(fmt::format("{} {}", expression.strategy->getName(), original), 200);
        else
            step->setStepDescription(fmt::format("{} {} {}", expression.strategy->getName(), suffix, original), 200);
    }
    return step;
}

QueryPlanPtr CascadesOptimizer::buildBestPlan(GroupId subtree_root_group_id, ExpressionProperties required_properties, const Memo & memo)
{
    const auto & cost_config = memo.getEnvironment().cost_config;

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

    /// DFS stack frame - one per expression (leaf, single-input, or multi-input).
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

    /// No implementation at the root means no distributable plan exists for this query
    /// (e.g. an operator that only runs on one node under a multi-node requirement).
    /// Deeper selection failures stay logical errors: a recorded best implementation
    /// guarantees its inputs are satisfiable.
    {
        auto root_group = memo.getGroup(subtree_root_group_id);
        auto root_best = root_group->selectInputImplementation(
            required_properties, cost_config, active_path, /*input_is_self_referential=*/false).expression;
        if (!root_best)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "make_distributed_plan cannot distribute this query: no plan satisfies {} at the root. "
                "The distributed Cascades optimizer is experimental; disable enable_cascades_optimizer "
                "or simplify the query.",
                required_properties.dump());
        pushFrame(subtree_root_group_id, std::move(root_best));
    }

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

        /// All children processed - build this node's plan (post-order).
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
