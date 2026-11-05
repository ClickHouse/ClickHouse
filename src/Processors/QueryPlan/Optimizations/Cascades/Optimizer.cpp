#include <Common/ThreadStatus.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Optimizer.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Processors/QueryPlan/Optimizations/Cascades/CascadesParams.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerDefaults.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <IO/WriteBufferFromString.h>
#include <fmt/format.h>
#include <exception>
#include <memory>
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

static String dumpQueryPlanShort(const QueryPlan & query_plan)
{
    WriteBufferFromOwnString out;
    query_plan.explainPlan(out, {.estimates = true});
    return out.str();
}

static ContextPtr getQueryContextOrThrow()
{
    auto query_context = CurrentThread::get().tryGetQueryContext();
    if (!query_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No query context available");
    return query_context;
}

/// Statistics come from a query-parameter hint when present, otherwise they are empty.
/// Reading them from real table statistics is not implemented yet.
static OptimizerStatisticsPtr createOptimizerStatistics(const ContextPtr & query_context)
{
    if (query_context->getQueryParameters().contains(CascadesParams::STAT_HINTS))
        return createStatisticsFromHint(query_context->getQueryParameters().at(CascadesParams::STAT_HINTS));
    return createEmptyStatistics();
}

/// Collects what the search needs: the cluster size (the query is rejected when it is
/// unknown), the cost model, and the query settings the rules honor.
static OptimizerContext buildContext(const ContextPtr & query_context, const QueryPlanOptimizationSettings & optimization_settings)
{
    OptimizerContext context;

    /// Seed the sort settings from the query so any sort added by `SortingEnforcer` keeps the query's
    /// size limits and spill thresholds instead of arbitrary defaults.
    context.sort_settings = SortingStep::Settings(query_context->getSettingsRef());

    /// Parameter takes priority (for testing or to limit parallelism); otherwise use the same worker
    /// source as the distributed executor.
    context.cluster_node_count = getCascadesClusterNodeCountParam(query_context);
    if (context.cluster_node_count == 0)
        context.cluster_node_count = getCascadesPlanningNodeCount(query_context);
    /// Reject rather than silently plan for one node, which would skip every distributed alternative.
    if (context.cluster_node_count == 0)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan with enable_cascades_optimizer cannot determine how many nodes will "
            "run the query. Configure a stateless worker cluster, or set `distributed_plan_workers_num` "
            "(also required for `distributed_plan_execute_locally` without a configured cluster).");
    /// The count sizes the read buckets and the exchange fan-out, so the read-bucket ceiling bounds it
    /// too: above it a plan would scatter to more destinations than a bucketed read may have.
    if (context.cluster_node_count > ReadFromMergeTree::max_distributed_read_buckets)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "make_distributed_plan with enable_cascades_optimizer cannot plan for {} nodes, the maximum "
            "is {}.", context.cluster_node_count, ReadFromMergeTree::max_distributed_read_buckets);

    /// If the cost-config override is set but invalid, let the error propagate instead of silently
    /// using the defaults, so a query that set it does not get a different cost model than it asked for.
    if (query_context->getQueryParameters().contains(CascadesParams::COST_CONFIG))
        context.cost_config = parseCostConfig(query_context->getQueryParameters().at(CascadesParams::COST_CONFIG));

    context.distributed_plan_execute_locally = optimization_settings.distributed_plan_execute_locally;
    context.distributed_aggregation_memory_efficient = optimization_settings.distributed_aggregation_memory_efficient;
    context.distributed_plan_force_shuffle_aggregation = optimization_settings.distributed_plan_force_shuffle_aggregation;
    context.cascades_aggregation_pushdown = optimization_settings.cascades_aggregation_pushdown;
    context.exact_rows_before_limit = optimization_settings.exact_rows_before_limit;

    return context;
}

CascadesOptimizer::CascadesOptimizer(QueryPlan & query_plan_, const QueryPlanOptimizationSettings & optimization_settings_)
    : query_plan(query_plan_)
    , optimization_settings(optimization_settings_)
    , statistics(createOptimizerStatistics(getQueryContextOrThrow()))
    , cost_estimator(memo)
    , statistics_derivation(memo, *statistics)
{
    memo.setContext(buildContext(getQueryContextOrThrow(), optimization_settings));

    addRule(createJoinCommutativity());
    addRule(createHashJoinImplementation());
    addRule(createDefaultImplementation());
    addRule(createDistributionPassthrough());
    addRule(createTwoStageAggregationTransformation());
    /// Registered conditionally: the rule can never apply when the setting is off,
    /// and the optimizer is built per query, so the gate belongs here.
    /// `distributed_plan_force_shuffle_aggregation` does not disable the whole rule: it forbids
    /// only the partial + merge split (variant A), which the rule skips itself.
    if (memo.getContext().cascades_aggregation_pushdown)
        addRule(createAggregationPushdown());
    addRule(createAggregationImplementation());
    addRule(createLocalReadImplementation());
    addRule(createParallelReadImplementation());
    addRule(createReplicatedReadImplementation());
    addRule(createReplicatedSubplanImplementation());
    addRule(createTopNImplementation());
    addRule(createTwoStageTopN());
    addEnforcerRule(createDistributionEnforcer());
    addEnforcerRule(createSortingEnforcer());
}

void CascadesOptimizer::addRule(OptimizationRulePtr rule)
{
    if (rule->isTransformation())
        transformation_rules.push_back(std::move(rule));
    else
        implementation_rules.push_back(std::move(rule));
}

void CascadesOptimizer::addEnforcerRule(OptimizationRulePtr rule)
{
    enforcer_rules.push_back(std::move(rule));
}

std::pair<GroupId, ExpressionProperties> CascadesOptimizer::addGroup(QueryPlan::Node & node)
{
    /// `CommonSubplanReferenceStep` must be resolved before the Cascades optimizer runs.
    /// TODO: it could instead be resolved here by mapping the target node to its group.
    const auto * subplan_reference = typeid_cast<const CommonSubplanReferenceStep *>(node.step.get());
    if (subplan_reference)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected CommonSubplanReferenceStep, it should be already resolved");

    /// Strip a limit-less SortingStep::Full - pure sorting is a physical property, not a logical
    /// one.  Return the child's GroupId along with the sorting as required properties, so the
    /// caller can attach them to the input link of the parent group expression.
    /// A SortingStep with a limit is a top-N (row-reducing) operator, so it is kept as an
    /// operator instead; the limit is owned by that operator, never by the sorting property.
    const auto * sorting_step = typeid_cast<const SortingStep *>(node.step.get());
    if (sorting_step && sorting_step->getType() == SortingStep::Type::Full && sorting_step->getLimit() == 0)
    {
        chassert(node.children.size() == 1);
        auto [child_group_id, _] = addGroup(*node.children.front());
        ExpressionProperties stripped_props;
        stripped_props.sorting = sorting_step->getSortDescription();
        return {child_group_id, stripped_props};
    }

    std::optional<ExpressionStatistics> prepopulated_statistics = estimateStatistics(node);

    auto group_expression = std::make_shared<GroupExpression>(std::move(node.step));
    auto group_id = memo.addGroup(group_expression);
    for (auto * child_node : node.children)
    {
        auto [input_group_id, pending_props] = addGroup(*child_node);
        group_expression->inputs.push_back({input_group_id, pending_props});
    }

    /// Set statistics on the group (shared by all expressions in the group)
    auto group = memo.getGroup(group_id);
    group->statistics = std::move(prepopulated_statistics);

    return {group_id, {}};
}

void CascadesOptimizer::pushTask(OptimizationTaskPtr task)
{
    tasks.push(std::move(task));
}

GroupPtr CascadesOptimizer::getGroup(GroupId group_id)
{
    return memo.getGroup(group_id);
}

void CascadesOptimizer::updateBestPlan(GroupExpressionPtr expression)
{
    /// No pruning: an expression that loses to the current best still keeps its cost, so the
    /// enforcer-input selection can consider it as an acyclic fallback.
    costAndUpdateBest(expression, /*prune_against_best=*/false);
}

bool CascadesOptimizer::costAndUpdateBest(GroupExpressionPtr expression, bool prune_against_best)
{
    const auto & cost_config = memo.getContext().cost_config;
    auto group = memo.getGroup(expression->group_id);
    auto cost = cost_estimator.estimateCost(expression);

    /// Leave the expression uncosted: plan extraction must never walk into a subtree
    /// that has no implementation for one of its inputs.
    if (!cost.buildable)
    {
        LOG_TEST(log, "Expression '{}' in group #{} has an unsatisfiable input, not a best-plan candidate",
            expression->getDescription(), expression->group_id);
        return false;
    }

    if (prune_against_best)
    {
        Float64 subtree_weighted = cost.subtree_cost.total(cost_config);
        Float64 current_best = group->getBestCostForProperties(expression->properties, cost_config);
        if (std::isfinite(current_best) && subtree_weighted >= current_best)
        {
            LOG_TEST(log, "Pruned expression '{}' in group #{}: "
                "cost {} >= current best {}",
                expression->getDescription(), expression->group_id,
                subtree_weighted, current_best);
            return false;
        }
    }

    expression->cost = cost;
    LOG_TEST(log, "group #{} expression '{}' cost {}",
        expression->group_id, expression->getDescription(), cost.subtree_cost.total(cost_config));
    group->updateBestImplementation(expression, cost_config);
    return true;
}

void CascadesOptimizer::deriveStatistics(GroupId group_id)
{
    statistics_derivation.deriveStatistics(group_id);
}

bool CascadesOptimizer::tryUpdateBestPlanDirectly(GroupExpressionPtr expression)
{
    const auto & cost_config = memo.getContext().cost_config;

    /// Check if all inputs are fully optimized (all stages complete) and have
    /// a satisfying implementation.  A group can be fully done with no best if
    /// no rule could produce the required distribution (e.g. `ReadFromSystemOne`
    /// at {N nodes}) - treat as pruned.
    for (const auto & input : expression->inputs)
    {
        auto child_group = getGroup(input.group_id);
        if (!child_group->isFullyDoneFor(input.required_properties))
            return false;
        if (!child_group->getBestImplementation(input.required_properties, cost_config).expression)
        {
            LOG_TEST(log, "Skipping unsatisfiable expression '{}' in group #{}: "
                "input group #{} has no implementation for {}",
                expression->getDescription(), expression->group_id,
                input.group_id, input.required_properties.dump());
            return true;  /// Unsatisfiable input - treat as pruned
        }
    }

    /// All inputs ready - compute cost directly, bypassing the `OptimizeInputsTask` chain.
    deriveStatistics(expression->group_id);
    costAndUpdateBest(expression, /*prune_against_best=*/true);
    return true;
}

void CascadesOptimizer::scheduleCosting(GroupExpressionPtr expression)
{
    if (!tryUpdateBestPlanDirectly(expression))
        pushTask(std::make_unique<OptimizeInputsTask>(expression, 0));
}

void CascadesOptimizer::optimize()
{
    if (optimize_was_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CascadesOptimizer::optimize called twice; the memo and the task stack belong to one run, construct a new optimizer instead");
    optimize_was_called = true;

    Stopwatch optimizer_timer;
    auto query_context = getQueryContextOrThrow();

    LOG_TRACE(log, "Cost config: {}, cluster node count: {}",
        memo.getContext().cost_config.dump(), memo.getContext().cluster_node_count);

    LOG_TEST(log, "Initial query plan:\n{}", dumpQueryPlanShort(query_plan));

    auto [root_group_id, root_required_properties] = addGroup(*query_plan.getRootNode());

    LOG_TEST(log, "Initial memo:\n{}", memo.dump());

    pushTask(std::make_unique<OptimizeGroupTask>(root_group_id, root_required_properties));

    /// Limit the time in terms of optimization tasks instead of wall clock time. This is done for stability of generated plans regardless of system load.
    /// Microsoft SQL Server's optimizer team describes this in Andy Pavlo's seminar: https://www.youtube.com/watch?v=pQe1LQJiXN0
    const size_t executed_tasks_limit = getCascadesTaskLimitParam(query_context, CascadesDefaults::DEFAULT_TASK_LIMIT);
    size_t executed_tasks_count = 0;
    for (; !tasks.empty() && executed_tasks_count < executed_tasks_limit; ++executed_tasks_count)
    {
        auto task = std::move(tasks.top());
        tasks.pop();
        task->execute(*this);
    }

    LOG_TEST(log, "Executed {} tasks, Memo after:\n{}", executed_tasks_count, memo.dump());

    /// Fail closed if the search did not finish within the task budget: building a plan from a
    /// partial memo can yield a non-minimal plan or a confusing failure deep inside buildBestPlan.
    /// Surface a clear error instead and point at the knob to raise the limit.
    if (!tasks.empty())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Cascades optimizer did not finish within the task budget of {} tasks "
            "(root group #{}, required properties {}, {} groups in memo, {} tasks left, next: {}). "
            "The distributed Cascades optimizer is experimental; set enable_cascades_optimizer = 0 "
            "or simplify the query.",
            executed_tasks_limit, root_group_id, root_required_properties.dump(),
            memo.getGroupCount(), tasks.size(), tasks.top()->describe());

    auto best_plan = buildBestPlan(root_group_id, root_required_properties);

    LOG_TEST(log, "Optimized plan:\n{}", dumpQueryPlanShort(*best_plan));

    /// Update the original plan in-place because there might be references to the root node of the original plan
    query_plan.replaceNodeWithPlan(query_plan.getRootNode(), std::move(*best_plan));

    LOG_TRACE(log, "Optimization took {} ms", optimizer_timer.elapsedMilliseconds());
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

QueryPlanPtr CascadesOptimizer::buildBestPlan(GroupId subtree_root_group_id, ExpressionProperties required_properties)
{
    const auto & cost_config = memo.getContext().cost_config;

    /// Single-input expressions on the current DFS path, used to break enforcer self-reference
    /// cycles. Path-local: added when a frame is pushed, removed when popped, so the same expression
    /// can still be reused in an independent sibling branch.
    std::unordered_set<GroupExpression *> active_path;

    /// Select the cheapest eligible (acyclic) implementation for a group. `input_is_self_referential`
    /// is true when this selection is for the self-referential input of a same-group enforcer.
    auto select_best = [&](GroupId group_id, const ExpressionProperties & props, bool input_is_self_referential) -> GroupExpressionPtr
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
    auto push_frame = [&](GroupId group_id, GroupExpressionPtr expression)
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
        push_frame(subtree_root_group_id, std::move(root_best));
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
            auto child = select_best(input.group_id, input.required_properties, input_is_self_referential);
            push_frame(input.group_id, std::move(child));
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
