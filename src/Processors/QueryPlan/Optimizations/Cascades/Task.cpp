#include <cmath>
#include <limits>
#include <memory>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Common/logger_useful.h>


namespace DB
{

void OptimizeGroupTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TEST(optimizer_context.log, "OptimizeGroupTask group #{}, required properties {}",
        group_id, required_properties.dump());
    auto group = optimizer_context.getGroup(group_id);

    /// Skip this group only if it is already fully processed (explored + implemented +
    /// enforced) for these properties and has a satisfying plan — re-running would be a no-op.
    /// We deliberately do NOT prune just because a current best is within a finite cost budget:
    /// the budget is an upper bound, not a lower bound, so such pruning is unsound for
    /// optimality, and it can return before stage-3 enforcers add the distributed alternatives.
    {
        const auto & cost_config = optimizer_context.getMemo().getCostConfig();
        bool group_fully_processed = group->isExplored()
            && group->isOptimizedFor(required_properties)
            && group->isEnforcedFor(required_properties);
        if (group_fully_processed && group->getBestImplementation(required_properties, cost_config).expression)
        {
            group->setFullyDoneFor(required_properties);
            LOG_TEST(optimizer_context.log, "OptimizeGroupTask group #{}: already fully processed", group_id);
            return;
        }
    }

    if (!group->isExplored())
    {
        /// Explore the group and then re-run OptimizeGroup again
        optimizer_context.pushTask(std::make_shared<OptimizeGroupTask>(group_id, required_properties, cost_limit));
        optimizer_context.pushTask(std::make_shared<ExploreGroupTask>(group_id, cost_limit));
    }
    else if (!group->isOptimizedFor(required_properties))
    {
        optimizer_context.pushTask(std::make_shared<OptimizeGroupTask>(group_id, required_properties, cost_limit));

        for (auto & expression : group->logical_expressions)
            optimizer_context.pushTask(std::make_shared<OptimizeExpressionTask>(expression, required_properties, cost_limit));

        group->setOptimizedFor(required_properties);
    }
    else if (!group->isEnforcedFor(required_properties))
    {
        /// Stage 3: Apply enforcers to physical expressions that don't satisfy the
        /// required properties.  Enforcers produce self-referential expressions whose
        /// inputs point back to the same group with relaxed requirements.
        ///
        /// The gate uses `isEnforcedFor` instead of `!getBestImplementation` so that
        /// enforcers always run exactly once per (group, properties) pair.  This lets
        /// enforcer-created plans (e.g. GatherExchange on a distributed subtree) compete
        /// on cost with passthrough implementations that already satisfy the properties.
        ///
        /// A fixed-point loop handles enforcer composition within a single invocation:
        /// e.g. SortingEnforcer creates Sort({N nodes, sorted}), then DistributionEnforcer
        /// creates GatherExchange(sorted) from it — all in the same Stage 3 pass.

        group->setEnforcedFor(required_properties);

        /// Collect enforcer expressions first, then push tasks in the right order.
        std::vector<GroupExpressionPtr> enforcer_expressions;

        /// Fixed-point loop: iterate over newly-added physical expressions until no
        /// new enforcers are produced.  Each iteration may create expressions that
        /// enable further enforcers (e.g. Sort enables sorted GatherExchange).
        size_t enforced_up_to = 0;
        bool new_enforcers_created = true;
        while (new_enforcers_created)
        {
            new_enforcers_created = false;

            /// Copy the list because enforcers add new physical expressions to the group.
            auto physical_expressions = group->physical_expressions;
            for (size_t i = enforced_up_to; i < physical_expressions.size(); ++i)
            {
                auto & expression = physical_expressions[i];

                if (required_properties.isSatisfiedBy(expression->properties))
                {
                    optimizer_context.updateBestPlan(expression);
                    continue;
                }

                for (const auto & enforcer : optimizer_context.getEnforcerRules())
                {
                    if (!enforcer->checkPattern(expression, required_properties, optimizer_context.getMemo()))
                        continue;

                    /// No coarse pass-local dedup here: physically identical enforcer outputs are
                    /// dropped by Group::addPhysicalExpression (structural dedup), while sources
                    /// that differ in sort direction or distribution keep their own enforced
                    /// alternative (e.g. a sorted gather for each requested direction).
                    /// Enforcers return only the expressions they actually inserted (structural
                    /// duplicates are dropped), so duplicate enforcer outputs are neither scheduled
                    /// nor counted as progress — they cannot exhaust the task budget.
                    auto new_expressions = enforcer->apply(expression, required_properties, optimizer_context.getMemo());
                    if (new_expressions.empty())
                        continue;

                    for (const auto & new_expression : new_expressions)
                    {
                        LOG_TEST(optimizer_context.log, "Enforcer '{}' on group #{} expression '{}' -> '{}'",
                            enforcer->getName(), group_id, expression->getDescription(), new_expression->getDescription());
                    }
                    enforcer_expressions.insert(enforcer_expressions.end(), new_expressions.begin(), new_expressions.end());
                    new_enforcers_created = true;
                }
            }
            enforced_up_to = physical_expressions.size();
        }

        if (!enforcer_expressions.empty())
        {
            /// Push self-task FIRST so it sits at the bottom of the stack (LIFO) and
            /// executes AFTER all OptimizeInputsTask complete.  This re-run checks
            /// whether the newly created enforcer expressions need further composition.
            optimizer_context.pushTask(
                std::make_shared<OptimizeGroupTask>(group_id, required_properties, cost_limit));

            for (const auto & new_expression : enforcer_expressions)
            {
                /// Fast path: if all inputs already have best implementations,
                /// compute cost directly — avoids the entire OptimizeInputsTask chain.
                if (!optimizer_context.tryUpdateBestPlanDirectly(new_expression))
                    optimizer_context.pushTask(
                        std::make_shared<OptimizeInputsTask>(new_expression, 0, cost_limit));
            }
        }
    }
    else
    {
        /// All stages complete: explored, implementation rules applied, enforcers tried,
        /// and a best implementation exists. Mark the group as fully done.
        group->setFullyDoneFor(required_properties);
    }
}


void ExploreGroupTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TEST(optimizer_context.log, "ExploreGroupTask group_id: {}", group_id);
    auto group = optimizer_context.getGroup(group_id);
    group->setExplored();

    for (const auto & expression : group->logical_expressions)
        optimizer_context.pushTask(std::make_shared<ExploreExpressionTask>(expression, cost_limit));
}


void ExploreExpressionTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TEST(optimizer_context.log, "ExploreExpressionTask group_id: {}, expression: {}",
        expression->group_id, expression->getName());

    std::vector<std::pair<Promise, OptimizationRulePtr>> moves;
    for (const auto & rule : optimizer_context.getTransformationRules())
    {
        if (!expression->isApplied(*rule, {}) && rule->checkPattern(expression, {}, optimizer_context.getMemo()))
            moves.push_back({rule->getPromise(), rule});
    }

    /// Sort moves by promise in ascending order
    std::sort(moves.begin(), moves.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    for (const auto & m : moves)
        optimizer_context.pushTask(std::make_shared<ApplyRuleTask>(expression, ExpressionProperties{}, m.second, m.first, cost_limit));

    for (const auto & input : expression->inputs)
    {
        if (!optimizer_context.getGroup(input.group_id)->isExplored())
            optimizer_context.pushTask(std::make_shared<ExploreGroupTask>(input.group_id, cost_limit));
    }
}


void OptimizeExpressionTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TEST(optimizer_context.log, "OptimizeExpressionTask group #{}, expression: {}, required properties {}",
        expression->group_id, expression->getName(), required_properties.dump());

    /// TODO: is this the same as ExploreExpressionTask::execute but just with a different set of rules?

    std::vector<std::pair<Promise, OptimizationRulePtr>> moves;
    for (const auto & rule : optimizer_context.getImplementationRules())
    {
        if (!expression->isApplied(*rule, required_properties) && rule->checkPattern(expression, required_properties, optimizer_context.getMemo()))
            moves.push_back({rule->getPromise(), rule});
    }

    /// Sort moves by promise in ascending order
    std::sort(moves.begin(), moves.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    for (const auto & m : moves)
        optimizer_context.pushTask(std::make_shared<ApplyRuleTask>(expression, required_properties, m.second, m.first, cost_limit));

    for (const auto & input : expression->inputs)
    {
        if (!optimizer_context.getGroup(input.group_id)->isExplored())
            optimizer_context.pushTask(std::make_shared<ExploreGroupTask>(input.group_id, cost_limit));
    }
}


void ApplyRuleTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TEST(optimizer_context.log, "ApplyRuleTask rule: '{}', group #{} expression: {}, required properties {}",
        rule->getName(), expression->group_id, expression->getName(), required_properties.dump());

    /// Ensure statistics are derived before applying rules (rules may need them for decisions)
    optimizer_context.deriveStatistics(expression->group_id);

    auto new_expressions = rule->apply(expression, required_properties, optimizer_context.getMemo());

    for (const auto & new_expression : new_expressions)
    {
        if (rule->isTransformation())
        {
            optimizer_context.pushTask(std::make_shared<ExploreExpressionTask>(new_expression, cost_limit));
        }
        else
        {
            /// Fast path: if all inputs already have best implementations,
            /// compute cost directly — avoids the entire OptimizeInputsTask chain.
            if (optimizer_context.tryUpdateBestPlanDirectly(new_expression))
                continue;

            /// Finite branch-and-bound budgets are disabled for now (see OptimizeInputsTask);
            /// pass the incoming limit through unchanged.
            optimizer_context.pushTask(std::make_shared<OptimizeInputsTask>(new_expression, 0, cost_limit));
        }
    }
}

void OptimizeInputsTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TEST(optimizer_context.log, "OptimizeInputsTask group #{} expression {}",
        expression->group_id, expression->dump(optimizer_context.getMemo().getCostConfig()));

    /// All inputs were processed?
    if (input_index_to_optimize == expression->inputs.size())
    {
        const auto & cost_config = optimizer_context.getMemo().getCostConfig();

        /// If any input has no satisfying implementation, this expression is
        /// unsatisfiable — skip cost estimation.
        for (const auto & input : expression->inputs)
        {
            if (!optimizer_context.getGroup(input.group_id)
                     ->getBestImplementation(input.required_properties, cost_config).expression)
            {
                LOG_TEST(optimizer_context.log, "Skipping unsatisfiable expression '{}' in group #{}: "
                    "input group #{} has no implementation for {}",
                    expression->getDescription(), expression->group_id,
                    input.group_id, input.required_properties.dump());
                return;
            }
        }

        /// Ensure statistics are derived before cost estimation
        optimizer_context.deriveStatistics(expression->group_id);

        /// Compute the cost and check if this expression beats the current best
        /// before storing it (branch-and-bound pruning).
        auto group = optimizer_context.getGroup(expression->group_id);
        auto cost = optimizer_context.getCostEstimator().estimateCost(expression);
        Float64 subtree_weighted = cost.subtree_cost.total(cost_config);

        Float64 current_best = group->getBestCostForProperties(expression->properties, cost_config);
        if (std::isfinite(current_best) && subtree_weighted >= current_best)
        {
            LOG_TEST(optimizer_context.log, "Pruned expression '{}' in group #{}: "
                "cost {} >= current best {}",
                expression->getDescription(), expression->group_id,
                subtree_weighted, current_best);
            return;
        }

        expression->cost = cost;
        LOG_TEST(optimizer_context.log, "group #{} expression '{}' cost {}",
            expression->group_id, expression->getDescription(), cost.subtree_cost.total(cost_config));
        group->updateBestImplementation(expression, cost_config);
        return;
    }
    else
    {
        const auto & input = expression->inputs[input_index_to_optimize];
        auto child_group = optimizer_context.getGroup(input.group_id);

        /// Skip pushing OptimizeGroupTask for this child only if it is already FULLY done
        /// (explored + implemented + enforced) for the required properties — matching
        /// tryUpdateBestPlanDirectly. A child that merely has an early local best may still
        /// gain a cheaper enforcer-built alternative, so it must keep being optimized.
        bool child_already_done = child_group->isFullyDoneFor(input.required_properties);

        optimizer_context.pushTask(
            std::make_shared<OptimizeInputsTask>(expression, input_index_to_optimize + 1, cost_limit));

        if (!child_already_done)
        {
            /// Finite child cost budgets are disabled for now: deriving them from sibling best
            /// costs is unsound, because an in-progress sibling best is an upper bound, not a
            /// lower bound, so it can prune a parent expression that would still become cheapest.
            /// Pass an unbounded limit; total work is bounded by the optimizer task budget,
            /// which fails closed (see CascadesOptimizer::optimize).
            optimizer_context.pushTask(
                std::make_shared<OptimizeGroupTask>(input.group_id, input.required_properties,
                    std::numeric_limits<CostLimit>::infinity()));
        }
    }
}

}
