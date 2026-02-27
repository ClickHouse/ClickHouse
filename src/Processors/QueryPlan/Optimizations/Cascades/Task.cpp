#include <cmath>
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

    /// Branch-and-bound: skip this group if it already has a satisfying plan and either:
    /// (a) the group has been fully processed (explored + optimized for these properties),
    ///     so re-running is a no-op — this avoids wasting tasks on repeated visits; or
    /// (b) the cost limit is finite and the current best is within budget,
    ///     so no further optimization can help the parent.
    {
        const auto & cost_config = optimizer_context.getMemo().getCostConfig();
        auto best = group->getBestImplementation(required_properties, cost_config);
        if (best.expression)
        {
            bool group_fully_processed = group->isExplored() && group->isOptimizedFor(required_properties);
            bool within_budget = std::isfinite(cost_limit)
                && best.cost.subtree_cost.weighted_total(cost_config) <= cost_limit;
            if (group_fully_processed || within_budget)
            {
                if (group_fully_processed)
                    group->setFullyDoneFor(required_properties);
                LOG_TEST(optimizer_context.log, "Pruned OptimizeGroupTask group #{}: "
                    "best cost {} <= limit {}",
                    group_id, best.cost.subtree_cost.weighted_total(cost_config), cost_limit);
                return;
            }
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
    else if (!group->getBestImplementation(required_properties, optimizer_context.getMemo().getCostConfig()).expression)
    {
        /// Stage 3: Apply enforcers to physical expressions that don't satisfy the
        /// required properties.  Enforcers produce self-referential expressions whose
        /// inputs point back to the same group with relaxed requirements.  After their
        /// inputs are optimized (via OptimizeInputsTask), the re-pushed self-task
        /// re-enters Stage 3 to try further enforcers on the newly created expressions,
        /// enabling natural composition (e.g. Sort + Gather -> Strategy A).

        /// Collect enforcer expressions first, then push tasks in the right order.
        std::vector<GroupExpressionPtr> enforcer_expressions;

        /// Deduplicate enforcers: different source expressions with the same
        /// (node_count, is_replicated) produce physically identical exchange steps.
        /// Track which (enforcer, source_distribution_shape) combos we've already applied.
        std::unordered_set<String> seen_enforcer_keys;

        /// Copy the list because enforcers add new physical expressions to the group.
        auto existing_implementations = group->physical_expressions;
        for (auto & expression : existing_implementations)
        {
            if (required_properties.isSatisfiedBy(expression->properties))
            {
                optimizer_context.updateBestPlan(expression);
                continue;
            }

            for (const auto & enforcer : optimizer_context.getEnforcerRules())
            {
                if (!enforcer->checkPattern(expression, required_properties, optimizer_context.getMemo()))
                    continue;

                String enforcer_key = enforcer->getName()
                    + ":" + std::to_string(expression->properties.distribution.node_count)
                    + ":" + std::to_string(expression->properties.distribution.is_replicated);
                if (!seen_enforcer_keys.insert(enforcer_key).second)
                    continue;

                auto new_expressions = enforcer->apply(expression, required_properties, optimizer_context.getMemo());
                enforcer_expressions.insert(enforcer_expressions.end(), new_expressions.begin(), new_expressions.end());
            }
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

    const auto & cost_config = optimizer_context.getMemo().getCostConfig();

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

            /// Branch-and-bound: use the current best cost for this group as the limit
            /// for optimizing inputs of new implementations.
            auto group = optimizer_context.getGroup(new_expression->group_id);
            Float64 best_cost = group->getBestCostForProperties(new_expression->properties, cost_config);
            CostLimit updated_limit = std::isfinite(best_cost) ? std::min(best_cost, cost_limit) : cost_limit;
            optimizer_context.pushTask(std::make_shared<OptimizeInputsTask>(new_expression, 0, updated_limit));
        }
    }
}

void OptimizeInputsTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TEST(optimizer_context.log, "OptimizeInputsTask group #{} expression {}",
        expression->group_id, expression->dump());

    /// All inputs were processed?
    if (input_index_to_optimize == expression->inputs.size())
    {
        /// Ensure statistics are derived before cost estimation
        optimizer_context.deriveStatistics(expression->group_id);

        /// Compute the cost and check if this expression beats the current best
        /// before storing it (branch-and-bound pruning).
        const auto & cost_config = optimizer_context.getMemo().getCostConfig();
        auto group = optimizer_context.getGroup(expression->group_id);
        auto cost = optimizer_context.getCostEstimator().estimateCost(expression);
        Float64 subtree_weighted = cost.subtree_cost.weighted_total(cost_config);

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
            expression->group_id, expression->getDescription(), cost.subtree_cost.total());
        group->updateBestImplementation(expression, cost_config);
        return;
    }
    else
    {
        const auto & input = expression->inputs[input_index_to_optimize];
        const auto & cost_config = optimizer_context.getMemo().getCostConfig();

        /// Budget-based early termination: if the costs of already-optimized siblings
        /// already exceed the limit, this expression cannot beat the current best.
        /// Abandon the entire remaining OptimizeInputsTask chain.
        CostLimit child_limit = optimizer_context.computeChildCostLimit(expression, input_index_to_optimize, cost_limit);
        if (std::isfinite(child_limit) && child_limit <= 0)
        {
            LOG_TEST(optimizer_context.log, "Pruned remaining inputs for '{}' in group #{}: "
                "budget exhausted (remaining limit {})",
                expression->getDescription(), expression->group_id, child_limit);
            return;
        }

        auto child_group = optimizer_context.getGroup(input.group_id);

        /// Skip pushing OptimizeGroupTask for this child if it is already fully optimized
        /// and has a satisfying implementation — the task would be a no-op.
        bool child_already_done = child_group->isExplored()
            && child_group->isOptimizedFor(input.required_properties)
            && child_group->getBestImplementation(input.required_properties, cost_config).expression;

        optimizer_context.pushTask(
            std::make_shared<OptimizeInputsTask>(expression, input_index_to_optimize + 1, cost_limit));

        if (!child_already_done)
        {
            optimizer_context.pushTask(
                std::make_shared<OptimizeGroupTask>(input.group_id, input.required_properties, child_limit));
        }
    }
}

}
