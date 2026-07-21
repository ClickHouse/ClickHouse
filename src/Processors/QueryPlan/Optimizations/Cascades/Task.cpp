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
    /// enforced) for these properties and has a satisfying plan - re-running would be a no-op.
    /// We deliberately do NOT prune just because a current best is within a finite cost budget:
    /// the budget is an upper bound, not a lower bound, so such pruning is unsound for
    /// optimality, and it can return before stage-3 enforcers add the distributed alternatives.
    {
        const auto & cost_config = optimizer_context.getMemo().getEnvironment().cost_config;
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
        optimizer_context.pushTask(std::make_shared<OptimizeGroupTask>(group_id, required_properties));
        optimizer_context.pushTask(std::make_shared<ExploreGroupTask>(group_id));
    }
    else if (!group->isOptimizedFor(required_properties))
    {
        optimizer_context.pushTask(std::make_shared<OptimizeGroupTask>(group_id, required_properties));

        for (auto & expression : group->logical_expressions)
            optimizer_context.pushTask(std::make_shared<OptimizeExpressionTask>(expression, required_properties));

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
        /// creates GatherExchange(sorted) from it - all in the same Stage 3 pass.

        group->setEnforcedFor(required_properties);

        auto enforcer_expressions = runEnforcementStage(optimizer_context, group);

        if (!enforcer_expressions.empty())
        {
            /// Push self-task FIRST so it sits at the bottom of the stack (LIFO) and
            /// executes AFTER all OptimizeInputsTask complete.  This re-run checks
            /// whether the newly created enforcer expressions need further composition.
            optimizer_context.pushTask(
                std::make_shared<OptimizeGroupTask>(group_id, required_properties));

            for (const auto & new_expression : enforcer_expressions)
                optimizer_context.scheduleCosting(new_expression);
        }
    }
    else
    {
        /// All stages complete: explored, implementation rules applied, enforcers tried,
        /// and a best implementation exists. Mark the group as fully done.
        group->setFullyDoneFor(required_properties);
    }
}

std::vector<GroupExpressionPtr> OptimizeGroupTask::runEnforcementStage(OptimizerContext & optimizer_context, const GroupPtr & group) const
{
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
                /// nor counted as progress - they cannot exhaust the task budget.
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

    return enforcer_expressions;
}


void ExploreGroupTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TEST(optimizer_context.log, "ExploreGroupTask group_id: {}", group_id);
    auto group = optimizer_context.getGroup(group_id);
    group->setExplored();

    for (const auto & expression : group->logical_expressions)
        optimizer_context.pushTask(std::make_shared<ExploreExpressionTask>(expression));
}


/// Schedule an ApplyRuleTask for every rule that matches the expression, then explore any
/// unexplored input group. Explore and optimize differ only in the rule set (transformation
/// vs implementation) and the properties the rules match against.
static void scheduleApplicableRules(
    OptimizerContext & optimizer_context,
    const GroupExpressionPtr & expression,
    const ExpressionProperties & required_properties,
    const std::vector<OptimizationRulePtr> & rules)
{
    std::vector<std::pair<Promise, OptimizationRulePtr>> moves;
    for (const auto & rule : rules)
    {
        if (!expression->isApplied(*rule, required_properties) && rule->checkPattern(expression, required_properties, optimizer_context.getMemo()))
            moves.push_back({rule->getPromise(), rule});
    }

    /// Sort ascending so the LIFO task stack pops the highest-promise rule first.
    std::sort(moves.begin(), moves.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    for (const auto & move : moves)
        optimizer_context.pushTask(std::make_shared<ApplyRuleTask>(expression, required_properties, move.second));

    for (const auto & input : expression->inputs)
    {
        if (!optimizer_context.getGroup(input.group_id)->isExplored())
            optimizer_context.pushTask(std::make_shared<ExploreGroupTask>(input.group_id));
    }
}


void ExploreExpressionTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TEST(optimizer_context.log, "ExploreExpressionTask group_id: {}, expression: {}",
        expression->group_id, expression->getName());

    /// Transformation rules produce logical alternatives, so they match against no required properties.
    scheduleApplicableRules(optimizer_context, expression, ExpressionProperties{}, optimizer_context.getTransformationRules());
}


void OptimizeExpressionTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TEST(optimizer_context.log, "OptimizeExpressionTask group #{}, expression: {}, required properties {}",
        expression->group_id, expression->getName(), required_properties.dump());

    /// Implementation rules produce physical alternatives that must satisfy the required properties.
    scheduleApplicableRules(optimizer_context, expression, required_properties, optimizer_context.getImplementationRules());
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
            optimizer_context.pushTask(std::make_shared<ExploreExpressionTask>(new_expression));
        }
        else
        {
            optimizer_context.scheduleCosting(new_expression);
        }
    }
}

void OptimizeInputsTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TEST(optimizer_context.log, "OptimizeInputsTask group #{} expression {}",
        expression->group_id, expression->dump(optimizer_context.getMemo().getEnvironment().cost_config));

    /// All inputs were processed?
    if (input_index_to_optimize == expression->inputs.size())
    {
        const auto & cost_config = optimizer_context.getMemo().getEnvironment().cost_config;

        /// If any input has no satisfying implementation, this expression is
        /// unsatisfiable - skip cost estimation.
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

        /// Cost the expression and drop it if the group already holds a cheaper best for the
        /// same properties (same-group best-cost pruning, not a cost-bounded search).
        optimizer_context.costAndUpdateBest(expression, /*prune_against_best=*/true);
        return;
    }
    else
    {
        const auto & input = expression->inputs[input_index_to_optimize];
        auto child_group = optimizer_context.getGroup(input.group_id);

        /// Skip pushing OptimizeGroupTask for this child only if it is already FULLY done
        /// (explored + implemented + enforced) for the required properties - matching
        /// tryUpdateBestPlanDirectly. A child that merely has an early local best may still
        /// gain a cheaper enforcer-built alternative, so it must keep being optimized.
        bool child_already_done = child_group->isFullyDoneFor(input.required_properties);

        optimizer_context.pushTask(
            std::make_shared<OptimizeInputsTask>(expression, input_index_to_optimize + 1));

        if (!child_already_done)
        {
            /// The search has no per-subtree cost budget: deriving one from sibling best costs
            /// is unsound, because an in-progress sibling best is an upper bound, not a lower
            /// bound, so it could prune a parent expression that would still become cheapest.
            /// Total work is bounded by the optimizer task budget, which fails closed
            /// (see CascadesOptimizer::optimize).
            optimizer_context.pushTask(
                std::make_shared<OptimizeGroupTask>(input.group_id, input.required_properties));
        }
    }
}


String ExploreExpressionTask::describe() const
{
    return fmt::format("explore expression '{}'", expression->getName());
}

String OptimizeExpressionTask::describe() const
{
    return fmt::format("optimize expression '{}' for {}", expression->getName(), required_properties.dump());
}

String ApplyRuleTask::describe() const
{
    return fmt::format("apply rule {} to '{}'", rule->getName(), expression->getName());
}

String OptimizeInputsTask::describe() const
{
    return fmt::format("optimize input #{} of '{}'", input_index_to_optimize, expression->getName());
}

}
