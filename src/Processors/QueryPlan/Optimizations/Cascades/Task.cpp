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
    LOG_TRACE(optimizer_context.log, "OptimizeGroupTask group_id: {}", group_id);
    auto group = optimizer_context.getGroup(group_id);
    if (!group->isExplored())
    {
        /// Explore the group and then re-run OptimizeGroup again
        optimizer_context.pushTask(std::make_shared<OptimizeGroupTask>(group_id, cost_limit));
        optimizer_context.pushTask(std::make_shared<ExploreGroupTask>(group_id, cost_limit));
    }
    else
    {
        for (auto & expression : group->expressions)
            optimizer_context.pushTask(std::make_shared<OptimizeExpressionTask>(expression, cost_limit));
    }
}


void ExploreGroupTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TRACE(optimizer_context.log, "ExploreGroupTask group_id: {}", group_id);
    auto group = optimizer_context.getGroup(group_id);
    group->setExplored();

    for (const auto & expression : group->expressions)
        optimizer_context.pushTask(std::make_shared<ExploreExpressionTask>(expression, cost_limit));
}


void ExploreExpressionTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TRACE(optimizer_context.log, "ExploreExpressionTask group_id: {}, expression: {}",
        expression->group_id, expression->getName());

    std::vector<std::pair<Promise, OptimizationRulePtr>> moves;
    for (const auto & rule : optimizer_context.getRules())
    {
        if (!expression->isApplied(*rule) && rule->checkPattern(expression, optimizer_context.getMemo()))
            moves.push_back({rule->getPromise(), rule});
    }

    /// Sort moves by promise in ascending order
    std::sort(moves.begin(), moves.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    for (const auto & m : moves)
        optimizer_context.pushTask(std::make_shared<ApplyRuleTask>(expression, m.second, m.first, cost_limit));

    for (GroupId input_group_id : expression->inputs)
    {
        if (!optimizer_context.getGroup(input_group_id)->isExplored())
            optimizer_context.pushTask(std::make_shared<ExploreGroupTask>(input_group_id, cost_limit));
    }
}


void OptimizeExpressionTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TRACE(optimizer_context.log, "OptimizeExpressionTask group_id: {}, expression: {}",
        expression->group_id, expression->getName());

    /// TODO: is this the same as ExploreExpressionTask::execute but just with a different set of rules? 

    std::vector<std::pair<Promise, OptimizationRulePtr>> moves;
    for (const auto & rule : optimizer_context.getRules())
    {
        if (!expression->isApplied(*rule) && rule->checkPattern(expression, optimizer_context.getMemo()))
            moves.push_back({rule->getPromise(), rule});
    }

    /// Sort moves by promise in ascending order
    std::sort(moves.begin(), moves.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    for (const auto & m : moves)
        optimizer_context.pushTask(std::make_shared<ApplyRuleTask>(expression, m.second, m.first, cost_limit));

    for (GroupId input_group_id : expression->inputs)
    {
        if (!optimizer_context.getGroup(input_group_id)->isExplored())
            optimizer_context.pushTask(std::make_shared<ExploreGroupTask>(input_group_id, cost_limit));
    }
}


void ApplyRuleTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TRACE(optimizer_context.log, "ApplyRuleTask rule: '{}', group_id: {} expression: {}",
        rule->getName(), expression->group_id, expression->getName());

    auto new_expressions = rule->apply(expression, optimizer_context.getMemo());
//    updateMemo(new_expressions, optimizer_context);

    /// TODO: implement further

    for (const auto & new_expression : new_expressions)
        optimizer_context.pushTask(std::make_shared<ExploreExpressionTask>(new_expression, cost_limit));
}

//void ApplyRuleTask::updateMemo(const std::vector<GroupExpressionPtr> & new_expressions, OptimizerContext & optimizer_context)
//{
//    for (const auto & new_expression : new_expressions)
//    {
//        auto group = optimizer_context.getGroup(expression->group_id);
//        group->addExpression(new_expression);
//    }
//}


void OptimizeInputsTask::execute(OptimizerContext & optimizer_context)
{
    LOG_TRACE(optimizer_context.log, "OptimizeInputsTask group_id: {}", expression->group_id);
}

}
