#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/QueryPlan/ShuffleExchangeStep.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace QueryPlanOptimizations
{

/// Replaces LogicalJoin step with a subtree like this:
///
///   GatherExchange
///     LogicalJoin
///       ShuffleExchange
///         Expression: compute join key for right source
///         ...
///       ShuffleExchange
///         Expression: compute join key for left source
///         ...
void tryMakeDistributedJoin(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    /// Is this a join step?
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        return;

    /// Joining two sources?
    if (node.children.size() != 2)
        return;

    /// Check if join is possible to be distributed
    const auto & join_info = join_step->getJoinInfo();
    if (join_info.kind != JoinKind::Inner ||
        join_info.strictness != JoinStrictness::All ||
        (join_info.locality != JoinLocality::Unspecified && join_info.locality != JoinLocality::Global) ||
        !join_info.expression.disjunctive_conditions.empty())
    {
        return;
    }

    Names join_keys_a;
    Names join_keys_b;

    /// Only equi-join is supported
    const auto & join_condition = join_info.expression.condition;
    for (const auto & predicate : join_condition.predicates)
    {
        if (predicate.op != PredicateOperator::Equals)
            return;

        join_keys_a.push_back(predicate.left_node.getColumnName());
        join_keys_b.push_back(predicate.right_node.getColumnName());
    }

    QueryPlan::Node * source_a = node.children[0];
    QueryPlan::Node * source_b = node.children[1];

    /// Extract expressions for calculating join on keys
    /// Move them into separate nodes
    /// Replace pre-join actions in the join step with pass-through (no-op) actions
    {
        const auto & actions = join_step->getExpressionActions();

        /// Adds all columns from header to inputs and outputs without any transformations
        auto add_pass_through_actions = [](ActionsDAG & actions_dag, const Block & header)
        {
            for (const auto & column : header.getColumnsWithTypeAndName())
            {
                actions_dag.addOrReplaceInOutputs(actions_dag.addInput(column));
            }
        };

        if (actions.left_pre_join_actions)
        {
            source_a = makeExpressionNodeOnTopOf(source_a, std::move(*actions.left_pre_join_actions), {}, nodes);
            add_pass_through_actions(*actions.left_pre_join_actions, source_a->step->getOutputHeader());
            join_step->updateInputHeader(source_a->step->getOutputHeader(), 0);
        }

        if (actions.right_pre_join_actions)
        {
            source_b = makeExpressionNodeOnTopOf(source_b, std::move(*actions.right_pre_join_actions), {}, nodes);
            add_pass_through_actions(*actions.right_pre_join_actions, source_b->step->getOutputHeader());
            join_step->updateInputHeader(source_b->step->getOutputHeader(), 1);
        }
    }

    const size_t bucket_count = optimization_settings.default_shuffle_join_bucket_count;    /// TODO: estimate number of buckets based on statistics and available nodes and memory

    /// Add shuffle exchange step above read from right source
    auto & exchange_shuffle_a_node = nodes.emplace_back();
    exchange_shuffle_a_node.step = std::make_unique<ShuffleExchangeStep>(source_a->step->getOutputHeader(), join_keys_a, bucket_count);
    exchange_shuffle_a_node.step->setStepDescription(fmt::format("by hash([{}])", fmt::join(join_keys_a, ", ")));
    exchange_shuffle_a_node.children = {source_a};

    /// Add shuffle exchange step above read from left source
    auto & exchange_shuffle_b_node = nodes.emplace_back();
    exchange_shuffle_b_node.step = std::make_unique<ShuffleExchangeStep>(source_b->step->getOutputHeader(), join_keys_b, bucket_count);
    exchange_shuffle_b_node.step->setStepDescription(fmt::format("by hash([{}])", fmt::join(join_keys_b, ", ")));
    exchange_shuffle_b_node.children = {source_b};

    /// Move join step to a new node
    auto & new_join_node = nodes.emplace_back();
    new_join_node.step = std::move(node.step);
    new_join_node.children = {&exchange_shuffle_a_node, &exchange_shuffle_b_node};

    /// Add gather exchange step above join
    QueryPlan::Node gather_node;
    QueryPlanStepPtr exchange_gather_step = std::make_unique<GatherExchangeStep>(new_join_node.step->getOutputHeader());
    gather_node.step = std::move(exchange_gather_step);
    gather_node.children = {&new_join_node};

    /// Replace join node with gather node
    node = std::move(gather_node);
}

}

}
