#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Functions/IFunction.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <stack>
#include <unordered_map>

using namespace DB;

namespace
{

using NodeSet = std::unordered_set<const ActionsDAG::Node *>;
using NodeMap = std::unordered_map<const ActionsDAG::Node *, bool>;

struct Frame
{
    const ActionsDAG::Node * node = nullptr;
    size_t next_child = 0;
};

bool isInjectiveFunction(const ActionsDAG::Node * node)
{
    if (node->function_base->isInjective({}))
        return true;

    size_t fixed_args = 0;
    for (const auto & child : node->children)
        if (child->type == ActionsDAG::ActionType::COLUMN)
            ++fixed_args;
    static const std::vector<String> injective = {"plus", "minus", "negate", "tuple"};
    return (fixed_args + 1 >= node->children.size()) && (std::ranges::find(injective, node->function_base->getName()) != injective.end());
}

void removeInjectiveFunctionsFromResultsRecursively(const ActionsDAG::Node * node, NodeSet & irreducible, NodeSet & visited)
{
    if (visited.contains(node))
        return;
    visited.insert(node);

    switch (node->type)
    {
        case ActionsDAG::ActionType::ALIAS:
            assert(node->children.size() == 1);
            removeInjectiveFunctionsFromResultsRecursively(node->children.at(0), irreducible, visited);
            break;
        case ActionsDAG::ActionType::ARRAY_JOIN:
            UNREACHABLE();
        case ActionsDAG::ActionType::COLUMN:
            irreducible.insert(node);
            break;
        case ActionsDAG::ActionType::FUNCTION:
            if (!isInjectiveFunction(node))
            {
                irreducible.insert(node);
            }
            else
            {
                for (const auto & child : node->children)
                    removeInjectiveFunctionsFromResultsRecursively(child, irreducible, visited);
            }
            break;
        case ActionsDAG::ActionType::INPUT:
            irreducible.insert(node);
            break;
    }
}

/// Our objective is to replace injective function nodes in `actions` results with its children
/// until only the irreducible subset of nodes remains. Against these set of nodes we will match partition key expression
/// to determine if it maps all rows with the same value of group by key to the same partition.
NodeSet removeInjectiveFunctionsFromResultsRecursively(const ActionsDAG & actions)
{
    NodeSet irreducible;
    NodeSet visited;
    for (const auto & node : actions.getOutputs())
        removeInjectiveFunctionsFromResultsRecursively(node, irreducible, visited);
    return irreducible;
}

bool allOutputsDependsOnlyOnAllowedNodes(
    const NodeSet & irreducible_nodes, const MatchedTrees::Matches & matches, const ActionsDAG::Node * node, NodeMap & visited)
{
    if (visited.contains(node))
        return visited[node];

    bool res = false;
    /// `matches` maps partition key nodes into nodes in group by actions
    if (matches.contains(node))
    {
        const auto & match = matches.at(node);
        /// Function could be mapped into its argument. In this case .monotonicity != std::nullopt (see matchTrees)
        if (match.node && !match.monotonicity)
            res = irreducible_nodes.contains(match.node);
    }

    if (!res)
    {
        switch (node->type)
        {
            case ActionsDAG::ActionType::ALIAS:
                assert(node->children.size() == 1);
                res = allOutputsDependsOnlyOnAllowedNodes(irreducible_nodes, matches, node->children.at(0), visited);
                break;
            case ActionsDAG::ActionType::ARRAY_JOIN:
                UNREACHABLE();
            case ActionsDAG::ActionType::COLUMN:
                /// Constants doesn't matter, so let's always consider them matched.
                res = true;
                break;
            case ActionsDAG::ActionType::FUNCTION:
                res = true;
                for (const auto & child : node->children)
                    res &= allOutputsDependsOnlyOnAllowedNodes(irreducible_nodes, matches, child, visited);
                break;
            case ActionsDAG::ActionType::INPUT:
                break;
        }
    }
    visited[node] = res;
    return res;
}

/// Here we check that partition key expression is a deterministic function of the reduced set of group by key nodes.
/// No need to explicitly check that each function is deterministic, because it is a guaranteed property of partition key expression (checked on table creation).
/// So it is left only to check that each output node depends only on the allowed set of nodes (`irreducible_nodes`).
bool allOutputsDependsOnlyOnAllowedNodes(
    const ActionsDAG & partition_actions, const NodeSet & irreducible_nodes, const MatchedTrees::Matches & matches)
{
    NodeMap visited;
    bool res = true;
    for (const auto & node : partition_actions.getOutputs())
        if (node->type != ActionsDAG::ActionType::INPUT)
            res &= allOutputsDependsOnlyOnAllowedNodes(irreducible_nodes, matches, node, visited);
    return res;
}

/// 0. Partition key columns should be a subset of group by key columns.
/// 1. Optimization is applicable if partition by expression is a deterministic function of col1, ..., coln and group by key is injective functions of these col1, ..., coln.
/// 2. To find col1, ..., coln we apply removeInjectiveFunctionsFromResultsRecursively to group by key actions.
/// 3. We match partition key actions with group by key actions to find col1', ..., coln' in partition key actions.
/// 4. We check that partition key is indeed a deterministic function of col1', ..., coln'.
bool isPartitionKeySuitsGroupByKey(
    const ReadFromMergeTree & reading, const ActionsDAG & group_by_actions, const AggregatingStep & aggregating)
{
    if (aggregating.isGroupingSets())
        return false;

    if (group_by_actions.hasArrayJoin() || group_by_actions.hasStatefulFunctions() || group_by_actions.hasNonDeterministic())
        return false;

    /// We are interested only in calculations required to obtain group by keys (and not aggregate function arguments for example).
    auto key_nodes = group_by_actions.findInOutputs(aggregating.getParams().keys);
    auto group_by_key_actions = ActionsDAG::cloneSubDAG(key_nodes, /*remove_aliases=*/ true);

    const auto & gb_key_required_columns = group_by_key_actions.getRequiredColumnsNames();

    const auto & partition_actions = reading.getStorageMetadata()->getPartitionKey().expression->getActionsDAG();

    /// Check that PK columns is a subset of GBK columns.
    for (const auto & col : partition_actions.getRequiredColumnsNames())
        if (std::ranges::find(gb_key_required_columns, col) == gb_key_required_columns.end())
            return false;

    const auto irreducibe_nodes = removeInjectiveFunctionsFromResultsRecursively(group_by_key_actions);

    const auto matches = matchTrees(group_by_key_actions.getOutputs(), partition_actions);

    return allOutputsDependsOnlyOnAllowedNodes(partition_actions, irreducibe_nodes, matches);
}
}

namespace DB::QueryPlanOptimizations
{

size_t tryAggregatePartitionsIndependently(QueryPlan::Node * node, QueryPlan::Nodes &)
{
    if (!node || node->children.size() != 1)
        return 0;

    auto * aggregating_step = typeid_cast<AggregatingStep *>(node->step.get());
    if (!aggregating_step)
        return 0;

    const auto * expression_node = node->children.front();
    const auto * expression_step = typeid_cast<const ExpressionStep *>(expression_node->step.get());
    if (!expression_step)
        return 0;

    auto * maybe_reading_step = expression_node->children.front()->step.get();

    if (const auto * /*filter*/ _ = typeid_cast<const FilterStep *>(maybe_reading_step))
    {
        const auto * filter_node = expression_node->children.front();
        if (filter_node->children.size() != 1 || !filter_node->children.front()->step)
            return 0;
        maybe_reading_step = filter_node->children.front()->step.get();
    }

    auto * reading = typeid_cast<ReadFromMergeTree *>(maybe_reading_step);
    if (!reading)
        return 0;

    if (!reading->willOutputEachPartitionThroughSeparatePort()
        && isPartitionKeySuitsGroupByKey(*reading, expression_step->getExpression(), *aggregating_step))
    {
        if (reading->requestOutputEachPartitionThroughSeparatePort())
            aggregating_step->skipMerging();
    }

    return 0;
}

}
