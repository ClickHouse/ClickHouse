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

auto print_node = [](const ActionsDAG::Node * node_)
{
    String children;
    for (const auto & child : node_->children)
        children += fmt::format("{}, ", static_cast<const void *>(child));
    LOG_DEBUG(
        &Poco::Logger::get("debug"),
        "current node {} {} {} {}",
        static_cast<const void *>(node_),
        node_->result_name,
        node_->type,
        children);
};

bool isInjectiveFunction(const ActionsDAG::Node * node)
{
    if (node->function_base->isInjective({}))
        return true;

    size_t fixed_args = 0;
    for (const auto & child : node->children)
        if (child->type == ActionsDAG::ActionType::COLUMN)
            ++fixed_args;
    static const std::vector<String> injective = {"plus", "minus"};
    return (fixed_args + 1 >= node->children.size()) && (std::ranges::find(injective, node->function_base->getName()) != injective.end());
}

void removeInjectiveColumnsFromResultsRecursively(
    const ActionsDAGPtr & actions, const ActionsDAG::Node * cur_node, NodeSet & irreducible, NodeSet & visited)
{
    if (visited.contains(cur_node))
        return;
    visited.insert(cur_node);

    print_node(cur_node);

    switch (cur_node->type)
    {
        case ActionsDAG::ActionType::ALIAS:
            assert(cur_node->children.size() == 1);
            removeInjectiveColumnsFromResultsRecursively(actions, cur_node->children.at(0), irreducible, visited);
            break;
        case ActionsDAG::ActionType::ARRAY_JOIN:
            break;
        case ActionsDAG::ActionType::COLUMN:
            irreducible.insert(cur_node);
            break;
        case ActionsDAG::ActionType::FUNCTION:
            LOG_DEBUG(&Poco::Logger::get("debug"), "{} {}", __LINE__, isInjectiveFunction(cur_node));
            if (!isInjectiveFunction(cur_node))
                irreducible.insert(cur_node);
            else
                for (const auto & child : cur_node->children)
                    removeInjectiveColumnsFromResultsRecursively(actions, child, irreducible, visited);
            break;
        case ActionsDAG::ActionType::INPUT:
            irreducible.insert(cur_node);
            break;
    }
}

/// Removes injective functions recursively from result columns until it is no longer possible.
NodeSet removeInjectiveColumnsFromResultsRecursively(ActionsDAGPtr actions)
{
    NodeSet irreducible;
    NodeSet visited;

    for (const auto & node : actions->getOutputs())
        removeInjectiveColumnsFromResultsRecursively(actions, node, irreducible, visited);

    LOG_DEBUG(&Poco::Logger::get("debug"), "irreducible nodes:");
    for (const auto & node : irreducible)
        print_node(node);

    return irreducible;
}

bool allOutputsCovered(
    const ActionsDAGPtr & partition_actions,
    const NodeSet & irreducible_nodes,
    const MatchedTrees::Matches & matches,
    const ActionsDAG::Node * cur_node,
    NodeMap & visited)
{
    if (visited.contains(cur_node))
        return visited[cur_node];

    auto has_match_in_group_by_actions = [&irreducible_nodes, &matches, &cur_node]()
    {
        if (matches.contains(cur_node))
        {
            if (const auto * node_in_gb_actions = matches.at(cur_node).node;
                node_in_gb_actions && node_in_gb_actions->type == cur_node->type)
            {
                return irreducible_nodes.contains(node_in_gb_actions);
            }
        }
        return false;
    };

    bool res = has_match_in_group_by_actions();
    if (!res)
    {
        switch (cur_node->type)
        {
            case ActionsDAG::ActionType::ALIAS:
                assert(cur_node->children.size() == 1);
                res = allOutputsCovered(partition_actions, irreducible_nodes, matches, cur_node->children.at(0), visited);
                break;
            case ActionsDAG::ActionType::ARRAY_JOIN:
                break;
            case ActionsDAG::ActionType::COLUMN:
                /// Constants doesn't matter, so let's always consider them matched.
                res = true;
                break;
            case ActionsDAG::ActionType::FUNCTION:
                res = true;
                for (const auto & child : cur_node->children)
                    res &= allOutputsCovered(partition_actions, irreducible_nodes, matches, child, visited);
                break;
            case ActionsDAG::ActionType::INPUT:
                break;
        }
    }
    print_node(cur_node);
    LOG_DEBUG(&Poco::Logger::get("debug"), "res={}", res);
    visited[cur_node] = res;
    return res;
}

bool allOutputsCovered(ActionsDAGPtr partition_actions, const NodeSet & irreducible_nodes, const MatchedTrees::Matches & matches)
{
    NodeMap visited;

    bool res = true;
    for (const auto & node : partition_actions->getOutputs())
        if (node->type != ActionsDAG::ActionType::INPUT)
            res &= allOutputsCovered(partition_actions, irreducible_nodes, matches, node, visited);
    return res;
}

bool isPartitionKeySuitsGroupByKey(const ReadFromMergeTree & reading, ActionsDAGPtr group_by_actions, const AggregatingStep & aggregating)
{
    /// 0. Partition key columns should be a subset of group by key columns.
    /// 1. Optimization is applicable if partition by expression is a deterministic function of col1, ..., coln and group by keys are injective functions of some of col1, ..., coln.

    if (aggregating.isGroupingSets() || group_by_actions->hasArrayJoin() || group_by_actions->hasStatefulFunctions())
        return false;

    /// Check that PK columns is a subset of GBK columns.
    const auto partition_actions = reading.getStorageMetadata()->getPartitionKey().expression->getActionsDAG().clone();

    /// We are interested only in calculations required to obtain group by keys.
    group_by_actions->removeUnusedActions(aggregating.getParams().keys);
    const auto & gb_keys = group_by_actions->getRequiredColumnsNames();

    LOG_DEBUG(&Poco::Logger::get("debug"), "group by req cols: {}", fmt::join(gb_keys, ", "));
    LOG_DEBUG(&Poco::Logger::get("debug"), "partition by cols: {}", fmt::join(partition_actions->getRequiredColumnsNames(), ", "));

    for (const auto & col : partition_actions->getRequiredColumnsNames())
        if (std::ranges::find(gb_keys, col) == gb_keys.end())
            return false;

    /* /// PK is always a deterministic expression without constants. No need to check. */

    /* /// We will work only with subexpression that depends on partition key columns. */
    LOG_DEBUG(&Poco::Logger::get("debug"), "group by actions before:\n{}", group_by_actions->dumpDAG());
    LOG_DEBUG(&Poco::Logger::get("debug"), "partition by actions before:\n{}", partition_actions->dumpDAG());

    LOG_DEBUG(&Poco::Logger::get("debug"), "group by actions after:\n{}", group_by_actions->dumpDAG());
    LOG_DEBUG(&Poco::Logger::get("debug"), "partition by actions after:\n{}", partition_actions->dumpDAG());

    /// For cases like `partition by col + group by col+1` or `partition by hash(col) + group by hash(col)`
    const auto irreducibe_nodes = removeInjectiveColumnsFromResultsRecursively(group_by_actions);

    const auto matches = matchTrees(*group_by_actions, *partition_actions);
    LOG_DEBUG(&Poco::Logger::get("debug"), "matches:");
    for (const auto & match : matches)
    {
        if (match.first)
            print_node(match.first);
        if (match.second.node)
            print_node(match.second.node);
        LOG_DEBUG(&Poco::Logger::get("debug"), "----------------");
    }

    const bool res = allOutputsCovered(partition_actions, irreducibe_nodes, matches);
    LOG_DEBUG(&Poco::Logger::get("debug"), "result={}", res);
    return res;
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
    if (expression_node->children.size() != 1 || !expression_step)
        return 0;

    auto * reading_step = expression_node->children.front()->step.get();

    if (const auto * filter = typeid_cast<const FilterStep *>(reading_step))
    {
        const auto * filter_node = expression_node->children.front();
        if (filter_node->children.size() != 1 || !filter_node->children.front()->step)
            return 0;
        reading_step = filter_node->children.front()->step.get();
    }

    auto * reading = typeid_cast<ReadFromMergeTree *>(reading_step);
    if (!reading)
        return 0;

    if (!reading->willOutputEachPartitionThroughSeparatePort()
        && isPartitionKeySuitsGroupByKey(*reading, expression_step->getExpression()->clone(), *aggregating_step))
    {
        if (reading->requestOutputEachPartitionThroughSeparatePort())
            aggregating_step->skipMerging();
    }

    return 0;
}

}
