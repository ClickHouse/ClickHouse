#include <Analyzer/Passes/OrderByLimitByDuplicateEliminationPass.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/ValidationUtils.h>
#include <Common/checkStackSize.h>

namespace DB
{

namespace
{

bool areWrittenAlike(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
{
    return compareGroupByKeys(lhs, rhs) && areConstantsWrittenAlike(lhs, rhs);
}

bool isKey(const QueryTreeNodePtr & node, const QueryTreeNodes & group_by_keys)
{
    return std::ranges::any_of(group_by_keys, [&](const QueryTreeNodePtr & key) { return areWrittenAlike(key, node); });
}

/// Returns true if `part` is calculated after the aggregation for `node`. Keys, and arguments of aggregate functions, are calculated before.
bool isCalculatedFor(const QueryTreeNodePtr & node, const QueryTreeNodePtr & part, const QueryTreeNodes & group_by_keys)
{
    checkStackSize();

    const auto * function_node = node->as<FunctionNode>();
    if (isKey(node, group_by_keys) || (function_node && function_node->isAggregateFunction()))
        return false;

    return areWrittenAlike(node, part)
        || std::ranges::any_of(
               node->getChildren(), [&](const auto & child) { return child && isCalculatedFor(child, part, group_by_keys); });
}

bool hasOnlyCalculatedArrayJoins(
    const QueryTreeNodePtr & node, const QueryTreeNodes & clauses, const QueryTreeNodes & group_by_keys)
{
    checkStackSize();

    if (isKey(node, group_by_keys))
        return true;

    if (const auto * function_node = node->as<FunctionNode>(); function_node && function_node->getFunctionName() == "arrayJoin")
        return std::ranges::any_of(clauses, [&](const auto & clause) { return isCalculatedFor(clause, node, group_by_keys); });

    return std::ranges::all_of(
        node->getChildren(), [&](const auto & child) { return !child || hasOnlyCalculatedArrayJoins(child, clauses, group_by_keys); });
}

class OrderByLimitByDuplicateEliminationVisitor : public InDepthQueryTreeVisitor<OrderByLimitByDuplicateEliminationVisitor>
{
public:
    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        const auto group_by_keys = getGroupByKeys(*query_node);
        QueryTreeNodes other_clauses;
        for (const auto & clause : query_node->getChildren())
        {
            const bool is_calculated_before_aggregation
                = query_node->hasGroupBy() && (clause == query_node->getWhere() || clause == query_node->getPrewhere());
            if (clause && clause != query_node->getJoinTreeNode() && clause != query_node->getGroupByNode()
                && clause != query_node->getOrderByNode() && clause != query_node->getLimitByNode() && !is_calculated_before_aggregation)
                other_clauses.push_back(clause);
        }

        auto is_redundant = [&](const QueryTreeNodePtr & expression, const QueryTreeNodePtr & previous_expression)
        {
            if (areConstantsWrittenAlike(expression, previous_expression))
                return true;

            /// It can only be calculated from the same key as the previous one.
            if (!group_by_keys.empty() && !canBeCalculatedFromKeys(expression, group_by_keys))
                return true;

            /// An `arrayJoin` that is not calculated for another clause would only add rows.
            return !hasOnlyCalculatedArrayJoins(expression, other_clauses, group_by_keys);
        };

        if (query_node->hasOrderBy())
        {
            QueryTreeNodePtrWithHashSet unique_expressions_nodes_set;
            QueryTreeNodes result_nodes;

            auto & query_order_by_nodes = query_node->getOrderBy().getNodes();

            for (auto & sort_node : query_order_by_nodes)
            {
                auto & sort_node_typed = sort_node->as<SortNode &>();

                /// Skip elements with WITH FILL
                if (sort_node_typed.withFill())
                {
                    result_nodes.push_back(sort_node);
                    continue;
                }

                const auto & expression = sort_node_typed.getExpression();

                /// Different values can be equal with a collation, so the following keys are not redundant after it.
                auto previous = unique_expressions_nodes_set.find(expression);
                if (previous != unique_expressions_nodes_set.end() && is_redundant(expression, previous->node))
                    continue;

                if (!sort_node_typed.getCollator())
                    unique_expressions_nodes_set.emplace(expression);
                result_nodes.push_back(sort_node);
            }

            query_order_by_nodes = std::move(result_nodes);
        }

        if (query_node->hasLimitBy())
        {
            QueryTreeNodePtrWithHashSet unique_expressions_nodes_set;
            QueryTreeNodes result_nodes;

            auto & query_limit_by_nodes = query_node->getLimitBy().getNodes();

            for (auto & limit_by_node : query_limit_by_nodes)
            {
                auto [previous, inserted] = unique_expressions_nodes_set.emplace(limit_by_node);
                if (inserted || !is_redundant(limit_by_node, previous->node))
                    result_nodes.push_back(limit_by_node);
            }

            query_limit_by_nodes = std::move(result_nodes);
        }
    }
};

}

void OrderByLimitByDuplicateEliminationPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr)
{
    OrderByLimitByDuplicateEliminationVisitor visitor;
    visitor.visit(query_tree_node);
}

}

