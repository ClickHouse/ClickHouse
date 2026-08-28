#include <Analyzer/Passes/ShipJoinPredicateToParallelReplicasPass.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 parallel_replicas_ship_join_predicate;
}

namespace
{

/// Every conjunct of an `AND` chain, or the node itself when it is not an `AND`.
void collectConjuncts(const QueryTreeNodePtr & node, QueryTreeNodes & result)
{
    const auto * function_node = node->as<FunctionNode>();
    if (function_node && function_node->getFunctionName() == "and")
    {
        for (const auto & argument : function_node->getArguments().getNodes())
            collectConjuncts(argument, result);
        return;
    }

    result.push_back(node);
}

/// The projection expression a column of `subquery` is an alias for, or null when the column is not a
/// plain projection element or is computed by an aggregate (then it is not a pre-aggregation predicate).
QueryTreeNodePtr findProjectionExpression(const QueryNode & subquery, const String & column_name)
{
    const auto & projection_columns = subquery.getProjectionColumns();
    const auto & projection_nodes = subquery.getProjection().getNodes();
    if (projection_columns.size() != projection_nodes.size())
        return nullptr;

    for (size_t i = 0; i < projection_columns.size(); ++i)
    {
        if (projection_columns[i].name != column_name)
            continue;
        if (hasAggregateFunctionNodes(projection_nodes[i]))
            return nullptr;
        return projection_nodes[i];
    }

    return nullptr;
}

/// `SELECT key FROM table_expression`, with the key's column source remapped onto the cloned
/// table expression: cloning the assembled node keeps both halves consistent.
QueryTreeNodePtr buildKeySubquery(
    const QueryTreeNodePtr & table_expression, const QueryTreeNodePtr & key, const ContextPtr & context)
{
    const auto * key_column = key->as<ColumnNode>();
    auto subquery = std::make_shared<QueryNode>(Context::createCopy(context));
    subquery->setIsSubquery(true);
    subquery->getJoinTreeNode() = table_expression;
    subquery->getProjection().getNodes() = {key};
    subquery->resolveProjectionColumns(
        {NameAndTypePair{key_column ? key_column->getColumnName() : key->getResultType()->getName(), key->getResultType()}});

    return subquery->clone();
}

/// Whether a side is cheap enough to be re-read as the `IN` source. Anything that aggregates or joins
/// would be duplicated - and, since the pass looks at both sides of a join, injecting the aggregating
/// side into its partner nests a whole second copy of it inside the predicate.
bool isUsableAsPredicateSource(const QueryTreeNodePtr & node)
{
    const auto node_type = node->getNodeType();
    if (node_type == QueryTreeNodeType::TABLE || node_type == QueryTreeNodeType::TABLE_FUNCTION)
        return true;

    const auto * query = node->as<QueryNode>();
    if (!query)
        return false;

    if (query->hasGroupBy() || query->hasLimit() || query->isDistinct())
        return false;

    for (const auto & projection_node : query->getProjection().getNodes())
        if (hasAggregateFunctionNodes(projection_node))
            return false;

    const auto join_tree_type = query->getJoinTreeNode()->getNodeType();
    return join_tree_type == QueryTreeNodeType::TABLE || join_tree_type == QueryTreeNodeType::TABLE_FUNCTION;
}

/// Whether a predicate on `expression` may be evaluated before the subquery's aggregation.
bool canFilterBeforeAggregation(const QueryNode & subquery, const QueryTreeNodePtr & expression)
{
    if (!subquery.hasGroupBy())
        return true;

    /// ROLLUP / CUBE / GROUPING SETS / WITH TOTALS synthesize rows whose keys did not come from the input,
    /// so a key predicate is not equivalent on either side of the aggregation.
    if (subquery.isGroupByWithRollup() || subquery.isGroupByWithCube() || subquery.isGroupByWithGroupingSets()
        || subquery.isGroupByWithTotals())
        return false;

    for (const auto & grouping_key : subquery.getGroupBy().getNodes())
        if (grouping_key->isEqual(*expression))
            return true;

    return false;
}

class ShipJoinPredicateVisitor : public InDepthQueryTreeVisitorWithContext<ShipJoinPredicateVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ShipJoinPredicateVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * join_node = node->as<JoinNode>();
        if (!join_node)
            return;

        /// Only an INNER join implies that a non-matching row can be dropped early. An outer join keeps
        /// unmatched rows of its preserved side, so the predicate is not implied there.
        if (join_node->getKind() != JoinKind::Inner)
            return;

        /// USING carries no expression to read the keys from.
        if (join_node->isUsingJoinExpression() || !join_node->getJoinExpression())
            return;

        QueryTreeNodes conjuncts;
        collectConjuncts(join_node->getJoinExpression(), conjuncts);

        for (const auto & conjunct : conjuncts)
        {
            const auto * equals = conjunct->as<FunctionNode>();
            if (!equals || equals->getFunctionName() != "equals" || equals->getArguments().getNodes().size() != 2)
                continue;

            const auto & lhs = equals->getArguments().getNodes()[0];
            const auto & rhs = equals->getArguments().getNodes()[1];

            /// The predicate can be pushed into either side; both are filtered by the same equality.
            tryInject(*join_node, lhs, rhs, join_node->getLeftTableExpressionNode(), join_node->getRightTableExpressionNode());
            tryInject(*join_node, rhs, lhs, join_node->getRightTableExpressionNode(), join_node->getLeftTableExpressionNode());
        }
    }

private:
    /// Push `target_key IN (SELECT source_key FROM source_side)` into `target_side` when that side is a
    /// subquery whose projection exposes the key.
    void tryInject(
        const JoinNode & join_node,
        const QueryTreeNodePtr & target_key,
        const QueryTreeNodePtr & source_key,
        const QueryTreeNodePtr & target_side,
        const QueryTreeNodePtr & source_side)
    {
        auto * target_query = target_side->as<QueryNode>();
        if (!target_query)
            return;

        if (!isUsableAsPredicateSource(source_side))
            return;

        const auto * target_column = target_key->as<ColumnNode>();
        const auto * source_column = source_key->as<ColumnNode>();
        if (!target_column || !source_column)
            return;

        if (target_column->getColumnSource().get() != target_side.get()
            || source_column->getColumnSource().get() != source_side.get())
            return;

        /// A `Nullable` key on either side makes the equality and `IN` disagree on NULLs, so leave it alone.
        if (target_key->getResultType()->isNullable() || source_key->getResultType()->isNullable())
            return;

        auto projection_expression = findProjectionExpression(*target_query, target_column->getColumnName());
        if (!projection_expression)
            return;

        const auto & context = getContext();
        const String function_name
            = context->getSettingsRef()[Setting::parallel_replicas_ship_join_predicate] == 2 ? "globalIn" : "in";

        auto in_function = std::make_shared<FunctionNode>(function_name);
        in_function->markAsOperator();
        in_function->getArguments().getNodes()
            = {projection_expression->clone(), buildKeySubquery(source_side, source_key, context)};
        resolveOrdinaryFunctionNodeByName(*in_function, function_name, context);

        /// WHERE is the whole point: it runs before the aggregation, so a replica reads and groups less.
        /// It is valid there when the key is a plain expression over the join tree and, if the subquery
        /// groups, one of its grouping keys - dropping such a row drops a whole group.
        QueryTreeNodePtr & destination
            = canFilterBeforeAggregation(*target_query, projection_expression) ? target_query->getWhere() : target_query->getHaving();

        if (destination)
        {
            auto and_function = std::make_shared<FunctionNode>("and");
            and_function->getArguments().getNodes() = {destination, in_function};
            resolveOrdinaryFunctionNodeByName(*and_function, "and", context);
            destination = std::move(and_function);
        }
        else
        {
            destination = std::move(in_function);
        }

        LOG_TRACE(getLogger("ShipJoinPredicateToParallelReplicas"),
            "Injected a {} predicate on {} into the {} side of the join",
            function_name, target_column->getColumnName(),
            target_side.get() == join_node.getLeftTableExpressionNode().get() ? "left" : "right");
    }
};

}

void ShipJoinPredicateToParallelReplicasPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    if (context->getSettingsRef()[Setting::parallel_replicas_ship_join_predicate] == 0)
        return;

    ShipJoinPredicateVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
