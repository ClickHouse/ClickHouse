#include <Analyzer/Passes/ShipJoinPredicateToParallelReplicasPass.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <Functions/IFunction.h>
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

/// `c1 AND c2 AND ...`, the single conjunct itself, or null for an empty list.
QueryTreeNodePtr combineConjuncts(QueryTreeNodes conjuncts, const ContextPtr & context)
{
    if (conjuncts.empty())
        return nullptr;

    if (conjuncts.size() == 1)
        return std::move(conjuncts.front());

    auto and_function = std::make_shared<FunctionNode>("and");
    and_function->getArguments().getNodes() = std::move(conjuncts);
    resolveOrdinaryFunctionNodeByName(*and_function, "and", context);

    return and_function;
}

/// `SELECT key FROM table_expression WHERE predicates`, with the key's column source remapped onto the
/// cloned table expression: cloning the assembled node keeps every half consistent.
QueryTreeNodePtr buildKeySubquery(
    const QueryTreeNodePtr & table_expression,
    const QueryTreeNodePtr & key,
    QueryTreeNodes predicates,
    const ContextPtr & context)
{
    const auto * key_column = key->as<ColumnNode>();
    auto subquery = std::make_shared<QueryNode>(Context::createCopy(context));
    subquery->setIsSubquery(true);
    subquery->getJoinTreeNode() = table_expression;
    subquery->getProjection().getNodes() = {key};
    subquery->getWhere() = combineConjuncts(std::move(predicates), context);
    subquery->resolveProjectionColumns(
        {NameAndTypePair{key_column ? key_column->getColumnName() : key->getResultType()->getName(), key->getResultType()}});

    return subquery->clone();
}

/// Whether a conjunct of the join's `ON` - or of the enclosing `WHERE` - restricts the source side and
/// nothing else, so that repeating it inside the injected subquery is sound and worth doing. It has to
/// read columns of that side only, be deterministic, since the subquery evaluates it a second time, and
/// carry no subquery of its own, which would nest another set to build inside the predicate.
bool restrictsSourceSideOnly(const QueryTreeNodePtr & node, const QueryTreeNodePtr & source_side)
{
    bool reads_source_side = false;

    QueryTreeNodes stack = {node};
    while (!stack.empty())
    {
        const auto current = stack.back();
        stack.pop_back();

        switch (current->getNodeType())
        {
            case QueryTreeNodeType::QUERY:
            case QueryTreeNodeType::UNION:
                return false;
            case QueryTreeNodeType::COLUMN:
            {
                if (current->as<ColumnNode>()->getColumnSourceOrNull() != source_side)
                    return false;
                reads_source_side = true;
                break;
            }
            case QueryTreeNodeType::FUNCTION:
            {
                const auto * function_node = current->as<FunctionNode>();
                if (!function_node->isOrdinaryFunction())
                    return false;
                const auto function_base = function_node->getFunction();
                if (!function_base || !function_base->isDeterministic())
                    return false;
                break;
            }
            default:
                break;
        }

        for (const auto & child : current->getChildren())
            if (child)
                stack.push_back(child);
    }

    return reads_source_side;
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

    /// `function_name_` is the form of the predicate to inject, `in` or `globalIn`. Empty makes the visitor
    /// only report whether a predicate could be injected, leaving the query tree untouched: the cost model
    /// that makes the shipping decision runs long after the query tree is fixed, and all it needs this
    /// early is whether the query is a candidate at all.
    ShipJoinPredicateVisitor(ContextPtr context, String function_name_)
        : Base(std::move(context))
        , function_name(std::move(function_name_))
    {
    }

    bool foundCandidate() const { return found_candidate; }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (auto * query_node = node->as<QueryNode>())
        {
            query_stack.push_back(query_node);
            return;
        }

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

        /// A conjunct of the enclosing `WHERE` restricts one side of an `INNER` join just like a conjunct
        /// of its `ON` does, but only when the whole join tree is this join: with another join above or
        /// below, whether the conjunct still implies a restriction on this side depends on the kind of
        /// every join in between. A nested join keeps its own `ON` conjuncts, which need no such argument.
        QueryTreeNodes where_conjuncts;
        if (!query_stack.empty() && query_stack.back()->getJoinTreeNode().get() == node.get() && query_stack.back()->hasWhere())
            collectConjuncts(query_stack.back()->getWhere(), where_conjuncts);

        for (const auto & conjunct : conjuncts)
        {
            const auto * equals = conjunct->as<FunctionNode>();
            if (!equals || equals->getFunctionName() != "equals" || equals->getArguments().getNodes().size() != 2)
                continue;

            const auto & lhs = equals->getArguments().getNodes()[0];
            const auto & rhs = equals->getArguments().getNodes()[1];

            /// The predicate can be pushed into either side; both are filtered by the same equality.
            tryInject(
                *join_node, lhs, rhs, join_node->getLeftTableExpressionNode(), join_node->getRightTableExpressionNode(),
                conjuncts, where_conjuncts);
            tryInject(
                *join_node, rhs, lhs, join_node->getRightTableExpressionNode(), join_node->getLeftTableExpressionNode(),
                conjuncts, where_conjuncts);
        }
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (node->as<QueryNode>())
            query_stack.pop_back();
    }

private:
    /// Push `target_key IN (SELECT source_key FROM source_side)` into `target_side` when that side is a
    /// subquery whose projection exposes the key.
    void tryInject(
        const JoinNode & join_node,
        const QueryTreeNodePtr & target_key,
        const QueryTreeNodePtr & source_key,
        const QueryTreeNodePtr & target_side,
        const QueryTreeNodePtr & source_side,
        const QueryTreeNodes & join_conjuncts,
        const QueryTreeNodes & where_conjuncts)
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

        found_candidate = true;
        if (function_name.empty())
            return;

        const auto & context = getContext();

        /// Everything else the query says about the source side belongs in the subquery too. Without it
        /// the shipped predicate is sound but weaker than the join it stands for - `ON agg.k = d.k AND
        /// d.x > 5` would ship every key of `d`, not just the keys of the rows the join can match - and
        /// the cost model that decides whether shipping pays measures the join, not the weaker predicate.
        QueryTreeNodes source_predicates;
        for (const auto & conjuncts : {std::cref(join_conjuncts), std::cref(where_conjuncts)})
            for (const auto & conjunct : conjuncts.get())
                if (restrictsSourceSideOnly(conjunct, source_side))
                    source_predicates.push_back(conjunct);

        auto in_function = std::make_shared<FunctionNode>(function_name);
        in_function->markAsOperator();
        in_function->getArguments().getNodes()
            = {projection_expression->clone(), buildKeySubquery(source_side, source_key, std::move(source_predicates), context)};
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

    const String function_name;
    bool found_candidate = false;

    /// The chain of query nodes enclosing the node being visited, innermost last.
    std::vector<QueryNode *> query_stack;
};

}

void ShipJoinPredicateToParallelReplicasPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    const auto form = context->getSettingsRef()[Setting::parallel_replicas_ship_join_predicate];
    if (form == 0)
        return;

    ShipJoinPredicateVisitor visitor(std::move(context), form == 2 ? "globalIn" : "in");
    visitor.visit(query_tree_node);
}

bool hasShippableJoinPredicate(const QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    ShipJoinPredicateVisitor visitor(std::move(context), /*function_name_=*/"");
    QueryTreeNodePtr node = query_tree_node;
    visitor.visit(node);
    return visitor.foundCandidate();
}

}
