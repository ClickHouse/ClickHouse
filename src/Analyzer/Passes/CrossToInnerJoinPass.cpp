#include <Analyzer/Passes/CrossToInnerJoinPass.h>

#include <DataTypes/getLeastSupertype.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

namespace
{

void exctractJoinConditions(const QueryTreeNodePtr & node, QueryTreeNodes & equi_conditions, QueryTreeNodes & other)
{
    auto * func = node->as<FunctionNode>();
    if (!func)
    {
        other.push_back(node);
        return;
    }

    const auto & args = func->getArguments().getNodes();

    if (args.size() == 2 && func->getFunctionName() == "equals")
    {
        equi_conditions.push_back(node);
    }
    else if (func->getFunctionName() == "and")
    {
        for (const auto & arg : args)
            exctractJoinConditions(arg, equi_conditions, other);
    }
    else
    {
        other.push_back(node);
    }
}

const QueryTreeNodePtr & getEquiArgument(const QueryTreeNodePtr & cond, size_t index)
{
    const auto * func = cond->as<FunctionNode>();
    chassert(func && func->getFunctionName() == "equals" && func->getArguments().getNodes().size() == 2);
    return func->getArguments().getNodes()[index];
}


/// Check that node has only one source and return it.
/// {_, false} - multiple sources
/// {nullptr, true} - no sources
/// {source, true} - single source
std::pair<const IQueryTreeNode *, bool> getExpressionSource(const QueryTreeNodePtr & node)
{
    if (const auto * column = node->as<ColumnNode>())
    {
        auto source = column->getColumnSourceOrNull();
        if (!source)
            return {nullptr, false};
        return {source.get(), true};
    }

    if (const auto * func = node->as<FunctionNode>())
    {
        const IQueryTreeNode * source = nullptr;
        const auto & args = func->getArguments().getNodes();
        for (const auto & arg : args)
        {
            auto [arg_source, is_ok] = getExpressionSource(arg);
            if (!is_ok)
                return {nullptr, false};

            if (!source)
                source = arg_source;
            else if (arg_source && !source->isEqual(*arg_source))
                return {nullptr, false};
        }
        return {source, true};

    }

    if (node->as<ConstantNode>())
        return {nullptr, true};

    return {nullptr, false};
}

bool findInTableExpression(const IQueryTreeNode * source, const QueryTreeNodePtr & table_expression)
{
    if (!source)
        return true;

    if (source->isEqual(*table_expression))
        return true;

    if (const auto * join_node = table_expression->as<JoinNode>())
    {
        return findInTableExpression(source, join_node->getLeftTableExpression())
            || findInTableExpression(source, join_node->getRightTableExpression());
    }


    return false;
}

void getJoinNodes(QueryTreeNodePtr & join_tree_node, std::vector<JoinNode *> & join_nodes)
{
    auto * join_node = join_tree_node->as<JoinNode>();
    if (!join_node)
        return;

    if (!isCrossOrComma(join_node->getKind()))
        return;

    join_nodes.push_back(join_node);
    getJoinNodes(join_node->getLeftTableExpression(), join_nodes);
    getJoinNodes(join_node->getRightTableExpression(), join_nodes);
}

class CrossToInnerJoinVisitor : public InDepthQueryTreeVisitorWithContext<CrossToInnerJoinVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CrossToInnerJoinVisitor>;
    using Base::Base;

    /// Returns false if can't rewrite cross to inner join
    bool tryRewrite(JoinNode & join_node, QueryTreeNodePtr & where_condition)
    {
        if (!isCrossOrComma(join_node.getKind()))
            return false;

        if (!where_condition)
            return false;

        const auto & left_table = join_node.getLeftTableExpression();
        const auto & right_table = join_node.getRightTableExpression();

        QueryTreeNodes equi_conditions;
        QueryTreeNodes other_conditions;
        exctractJoinConditions(where_condition, equi_conditions, other_conditions);
        bool can_convert_cross_to_inner = false;
        for (auto & condition : equi_conditions)
        {
            const auto & lhs_equi_argument = getEquiArgument(condition, 0);
            const auto & rhs_equi_argument = getEquiArgument(condition, 1);

            DataTypes key_types = {lhs_equi_argument->getResultType(), rhs_equi_argument->getResultType()};
            DataTypePtr common_key_type = tryGetLeastSupertype(key_types);

            /// If there is common key type, we can join on this condition
            if (common_key_type)
            {
                auto left_src = getExpressionSource(lhs_equi_argument);
                auto right_src = getExpressionSource(rhs_equi_argument);

                if (left_src.second && right_src.second && left_src.first && right_src.first)
                {
                    if ((findInTableExpression(left_src.first, left_table) && findInTableExpression(right_src.first, right_table)) ||
                        (findInTableExpression(left_src.first, right_table) && findInTableExpression(right_src.first, left_table)))
                    {
                        can_convert_cross_to_inner = true;
                        continue;
                    }
                }
            }

            /// Can't join on this condition, move it to other conditions
            other_conditions.push_back(condition);
            condition = nullptr;
        }

        if (!can_convert_cross_to_inner)
            return false;

        equi_conditions.erase(std::remove(equi_conditions.begin(), equi_conditions.end(), nullptr), equi_conditions.end());
        join_node.crossToInner(makeConjunction(equi_conditions));
        where_condition = makeConjunction(other_conditions);
        return true;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!isEnabled())
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        auto & where_node = query_node->getWhere();
        if (!where_node)
            return;

        auto & join_tree_node = query_node->getJoinTree();
        if (!join_tree_node || join_tree_node->getNodeType() != QueryTreeNodeType::JOIN)
            return;

        /// In case of multiple joins, we can try to rewrite all of them
        /// Example: SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t2.a = t3.a
        std::vector<JoinNode *> join_nodes;
        getJoinNodes(join_tree_node, join_nodes);

        for (auto * join_node : join_nodes)
        {
            bool is_rewritten = tryRewrite(*join_node, where_node);

            if (!is_rewritten && forceRewrite(join_node->getKind()))
            {
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "Failed to rewrite '{}' to INNER JOIN: "
                    "no equi-join conditions found in WHERE clause. "
                    "You may set setting `cross_to_inner_join_rewrite` to `1` to allow slow CROSS JOIN for this case",
                    join_node->formatASTForErrorMessage());
            }
        }
    }

private:
    bool isEnabled() const
    {
        return getSettings().cross_to_inner_join_rewrite;
    }

    bool forceRewrite(JoinKind kind) const
    {
        if (kind == JoinKind::Cross)
            return false;
        /// Comma join can be forced to rewrite
        return getSettings().cross_to_inner_join_rewrite >= 2;
    }

    QueryTreeNodePtr makeConjunction(const QueryTreeNodes & nodes)
    {
        if (nodes.empty())
            return nullptr;

        if (nodes.size() == 1)
            return nodes.front();

        auto function_node = std::make_shared<FunctionNode>("and");
        for (const auto & node : nodes)
            function_node->getArguments().getNodes().push_back(node);

        const auto & function = FunctionFactory::instance().get("and", getContext());
        function_node->resolveAsFunction(function->build(function_node->getArgumentColumns()));
        return function_node;
    }
};

}

void CrossToInnerJoinPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    CrossToInnerJoinVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
