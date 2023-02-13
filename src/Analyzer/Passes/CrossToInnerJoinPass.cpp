#include <Analyzer/Passes/CrossToInnerJoinPass.h>

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

using EquiCondition = std::tuple<QueryTreeNodePtr, QueryTreeNodePtr>;

void exctractJoinConditions(const QueryTreeNodePtr & node, QueryTreeNodes & equi_conditions, QueryTreeNodes & other)
{
    if (auto * func = node->as<FunctionNode>())
    {
        const auto & args = func->getArguments().getNodes();

        if (args.size() == 2 && func->getFunctionName() == "equals")
        {
            equi_conditions.push_back(node);
            return;
        }

        if (func->getFunctionName() == "and")
        {
            for (auto & arg : args)
                exctractJoinConditions(arg, equi_conditions, other);
            return;
        }
    }

    other.push_back(node);
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
        for (auto & arg : args)
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

    if (const auto * query_node = table_expression->as<QueryNode>())
    {
        return findInTableExpression(source, query_node->getJoinTree());
    }

    return false;
}

class CrossToInnerJoinVisitor : public InDepthQueryTreeVisitorWithContext<CrossToInnerJoinVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CrossToInnerJoinVisitor>;
    using Base::Base;

    /// Returns false if can't rewrite cross to inner join
    bool tryRewrite(JoinNode * join_node)
    {
        if (!isCrossOrComma(join_node->getKind()))
            return true;

        if (where_stack.empty())
            return false;

        auto & where_condition = *where_stack.back();
        if (!where_condition)
            return false;

        const auto & left_table = join_node->getLeftTableExpression();
        const auto & right_table = join_node->getRightTableExpression();

        QueryTreeNodes equi_conditions;
        QueryTreeNodes other_conditions;
        exctractJoinConditions(where_condition, equi_conditions, other_conditions);
        bool can_join_on_anything = false;
        for (auto & cond : equi_conditions)
        {
            auto left_src = getExpressionSource(getEquiArgument(cond, 0));
            auto right_src = getExpressionSource(getEquiArgument(cond, 1));
            if (left_src.second && right_src.second && left_src.first && right_src.first)
            {
                bool can_join_on = (findInTableExpression(left_src.first, left_table) && findInTableExpression(right_src.first, right_table))
                    || (findInTableExpression(left_src.first, right_table) && findInTableExpression(right_src.first, left_table));

                if (can_join_on)
                {
                    can_join_on_anything = true;
                    continue;
                }
            }

            /// Can't join on this condition, move it to other conditions
            other_conditions.push_back(cond);
            cond = nullptr;
        }

        if (!can_join_on_anything)
            return false;

        equi_conditions.erase(std::remove(equi_conditions.begin(), equi_conditions.end(), nullptr), equi_conditions.end());
        join_node->crossToInner(makeConjunction(equi_conditions));
        where_condition = makeConjunction(other_conditions);
        return true;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (!isEnabled())
            return;

        if (auto * query_node = node->as<QueryNode>())
        {
            /// We are entering the subtree and can use WHERE condition from this subtree
            if (auto & where_node = query_node->getWhere())
                where_stack.push_back(&where_node);
        }

        if (auto * join_node = node->as<JoinNode>())
        {
            bool is_rewritten = tryRewrite(join_node);
            if (!is_rewritten && forceRewrite(join_node->getKind()))
            {
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "Failed to rewrite '{}' to INNER JOIN: "
                    "no equi-join conditions found in WHERE clause. "
                    "You may set setting `cross_to_inner_join_rewrite` to `1` to allow slow CROSS JOIN for this case",
                    join_node->formatASTForErrorMessage());
            }
        }

        if (!where_stack.empty() && where_stack.back()->get() == node.get())
        {
            /// We are visiting the WHERE clause.
            /// It means that we have visited current subtree and will go out of WHERE scope.
            where_stack.pop_back();
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
        for (auto & node : nodes)
            function_node->getArguments().getNodes().push_back(node);

        const auto & function = FunctionFactory::instance().get("and", getContext());
        function_node->resolveAsFunction(function->build(function_node->getArgumentColumns()));
        return function_node;
    }

    std::deque<QueryTreeNodePtr *> where_stack;
};

}

void CrossToInnerJoinPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    CrossToInnerJoinVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
