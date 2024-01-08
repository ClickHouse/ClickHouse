#include <Analyzer/Passes/RewriteSumFunctionWithSumAndCountPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

/** Rewrite the following AST to break the function `sum(column + literal)` into two individual functions
 * `sum(column)` and `literal * count(column)`.
 *  sum(column + literal)  ->  sum(column) + literal * count(column)
 *  sum(literal + column)  ->  sum(column) + literal * count(column)
 *  sum(column - literal)  ->  sum(column) - literal * count(column)
 *  sum(literal - column)  ->  sum(column) - literal * count(column)
 */

namespace
{

class RewriteSumFunctionWithSumAndCountVisitor : public InDepthQueryTreeVisitorWithContext<RewriteSumFunctionWithSumAndCountVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteSumFunctionWithSumAndCountVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        static const std::unordered_set<String> nested_func_supported = {
            "plus",
            "minus"
        };

        const auto * function = node->as<FunctionNode>();
        if (!function || Poco::toLower(function->getFunctionName()) != "sum")
            return;

        auto & func_node = function->getArguments().getNodes();
        if (func_node.size() != 1)
            return;

        const auto * nested_func = func_node[0]->as<FunctionNode>();
        if (!nested_func || !nested_func_supported.contains(Poco::toLower(nested_func->getFunctionName())))
            return;

        auto & nested_func_node = nested_func->getArguments().getNodes();
        if (nested_func_node.size() != 2)
            return;

        size_t column_id = nested_func_node.size();
        for (size_t i = 0; i < nested_func_node.size(); i++)
        {
            if (const auto * column_node = nested_func_node[i]->as<ColumnNode>())
                column_id = i;
        }
        if (column_id == nested_func_node.size())
            return;

        size_t literal_id = 1 - column_id;
        const auto * literal = nested_func_node[literal_id]->as<ConstantNode>();
        if (!literal || !WhichDataType(literal->getResultType()).isNumber())
            return;

        const auto * column_node = nested_func_node[column_id]->as<ColumnNode>();
        if (!column_node)
            return;

        const auto column_type = column_node->getColumnType();
        if (!column_type || !isNumber(column_type))
            return;

        auto column_name = column_node->getColumnName();

        const auto lhs = std::make_shared<FunctionNode>("sum");
        lhs->getArguments().getNodes().push_back(std::make_shared<ColumnNode>(column_node->getColumn(), column_node->getColumnSource()));
        resolveAggregateFunctionNode(*lhs, lhs->getArguments().getNodes()[0], lhs->getFunctionName());

        const auto rhs_nested_right = std::make_shared<FunctionNode>("count");
        rhs_nested_right->getArguments().getNodes().push_back(std::make_shared<ColumnNode>(column_node->getColumn(), column_node->getColumnSource()));
        resolveAggregateFunctionNode(*rhs_nested_right, rhs_nested_right->getArguments().getNodes()[0], rhs_nested_right->getFunctionName());

        const auto rhs = std::make_shared<FunctionNode>("multiply");
        rhs->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(literal));
        rhs->getArguments().getNodes().push_back(rhs_nested_right);
        resolveOrdinaryFunctionNode(*rhs, rhs->getFunctionName());

        const auto new_node = std::make_shared<FunctionNode>("plus");

        if (column_id == 0)
            new_node->getArguments().getNodes() = {lhs, rhs};
        else if (column_id == 1)
            new_node->getArguments().getNodes() = {rhs, lhs};

        resolveOrdinaryFunctionNode(*new_node, new_node->getFunctionName());

        if (!new_node)
            return;

        node = new_node;
    }

private:
    void resolveOrdinaryFunctionNode(FunctionNode & function_node, const String & function_name) const
    {
        auto function = FunctionFactory::instance().get(function_name, getContext());
        function_node.resolveAsFunction(function->build(function_node.getArgumentColumns()));
    }

    static inline void resolveAggregateFunctionNode(FunctionNode & function_node, const QueryTreeNodePtr & argument, const String & aggregate_function_name)
    {
        AggregateFunctionProperties properties;
        auto aggregate_function = AggregateFunctionFactory::instance().get(aggregate_function_name,
            { argument->getResultType() },
            {},
            properties);

        function_node.resolveAsAggregateFunction(std::move(aggregate_function));
    }

};

}

void RewriteSumFunctionWithSumAndCountPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    RewriteSumFunctionWithSumAndCountVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
