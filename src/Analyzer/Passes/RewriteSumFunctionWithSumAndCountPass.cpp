#include <Analyzer/Passes/RewriteSumFunctionWithSumAndCountPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_arithmetic_operations_in_aggregate_functions;
}

namespace
{

class RewriteSumFunctionWithSumAndCountVisitor : public InDepthQueryTreeVisitorWithContext<RewriteSumFunctionWithSumAndCountVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteSumFunctionWithSumAndCountVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_arithmetic_operations_in_aggregate_functions])
            return;

        static const std::unordered_set<String> func_supported = {
            "plus",
            "minus"
        };

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || Poco::toLower(function_node->getFunctionName()) != "sum")
            return;

        const auto & function_nodes = function_node->getArguments().getNodes();
        if (function_nodes.size() != 1)
            return;

        const auto * func_plus_minus_node = function_nodes[0]->as<FunctionNode>();
        if (!func_plus_minus_node || !func_supported.contains(Poco::toLower(func_plus_minus_node->getFunctionName())))
            return;

        const auto & func_plus_minus_nodes = func_plus_minus_node->getArguments().getNodes();
        if (func_plus_minus_nodes.size() != 2)
            return;

        size_t column_id;
        if (func_plus_minus_nodes[0]->as<ColumnNode>() && func_plus_minus_nodes[1]->as<ConstantNode>())
            column_id = 0;
        else if (func_plus_minus_nodes[0]->as<ConstantNode>() && func_plus_minus_nodes[1]->as<ColumnNode>())
            column_id = 1;
        else
            return;

        size_t literal_id = 1 - column_id;
        const auto * literal = func_plus_minus_nodes[literal_id]->as<ConstantNode>();
        if (!literal)
            return;

        const auto literal_type = literal->getResultType();
        if (!literal_type || !WhichDataType(literal_type).isNumber())
            return;

        const auto * column_node = func_plus_minus_nodes[column_id]->as<ColumnNode>();
        if (!column_node)
            return;

        const auto column_type = column_node->getColumnType();
        if (!column_type || !isNumber(column_type))
            return;

        const auto lhs = std::make_shared<FunctionNode>("sum");
        lhs->getArguments().getNodes().push_back(func_plus_minus_nodes[column_id]);
        resolveAggregateFunctionNodeByName(*lhs, lhs->getFunctionName());

        const auto rhs_count = std::make_shared<FunctionNode>("count");
        rhs_count->getArguments().getNodes().push_back(func_plus_minus_nodes[column_id]);
        resolveAggregateFunctionNodeByName(*rhs_count, rhs_count->getFunctionName());

        const auto rhs = std::make_shared<FunctionNode>("multiply");
        rhs->getArguments().getNodes().push_back(func_plus_minus_nodes[literal_id]);
        rhs->getArguments().getNodes().push_back(rhs_count);
        resolveOrdinaryFunctionNodeByName(*rhs, rhs->getFunctionName(), getContext());

        auto new_node = std::make_shared<FunctionNode>(Poco::toLower(func_plus_minus_node->getFunctionName()));
        if (column_id == 0)
            new_node->getArguments().getNodes() = {lhs, rhs};
        else if (column_id == 1)
            new_node->getArguments().getNodes() = {rhs, lhs};

        resolveOrdinaryFunctionNodeByName(*new_node, new_node->getFunctionName(), getContext());

        if (!new_node)
            return;

        QueryTreeNodePtr res = std::move(new_node);

        if (!res->getResultType()->equals(*function_node->getResultType()))
            res = createCastFunction(res, function_node->getResultType(), getContext());

        node = std::move(res);
    }
};

}

void RewriteSumFunctionWithSumAndCountPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteSumFunctionWithSumAndCountVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
