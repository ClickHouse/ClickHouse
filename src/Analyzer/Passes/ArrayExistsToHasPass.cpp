#include <Analyzer/Passes/ArrayExistsToHasPass.h>

#include <Functions/FunctionFactory.h>
#include <Functions/array/has.h>

#include <Interpreters/Context.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/LambdaNode.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_array_exists_to_has;
}

namespace
{

class RewriteArrayExistsToHasVisitor : public InDepthQueryTreeVisitorWithContext<RewriteArrayExistsToHasVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteArrayExistsToHasVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_array_exists_to_has])
            return;

        auto * array_exists_function_node = node->as<FunctionNode>();
        if (!array_exists_function_node || array_exists_function_node->getFunctionName() != "arrayExists")
            return;

        auto & array_exists_function_arguments_nodes = array_exists_function_node->getArguments().getNodes();
        if (array_exists_function_arguments_nodes.size() != 2)
            return;

        /// lambda function must be like: x -> x = elem
        auto * lambda_node = array_exists_function_arguments_nodes[0]->as<LambdaNode>();
        if (!lambda_node)
            return;

        auto & lambda_arguments_nodes = lambda_node->getArguments().getNodes();
        if (lambda_arguments_nodes.size() != 1)
            return;

        const auto & lambda_argument_column_node = lambda_arguments_nodes[0];
        if (lambda_argument_column_node->getNodeType() != QueryTreeNodeType::COLUMN)
            return;

        auto * filter_node = lambda_node->getExpression()->as<FunctionNode>();
        if (!filter_node || filter_node->getFunctionName() != "equals")
            return;

        const auto & filter_arguments_nodes = filter_node->getArguments().getNodes();
        if (filter_arguments_nodes.size() != 2)
            return;

        const auto & filter_lhs_argument_node = filter_arguments_nodes[0];
        auto filter_lhs_argument_node_type = filter_lhs_argument_node->getNodeType();

        const auto & filter_rhs_argument_node = filter_arguments_nodes[1];
        auto filter_rhs_argument_node_type = filter_rhs_argument_node->getNodeType();

        QueryTreeNodePtr has_constant_element_argument;

        if (filter_lhs_argument_node_type == QueryTreeNodeType::COLUMN &&
            filter_rhs_argument_node_type == QueryTreeNodeType::CONSTANT &&
            filter_lhs_argument_node->isEqual(*lambda_argument_column_node))
        {
            /// Rewrite arrayExists(x -> x = elem, arr) -> has(arr, elem)
            has_constant_element_argument = filter_rhs_argument_node;
        }
        else if (filter_lhs_argument_node_type == QueryTreeNodeType::CONSTANT &&
            filter_rhs_argument_node_type == QueryTreeNodeType::COLUMN &&
            filter_rhs_argument_node->isEqual(*lambda_argument_column_node))
        {
            /// Rewrite arrayExists(x -> elem = x, arr) -> has(arr, elem)
            has_constant_element_argument = filter_lhs_argument_node;
        }
        else
        {
            return;
        }

        auto has_function = createInternalFunctionHasOverloadResolver();

        array_exists_function_arguments_nodes[0] = std::move(array_exists_function_arguments_nodes[1]);
        array_exists_function_arguments_nodes[1] = std::move(has_constant_element_argument);
        array_exists_function_node->resolveAsFunction(has_function->build(array_exists_function_node->getArgumentColumns()));
    }
};

}

void RewriteArrayExistsToHasPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteArrayExistsToHasVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
