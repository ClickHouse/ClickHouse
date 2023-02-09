#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/LambdaNode.h>

#include "ArrayExistsToHasPass.h"

namespace DB
{
namespace
{
    class RewriteArrayExistsToHasVisitor : public InDepthQueryTreeVisitorWithContext<RewriteArrayExistsToHasVisitor>
    {
    public:
        using Base = InDepthQueryTreeVisitorWithContext<RewriteArrayExistsToHasVisitor>;
        using Base::Base;

        void visitImpl(QueryTreeNodePtr & node)
        {
            if (!getSettings().optimize_rewrite_array_exists_to_has)
                return;

            auto * function_node = node->as<FunctionNode>();
            if (!function_node || function_node->getFunctionName() != "arrayExists")
                return;

            auto & function_arguments_nodes = function_node->getArguments().getNodes();
            if (function_arguments_nodes.size() != 2)
                return;

            /// lambda function must be like: x -> x = elem
            auto * lambda_node = function_arguments_nodes[0]->as<LambdaNode>();
            if (!lambda_node)
                return;

            auto & lambda_arguments_nodes = lambda_node->getArguments().getNodes();
            if (lambda_arguments_nodes.size() != 1)
                return;
            auto * column_node = lambda_arguments_nodes[0]->as<ColumnNode>();

            auto * filter_node = lambda_node->getExpression()->as<FunctionNode>();
            if (!filter_node || filter_node->getFunctionName() != "equals")
                return;

            auto filter_arguments_nodes = filter_node->getArguments().getNodes();
            if (filter_arguments_nodes.size() != 2)
                return;

            ColumnNode * filter_column_node = nullptr;
            if (filter_arguments_nodes[1]->as<ConstantNode>() && (filter_column_node = filter_arguments_nodes[0]->as<ColumnNode>())
                && filter_column_node->getColumnName() == column_node->getColumnName())
            {
                /// Rewrite arrayExists(x -> x = elem, arr) -> has(arr, elem)
                function_arguments_nodes[0] = std::move(function_arguments_nodes[1]);
                function_arguments_nodes[1] = std::move(filter_arguments_nodes[1]);
                function_node->resolveAsFunction(
                    FunctionFactory::instance().get("has", getContext())->build(function_node->getArgumentColumns()));
            }
            else if (
                filter_arguments_nodes[0]->as<ConstantNode>() && (filter_column_node = filter_arguments_nodes[1]->as<ColumnNode>())
                && filter_column_node->getColumnName() == column_node->getColumnName())
            {
                /// Rewrite arrayExists(x -> elem = x, arr) -> has(arr, elem)
                function_arguments_nodes[0] = std::move(function_arguments_nodes[1]);
                function_arguments_nodes[1] = std::move(filter_arguments_nodes[0]);
                function_node->resolveAsFunction(
                    FunctionFactory::instance().get("has", getContext())->build(function_node->getArgumentColumns()));
            }
        }
    };

}

void RewriteArrayExistsToHasPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    RewriteArrayExistsToHasVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
