#include "AnyFunctionPass.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>

namespace DB
{

namespace
{

class AnyFunctionVisitor : public InDepthQueryTreeVisitorWithContext<AnyFunctionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<AnyFunctionVisitor>;
    using Base::Base;

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_move_functions_out_of_any)
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        /// check function is any
        const auto & function_name = function_node->getFunctionName();
        if (!(function_name == "any" || function_name == "anyLast"))
            return;

        auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 1)
            return;

        auto * inside_function_node = arguments[0]->as<FunctionNode>();

        /// check argument is a function and can not be arrayJoin or lambda
        if (!inside_function_node || inside_function_node->getFunctionName() == "arrayJoin"
            || inside_function_node->getFunctionName() == "lambda")
            return;

        auto & inside_arguments = inside_function_node->getArguments().getNodes();

        /// case any(f())
        if (inside_arguments.empty())
            return;

        if (rewritten.count(node.get()))
        {
            node = rewritten.at(node.get());
            return;
        }

        /// checking done, rewrite function
        bool pushed = false;
        for (auto & inside_argument : inside_arguments)
        {
            if (inside_argument->as<ConstantNode>()) /// skip constant node
                break;

            AggregateFunctionProperties properties;
            auto aggregate_function = AggregateFunctionFactory::instance().get(function_name, {inside_argument->getResultType()}, {}, properties);

            auto any_function = std::make_shared<FunctionNode>(function_name);
            any_function->resolveAsAggregateFunction(std::move(aggregate_function));

            auto & any_function_arguments = any_function->getArguments().getNodes();
            any_function_arguments.push_back(std::move(inside_argument));

            inside_argument = std::move(any_function);
            pushed = true;
        }

        if (pushed)
        {
            rewritten.insert({node.get(), arguments[0]});
            node = arguments[0];
        }
    }

private:
    /// After query analysis alias will be rewritten to QueryTreeNode
    /// whose memory address is same with the original one.
    /// So we can reuse the rewritten one.
    std::unordered_map<IQueryTreeNode *, QueryTreeNodePtr > rewritten;
};

}

void AnyFunctionPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    AnyFunctionVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
