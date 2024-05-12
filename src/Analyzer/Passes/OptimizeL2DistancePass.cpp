#include <Analyzer/Passes/OptimizeL2DistancePass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

class L2DistanceOptimizationPassVisitor : public InDepthQueryTreeVisitorWithContext<L2DistanceOptimizationPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<L2DistanceOptimizationPassVisitor>;
    using Base::Base;

    explicit L2DistanceOptimizationPassVisitor(ContextPtr context)
        : Base(std::move(context))
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "L2Distance")
            return;

        auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 2)
            return;

        auto vec_arg = arguments[1]->toString();
        auto new_function_name = "sqrt";
        auto new_function_arg = std::make_shared<FunctionNode>("L2SquaredDistance");
        new_function_arg->addChild(arguments[0]);
        new_function_arg->addChild(arguments[1]);
        
        auto new_function_node = std::make_shared<FunctionNode>(new_function_name);
        new_function_node->addChild(new_function_arg);

        node = std::move(new_function_node);
    }
};

}

void L2DistanceOptimizationPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    L2DistanceOptimizationPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
