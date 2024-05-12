#include <Analyzer/Passes/OptimizeL2DistancePass.h>
#include <DataTypes/DataTypesNumber.h>
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

    explicit L2DistanceOptimizationPassVisitor(ContextPtr ctx)
        : Base(std::move(ctx)), context(ctx)
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "L2Distance")
            return;

        auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 2)
            return;

        auto squared_distance_function = std::make_shared<FunctionNode>("L2SquaredDistance");
        squared_distance_function->getArguments().getNodes().insert(squared_distance_function->getArguments().getNodes().end(), arguments.begin(), arguments.end());
        squared_distance_function->resolveAsFunction(FunctionFactory::instance().get("L2SquaredDistance", context));

        auto sqrt_function = std::make_shared<FunctionNode>("sqrt");
        sqrt_function->getArguments().getNodes().push_back(squared_distance_function);
        sqrt_function->resolveAsFunction(FunctionFactory::instance().get("sqrt", context));

        node = sqrt_function;
    }

private:
    ContextPtr context;
};

}

void L2DistanceOptimizationPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    L2DistanceOptimizationPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}
}
