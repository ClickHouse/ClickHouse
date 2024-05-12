#include <Analyzer/Passes/OptimizeL2DistancePass.h>

#include "QueryTree/Nodes/OrderByNode.h"
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
            // Проверяем, является ли текущий узел функциональным узлом.
        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        // Проверяем, является ли функция вызовом L2Distance.
        if (function_node->getFunctionName() == "L2Distance")
        {
            auto & arguments = function_node->getArguments().getNodes();
            if (arguments.size() == 2)
            {
                // Создаем узел для функции L2SquaredDistance.
                auto squared_distance_function = std::make_shared<FunctionNode>("L2SquaredDistance");
                squared_distance_function->getArguments().getNodes().insert(squared_distance_function->getArguments().getNodes().end(), arguments.begin(), arguments.end());
                squared_distance_function->resolveAsFunction(FunctionFactory::instance().get("L2SquaredDistance", context));

                // Создаем узел для функции sqrt.
                auto sqrt_function = std::make_shared<FunctionNode>("sqrt");
                sqrt_function->getArguments().getNodes().push_back(squared_distance_function);
                sqrt_function->resolveAsFunction(FunctionFactory::instance().get("sqrt", context));

                // Заменяем узел функции L2Distance на узел функции sqrt(L2SquaredDistance).
                node = sqrt_function;
            }
        }
        // Обработка вызовов функции L2Distance в секции ORDER BY.
        else if (auto * order_by_node = node->as<OrderByNode>())
        {
            auto * function_node_order_by = order_by_node->getChild()->as<FunctionNode>();
            if (function_node_order_by && function_node_order_by->getFunctionName() == "L2Distance")
            {
                auto & arguments_order_by = function_node_order_by->getArguments().getNodes();
                if (arguments_order_by.size() == 2)
                {
                    auto squared_distance_function_order_by = std::make_shared<FunctionNode>("L2SquaredDistance");
                    squared_distance_function_order_by->getArguments().getNodes().insert(squared_distance_function_order_by->getArguments().getNodes().end(), arguments_order_by.begin(), arguments_order_by.end());
                    squared_distance_function_order_by->resolveAsFunction(FunctionFactory::instance().get("L2SquaredDistance", context));

                    auto sqrt_function_order_by = std::make_shared<FunctionNode>("sqrt");
                    sqrt_function_order_by->getArguments().getNodes().push_back(squared_distance_function_order_by);
                    sqrt_function_order_by->resolveAsFunction(FunctionFactory::instance().get("sqrt", context));

                    order_by_node->setChild(sqrt_function_order_by);
                }
            }
        }
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
