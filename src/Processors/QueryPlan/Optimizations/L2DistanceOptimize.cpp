#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace QueryPlanOptimizations
{

size_t tryReplaceL2DistanceWithL2Squared(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes)
{
    if (!parent_node || !parent_node->step)
        return 0;

    auto * expression_step = dynamic_cast<ExpressionStep *>(parent_node->step.get());
    if (!expression_step)
        return 0;

    auto & actions = expression_step->getActions();
    for (auto & action : actions)
    {
        auto * function = dynamic_cast<FunctionExpression *>(action.get());
        if (!function)
            continue;

        if (function->getName() == "L2Distance")
        {
            // Replace L2Distance with sqrt(L2SquaredDistance)
            auto l2_squared_distance_function = FunctionFactory::instance().get("L2SquaredDistance");
            if (!l2_squared_distance_function)
                throw Exception("L2SquaredDistance function not found", ErrorCodes::LOGICAL_ERROR);

            auto sqrt_function = FunctionFactory::instance().get("sqrt");
            if (!sqrt_function)
                throw Exception("sqrt function not found", ErrorCodes::LOGICAL_ERROR);

            auto sqrt_l2_squared_distance = std::make_shared<FunctionExpression>(sqrt_function);
            sqrt_l2_squared_distance->addChild(function->arguments->clone());
            auto l2_squared_distance = std::make_shared<FunctionExpression>(l2_squared_distance_function);
            l2_squared_distance->addChild(function->arguments->clone());

            action = sqrt_l2_squared_distance;
            nodes.emplace_back(QueryPlan::Node{std::make_unique<ExpressionStep>(l2_squared_distance)});
            return 1;
        }
    }

    return 0;
}

}

}
