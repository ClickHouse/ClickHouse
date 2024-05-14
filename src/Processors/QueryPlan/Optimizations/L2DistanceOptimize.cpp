#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/Exception.h>
#include <DataTypes/IDataType.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

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

    // Получаем действия из шага
    auto & actions = expression_step->actions;
    for (auto & action : actions)
    {
        // Проверяем, является ли действие вызовом функции
        auto * function = dynamic_cast<FunctionExpression *>(action.get());
        if (!function)
            continue;

        if (function->getName() == "L2Distance")
        {
            // Заменяем вызов L2Distance на sqrt(L2SquaredDistance)
            auto l2_squared_distance_function = FunctionFactory::instance().get("L2SquaredDistance", nullptr); // Добавляем второй аргумент
            if (!l2_squared_distance_function)
                throw Exception("L2SquaredDistance function not found", ErrorCodes::LOGICAL_ERROR);

            auto sqrt_function = FunctionFactory::instance().get("sqrt", nullptr); // Добавляем второй аргумент
            if (!sqrt_function)
                throw Exception("sqrt function not found", ErrorCodes::LOGICAL_ERROR);

            // Создаем выражения для вызова sqrt(L2SquaredDistance) и L2SquaredDistance с аргументами
            auto sqrt_l2_squared_distance = FunctionHelpers::buildFunction({function->arguments->clone()}, sqrt_function);
            auto l2_squared_distance = FunctionHelpers::buildFunction({function->arguments->clone()}, l2_squared_distance_function);

            // Заменяем текущее действие на sqrt(L2SquaredDistance)
            action = sqrt_l2_squared_distance;
            // Добавляем новый узел для L2SquaredDistance
            nodes.emplace_back(QueryPlan::Node{std::make_unique<ExpressionStep>(l2_squared_distance)});
            return 1;
        }
    }

    return 0;
}

}

}
