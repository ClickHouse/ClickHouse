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

namespace DB::QueryPlanOptimizations
{
size_t tryReplaceL2DistanceWithL2Squared(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes) {
    // Проверяем, является ли узел ExpressionStep, содержащий вызов L2Distance
    if (auto *expression_node = typeid_cast<ExpressionStep *>(parent_node->node)) {
        if (expression_node->expression->getFunctionName() == "L2Distance") {
            // Заменяем вызов L2Distance на sqrt(L2SquaredDistance)
            auto l2_distance_args = std::move(expression_node->children.front()->children);
            auto l2_squared_distance = std::make_shared<ExpressionFunction>("L2SquaredDistance", l2_distance_args);
            auto sqrt_function = std::make_shared<ExpressionFunction>("sqrt", ExpressionActions::Actions{{}, l2_squared_distance});
            expression_node->expression = sqrt_function;

            // Возвращаем 1, чтобы указать, что мы внесли изменения
            return 1;
        }
    }
    return 0; // Не внесли изменений
}

}
