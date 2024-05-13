#include <Processors/QueryPlan/ExpressionStep.h>
#include <Functions/FunctionFactory.h>
#include <Common/typeid_cast.h>

size_t tryReplaceL2DistanceWithL2Squared(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes)
{
    // Проверяем, является ли родительский узел функцией L2Distance
    auto * expression_step = typeid_cast<ExpressionStep *>(parent_node->step.get());
    if (!expression_step)
        return 0;

    const auto & function = expression_step->getExpression()->function;

    if (function->getName() != "L2Distance")
        return 0;

    // Создаем новую функцию sqrt(L2SquaredDistance)
    auto sqrt_function = FunctionFactory::instance().get("sqrt", {});

    std::vector<ColumnWithTypeAndName> arguments;
    arguments.emplace_back(expression_step->getOutputStream(), "");

    auto l2_squared_function = FunctionFactory::instance().get("L2SquaredDistance", {});

    auto sqrt_expression = std::make_shared<ASTFunction>();
    sqrt_expression->name = sqrt_function->getName();
    sqrt_expression->arguments = arguments;

    auto l2_squared_expression = std::make_shared<ASTFunction>();
    l2_squared_expression->name = l2_squared_function->getName();
    l2_squared_expression->arguments = arguments;

    auto sqrt_l2_squared_expression = std::make_shared<ASTFunction>();
    sqrt_l2_squared_expression->name = sqrt_function->getName();
    sqrt_l2_squared_expression->arguments.emplace_back(l2_squared_expression);

    // Обновляем выражение в родительском узле
    expression_step->getExpression()->function = sqrt_l2_squared_expression;

    // Обновляем информацию о функции sqrt как монотонной
    auto & settings = parent_node->settings;
    settings.is_function_monotonic[sqrt_function->getName()] = true;

    return 1;
}
