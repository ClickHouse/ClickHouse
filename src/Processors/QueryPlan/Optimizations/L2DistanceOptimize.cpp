#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/Exception.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

namespace
{

const DB::DataStream & getChildOutputStream(DB::QueryPlan::Node & node)
{
    if (node.children.size() != 1)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Node \"{}\" is expected to have only one child.", node.step->getName());
    return node.children.front()->step->getOutputStream();
}

}

namespace DB::QueryPlanOptimizations {

size_t tryReplaceL2DistanceWithL2Squared(QueryPlan::Node *parent_node, QueryPlan::Nodes &nodes) {
    if (parent_node->children.size() != 1)
        return 0;

    auto *child_node = parent_node->children.front();
    auto &parent_step = parent_node->step;
    auto &child_step = child_node->step;
    auto *sorting_step = dynamic_cast<SortingStep *>(parent_step.get());
    auto *expression_step = dynamic_cast<ExpressionStep *>(child_step.get());

    if (!sorting_step || !expression_step)
        return 0;

    auto &expressions = expression_step->getExpressions();
    if (expressions.size() != 1)
        return 0;

    auto &function_expr = expressions.front();
    if (function_expr->getFunctionName() != "L2Distance")
        return 0;

    auto l2_squared_distance_function = FunctionFactory::instance().get("l2SquaredDistance");
    auto sqrt_function = FunctionFactory::instance().get("sqrt");

    if (!l2_squared_distance_function || !sqrt_function)
        return 0;

    auto sqrt_l2_squared_distance = std::make_shared<FunctionExpression>(sqrt_function, function_expr->getArguments()->clone());
    auto l2_squared_distance = std::make_shared<FunctionExpression>(l2_squared_distance_function, function_expr->getArguments()->clone());

    auto &node_with_l2_squared = nodes.emplace_back();
    std::swap(node_with_l2_squared.children, child_node->children);
    child_node->children = {&node_with_l2_squared};

    node_with_l2_squared.step = std::make_unique<ExpressionStep>(child_step->getOutputStream(), std::move(l2_squared_distance));
    node_with_l2_squared.step->setStepDescription(child_step->getStepDescription());

    sorting_step->updateInputStream(node_with_l2_squared.step->getOutputStream());

    return 3;
}

}
