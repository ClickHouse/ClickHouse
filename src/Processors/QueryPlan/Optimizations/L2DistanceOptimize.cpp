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
    auto *sorting_step = typeid_cast<SortingStep *>(parent_step.get());
    auto *expression_step = typeid_cast<ExpressionStep *>(child_step.get());

    if (!sorting_step || !expression_step)
        return 0;

    auto actions = expression_step->getActions();
    auto *function_expr = dynamic_cast<FunctionExpression *>(actions.back().get());

    if (!function_expr || function_expr->functionName() != "L2Distance")
        return 0;

    auto *l2_squared_distance_function = FunctionFactory::instance().tryGet("l2SquaredDistance", {});
    auto *sqrt_function = FunctionFactory::instance().tryGet("sqrt", {});

    auto sqrt_l2_squared_distance = FunctionFactory::instance().build("sqrt", {function_expr->arguments()->clone()}, {});
    auto l2_squared_distance = FunctionFactory::instance().build("l2SquaredDistance", {function_expr->arguments()->clone()}, {});

    expression_step->replaceAction(actions.size() - 1, std::move(sqrt_l2_squared_distance));

    auto &node_with_l2_squared = nodes.emplace_back();
    std::swap(node_with_l2_squared.children, child_node->children);
    child_node->children = {&node_with_l2_squared};

    node_with_l2_squared.step = std::make_unique<ExpressionStep>(child_step->getOutputStream(), std::move(l2_squared_distance));
    node_with_l2_squared.step->setStepDescription(child_step->getStepDescription());

    sorting_step->updateInputStream(node_with_l2_squared.step->getOutputStream());

    return 3;
}

} // namespace DB::QueryPlanOptimizations
