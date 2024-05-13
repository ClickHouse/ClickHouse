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

namespace DB
{

namespace QueryPlanOptimizations
{

size_t tryReplaceL2DistanceWithL2Squared(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes)
{
    if (!parent_node->as<QueryPlan::ExpressionStep>())
        return 0;

    auto & expression_step = parent_node->as<QueryPlan::ExpressionStep>();

    // Check if the expression is a call to L2Distance
    if (expression_step.function_base->name != "L2Distance")
        return 0;

    // Replace the function name with L2SquaredDistance
    expression_step.function_base->name = "L2SquaredDistance";

    // Add sqrt function as a parent of L2SquaredDistance
    auto sqrt_function = std::make_shared<Function>("sqrt", FunctionFactory::instance());
    auto l2_squared_distance_function = expression_step.function_base;
    expression_step.function_base = sqrt_function;

    auto l2_squared_distance_argument = l2_squared_distance_function->arguments[0];
    l2_squared_distance_function->arguments.clear();
    sqrt_function->arguments.push_back(l2_squared_distance_argument);

    // Add L2SquaredDistance as an argument to the sqrt function
    sqrt_function->arguments.push_back(l2_squared_distance_function);

    return 1;
}

}

}