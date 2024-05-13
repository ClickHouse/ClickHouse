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
    if (!parent_node)
        return 0;

    size_t num_changes = 0;

    // Check if the parent node is a FunctionStep and if it calls L2Distance function
    auto * function_step = parent_node->as<FunctionStep>();
    if (!function_step || function_step->getFunctionName() != "L2Distance")
        return num_changes;

    // Replace L2Distance function with sqrt(L2SquaredDistance(...))
    auto & arguments = function_step->getArguments();
    if (arguments.size() != 2)
        return num_changes;

    auto squared_distance_function = std::make_shared<FunctionNode>("L2SquaredDistance");
    squared_distance_function->getArguments().getNodes().insert(squared_distance_function->getArguments().getNodes().end(), arguments.begin(), arguments.end());
    squared_distance_function->resolveAsFunction(FunctionFactory::instance().get("L2SquaredDistance"));

    auto sqrt_function = std::make_shared<FunctionNode>("sqrt");
    sqrt_function->getArguments().getNodes().push_back(squared_distance_function);
    sqrt_function->resolveAsFunction(FunctionFactory::instance().get("sqrt"));

    // Replace the FunctionStep node with the new sqrt(L2SquaredDistance(...)) function node
    function_step->setFunctionName("sqrt");
    function_step->setArguments(std::move(sqrt_function->getArguments()));

    // Update the node in the list of nodes
    for (auto & node : nodes)
    {
        if (node.get() == function_step)
        {
            node = std::move(sqrt_function);
            break;
        }
    }

    num_changes++;

    return num_changes;
}

}

}