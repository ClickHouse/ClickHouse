// #include <Interpreters/ActionsDAG.h>
// #include <Processors/QueryPlan/ExpressionStep.h>
// #include <Processors/QueryPlan/FillingStep.h>
// #include <Processors/QueryPlan/Optimizations/Optimizations.h>
// #include <Processors/QueryPlan/SortingStep.h>
// #include <Common/Exception.h>
// #include <Functions/FunctionFactory.h>
// #include <Functions/IFunction.h>

// namespace
// {

// const DB::DataStream & getChildOutputStream(DB::QueryPlan::Node & node)
// {
//     if (node.children.size() != 1)
//         throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Node \"{}\" is expected to have only one child.", node.step->getName());
//     return node.children.front()->step->getOutputStream();
// }

// }

// namespace DB::QueryPlanOptimizations {

// size_t tryReplaceL2DistanceWithL2Squared(QueryPlan::Node *parent_node, QueryPlan::Nodes &nodes) {
//     if (!parent_node || parent_node->children.empty())
//         return 0;

//     auto & children = parent_node->children;

//     for (size_t i = 0; i < children.size(); ++i)
//     {
//         auto & child = children[i];
        
//         // If the child node is of type ExpressionStep and represents L2Distance function
//         if (child->type == QueryPlanNodeType::ExpressionStep &&
//             child->expression == "L2Distance")
//         {
//             // Replace L2Distance with sqrt(L2SquaredDistance)
//             child->expression = "sqrt";
//             child->arguments.clear();
//             child->arguments.emplace_back("L2SquaredDistance");
//             // No need to continue the loop as we've made the replacement
//             return 1;
//         }
        
//         // Recursively apply the optimization to the child nodes
//         if (tryReplaceL2DistanceWithL2Squared(child->get(), nodes) > 0)
//             return 1;
//     }

//     return 0;
// }

// }
