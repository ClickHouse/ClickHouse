#include <Planner/PlannerAggregation.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>

#include <Planner/PlannerActionsVisitor.h>

namespace DB
{

AggregateDescriptions extractAggregateDescriptions(const QueryTreeNodes & aggregate_function_nodes, const PlannerContext & planner_context)
{
    QueryTreeNodeToName node_to_name;
    NameSet unique_aggregate_action_node_names;
    AggregateDescriptions aggregate_descriptions;

    for (const auto & aggregate_function_node : aggregate_function_nodes)
    {
        const auto & aggregate_function_node_typed = aggregate_function_node->as<FunctionNode &>();
        String node_name = calculateActionNodeName(aggregate_function_node, planner_context, node_to_name);
        auto [_, inserted] = unique_aggregate_action_node_names.emplace(node_name);
        if (!inserted)
            continue;

        AggregateDescription aggregate_description;
        aggregate_description.function = aggregate_function_node_typed.getAggregateFunction();

        const auto & parameters_nodes = aggregate_function_node_typed.getParameters().getNodes();
        aggregate_description.parameters.reserve(parameters_nodes.size());

        for (const auto & parameter_node : parameters_nodes)
        {
            /// Function parameters constness validated during analysis stage
            aggregate_description.parameters.push_back(parameter_node->as<ConstantNode &>().getValue());
        }

        const auto & arguments_nodes = aggregate_function_node_typed.getArguments().getNodes();
        aggregate_description.argument_names.reserve(arguments_nodes.size());

        for (const auto & argument_node : arguments_nodes)
        {
            String argument_node_name = calculateActionNodeName(argument_node, planner_context, node_to_name);
            aggregate_description.argument_names.emplace_back(std::move(argument_node_name));
        }

        aggregate_description.column_name = std::move(node_name);
        aggregate_descriptions.push_back(std::move(aggregate_description));
    }

    return aggregate_descriptions;
}

}
