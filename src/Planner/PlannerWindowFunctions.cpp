#include <Planner/PlannerWindowFunctions.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/WindowNode.h>

#include <Interpreters/Context.h>

#include <Planner/PlannerSorting.h>
#include <Planner/PlannerActionsVisitor.h>

namespace DB
{

namespace
{

WindowDescription extractWindowDescriptionFromWindowNode(const QueryTreeNodePtr & node, const PlannerContext & planner_context)
{
    auto & window_node = node->as<WindowNode &>();

    WindowDescription window_description;
    window_description.window_name = calculateWindowNodeActionName(node, planner_context);

    for (const auto & partition_by_node : window_node.getPartitionBy().getNodes())
    {
        auto partition_by_node_action_name = calculateActionNodeName(partition_by_node, planner_context);
        auto partition_by_sort_column_description = SortColumnDescription(partition_by_node_action_name, 1 /* direction */, 1 /* nulls_direction */);
        window_description.partition_by.push_back(std::move(partition_by_sort_column_description));
    }

    window_description.order_by = extractSortDescription(window_node.getOrderByNode(), planner_context);

    window_description.full_sort_description = window_description.partition_by;
    window_description.full_sort_description.insert(window_description.full_sort_description.end(), window_description.order_by.begin(), window_description.order_by.end());

    /// WINDOW frame is validated during query analysis stage
    window_description.frame = window_node.getWindowFrame();

    const auto & query_context = planner_context.getQueryContext();
    const auto & query_context_settings = query_context->getSettingsRef();

    bool compile_sort_description = query_context_settings.compile_sort_description;
    size_t min_count_to_compile_sort_description = query_context_settings.min_count_to_compile_sort_description;

    window_description.partition_by.compile_sort_description = compile_sort_description;
    window_description.partition_by.min_count_to_compile_sort_description = min_count_to_compile_sort_description;

    window_description.order_by.compile_sort_description = compile_sort_description;
    window_description.order_by.min_count_to_compile_sort_description = min_count_to_compile_sort_description;

    window_description.full_sort_description.compile_sort_description = compile_sort_description;
    window_description.full_sort_description.min_count_to_compile_sort_description = min_count_to_compile_sort_description;

    return window_description;
}

}

std::vector<WindowDescription> extractWindowDescriptions(const QueryTreeNodes & window_function_nodes, const PlannerContext & planner_context)
{
    std::unordered_map<std::string, WindowDescription> window_name_to_description;

    for (const auto & window_function_node : window_function_nodes)
    {
        auto & window_function_node_typed = window_function_node->as<FunctionNode &>();

        auto function_window_description = extractWindowDescriptionFromWindowNode(window_function_node_typed.getWindowNode(), planner_context);
        auto window_name = function_window_description.window_name;

        auto [it, _] = window_name_to_description.emplace(window_name, std::move(function_window_description));
        auto & window_description = it->second;

        WindowFunctionDescription window_function;
        window_function.function_node = nullptr;
        window_function.column_name = calculateActionNodeName(window_function_node, planner_context);
        window_function.aggregate_function = window_function_node_typed.getAggregateFunction();

        const auto & parameters_nodes = window_function_node_typed.getParameters().getNodes();
        window_function.function_parameters.reserve(parameters_nodes.size());

        for (const auto & parameter_node : parameters_nodes)
        {
            /// Function parameters constness validated during analysis stage
            window_function.function_parameters.push_back(parameter_node->getConstantValue().getValue());
        }

        const auto & arguments_nodes = window_function_node_typed.getArguments().getNodes();
        window_function.argument_names.reserve(arguments_nodes.size());
        window_function.argument_types.reserve(arguments_nodes.size());

        for (const auto & argument_node : arguments_nodes)
        {
            String argument_node_name = calculateActionNodeName(argument_node, planner_context);
            window_function.argument_names.emplace_back(std::move(argument_node_name));
            window_function.argument_types.emplace_back(argument_node->getResultType());
        }

        window_description.window_functions.push_back(window_function);
    }

    std::vector<WindowDescription> result;
    result.reserve(window_name_to_description.size());

    for (auto && [_, window_description] : window_name_to_description)
        result.push_back(std::move(window_description));

    return result;
}

void sortWindowDescriptions(std::vector<WindowDescription> & window_descriptions)
{
    auto window_description_comparator = [](const WindowDescription & lhs, const WindowDescription & rhs)
    {
        const auto & left = lhs.full_sort_description;
        const auto & right = rhs.full_sort_description;

        for (size_t i = 0; i < std::min(left.size(), right.size()); ++i)
        {
            if (left[i].column_name < right[i].column_name)
                return true;
            else if (left[i].column_name > right[i].column_name)
                return false;
            else if (left[i].direction < right[i].direction)
                return true;
            else if (left[i].direction > right[i].direction)
                return false;
            else if (left[i].nulls_direction < right[i].nulls_direction)
                return true;
            else if (left[i].nulls_direction > right[i].nulls_direction)
                return false;

            assert(left[i] == right[i]);
        }

        /** Note that we check the length last, because we want to put together the
          * sort orders that have common prefix but different length.
          */
        return left.size() > right.size();
    };

    ::sort(window_descriptions.begin(), window_descriptions.end(), window_description_comparator);
}

}
