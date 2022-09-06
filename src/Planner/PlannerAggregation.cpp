#include <Planner/PlannerAggregation.h>

#include <Functions/grouping.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/CollectAggregateFunctionVisitor.h>

#include <Planner/PlannerActionsVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

enum class GroupByKind
{
    ORDINARY,
    ROLLUP,
    CUBE,
    GROUPING_SETS
};

class GroupingFunctionResolveMatcher
{
public:
    using Visitor = InDepthQueryTreeVisitor<GroupingFunctionResolveMatcher, true, false>;

    struct Data
    {
        Data(GroupByKind group_by_kind_,
            const Names & aggregation_keys_,
            const GroupingSetsParamsList & grouping_sets_parameters_list_,
            const PlannerContext & planner_context_)
            : group_by_kind(group_by_kind_)
            , planner_context(planner_context_)
        {
            size_t aggregation_keys_size = aggregation_keys_.size();
            for (size_t i = 0; i < aggregation_keys_size; ++i)
                aggegation_key_to_index.emplace(aggregation_keys_[i], i);

            for (const auto & grouping_sets_parameter : grouping_sets_parameters_list_)
            {
                grouping_sets_keys_indices.emplace_back();
                auto & grouping_set_keys_indices = grouping_sets_keys_indices.back();

                for (const auto & used_key : grouping_sets_parameter.used_keys)
                {
                    auto aggregation_key_index_it = aggegation_key_to_index.find(used_key);
                    if (aggregation_key_index_it == aggegation_key_to_index.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Aggregation key {} in GROUPING SETS is not found in GROUP BY keys");

                    grouping_set_keys_indices.push_back(aggregation_key_index_it->second);
                }
            }
        }

        GroupByKind group_by_kind;
        std::unordered_map<std::string, size_t> aggegation_key_to_index;
        // Indexes of aggregation keys used in each grouping set (only for GROUP BY GROUPING SETS)
        ColumnNumbersList grouping_sets_keys_indices;
        const PlannerContext & planner_context;
    };

    static void visit(const QueryTreeNodePtr & node, Data & data)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "grouping")
            return;

        size_t aggregation_keys_size = data.aggegation_key_to_index.size();

        ColumnNumbers arguments_indexes;

        for (const auto & argument : function_node->getArguments().getNodes())
        {
            String action_node_name = calculateActionNodeName(argument, data.planner_context);

            auto it = data.aggegation_key_to_index.find(action_node_name);
            if (it == data.aggegation_key_to_index.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Argument of GROUPING function {} is not a part of GROUP BY clause",
                        argument->formatASTForErrorMessage());

            arguments_indexes.push_back(it->second);
        }

        QueryTreeNodeWeakPtr column_source;
        auto grouping_set_argument_column = std::make_shared<ColumnNode>(NameAndTypePair{"__grouping_set", std::make_shared<DataTypeUInt64>()}, column_source);
        function_node->getArguments().getNodes().clear();

        switch (data.group_by_kind)
        {
            case GroupByKind::ORDINARY:
            {
                auto grouping_ordinary_function = std::make_shared<FunctionGroupingOrdinary>(arguments_indexes);
                auto grouping_ordinary_function_adaptor = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(grouping_ordinary_function));
                function_node->resolveAsFunction(grouping_ordinary_function_adaptor, std::make_shared<DataTypeUInt64>());
                break;
            }
            case GroupByKind::ROLLUP:
            {
                auto grouping_ordinary_function = std::make_shared<FunctionGroupingForRollup>(arguments_indexes, aggregation_keys_size);
                auto grouping_ordinary_function_adaptor = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(grouping_ordinary_function));
                function_node->resolveAsFunction(grouping_ordinary_function_adaptor, std::make_shared<DataTypeUInt64>());
                function_node->getArguments().getNodes().push_back(std::move(grouping_set_argument_column));
                break;
            }
            case GroupByKind::CUBE:
            {
                auto grouping_ordinary_function = std::make_shared<FunctionGroupingForCube>(arguments_indexes, aggregation_keys_size);
                auto grouping_ordinary_function_adaptor = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(grouping_ordinary_function));
                function_node->resolveAsFunction(grouping_ordinary_function_adaptor, std::make_shared<DataTypeUInt64>());
                function_node->getArguments().getNodes().push_back(std::move(grouping_set_argument_column));
                break;
            }
            case GroupByKind::GROUPING_SETS:
            {
                auto grouping_grouping_sets_function = std::make_shared<FunctionGroupingForGroupingSets>(arguments_indexes, data.grouping_sets_keys_indices);
                auto grouping_ordinary_function_adaptor = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(grouping_grouping_sets_function));
                function_node->resolveAsFunction(grouping_ordinary_function_adaptor, std::make_shared<DataTypeUInt64>());
                function_node->getArguments().getNodes().push_back(std::move(grouping_set_argument_column));
                break;
            }
        }
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return !(child_node->getNodeType() == QueryTreeNodeType::QUERY || child_node->getNodeType() == QueryTreeNodeType::UNION);
    }
};

using GroupingFunctionResolveVisitor = GroupingFunctionResolveMatcher::Visitor;

void resolveGroupingFunctions(QueryTreeNodePtr & node,
    GroupByKind group_by_kind,
    const Names & aggregation_keys,
    const GroupingSetsParamsList & grouping_sets_parameters_list,
    const PlannerContext & planner_context)
{
    GroupingFunctionResolveVisitor::Data data {group_by_kind, aggregation_keys, grouping_sets_parameters_list, planner_context};
    GroupingFunctionResolveVisitor visitor(data);
    visitor.visit(node);
}

}

void resolveGroupingFunctions(QueryTreeNodePtr & query_node,
    const Names & aggregation_keys,
    const GroupingSetsParamsList & grouping_sets_parameters_list,
    const PlannerContext & planner_context)
{
    auto & query_node_typed = query_node->as<QueryNode &>();

    GroupByKind group_by_kind = GroupByKind::ORDINARY;
    if (query_node_typed.isGroupByWithRollup())
        group_by_kind = GroupByKind::ROLLUP;
    else if (query_node_typed.isGroupByWithCube())
        group_by_kind = GroupByKind::CUBE;
    else if (query_node_typed.isGroupByWithGroupingSets())
        group_by_kind = GroupByKind::GROUPING_SETS;

    if (query_node_typed.hasHaving())
    {
        resolveGroupingFunctions(query_node_typed.getHaving(),
            group_by_kind,
            aggregation_keys,
            grouping_sets_parameters_list,
            planner_context);
    }

    resolveGroupingFunctions(query_node_typed.getOrderByNode(),
        group_by_kind,
        aggregation_keys,
        grouping_sets_parameters_list,
        planner_context);

    resolveGroupingFunctions(query_node_typed.getProjectionNode(),
        group_by_kind,
        aggregation_keys,
        grouping_sets_parameters_list,
        planner_context);
}

QueryTreeNodes extractAggregateFunctionNodes(const QueryTreeNodePtr & query_node)
{
    const auto & query_node_typed = query_node->as<QueryNode &>();

    QueryTreeNodes aggregate_function_nodes;
    if (query_node_typed.hasHaving())
        collectAggregateFunctionNodes(query_node_typed.getHaving(), aggregate_function_nodes);

    if (query_node_typed.hasOrderBy())
        collectAggregateFunctionNodes(query_node_typed.getOrderByNode(), aggregate_function_nodes);

    collectAggregateFunctionNodes(query_node_typed.getProjectionNode(), aggregate_function_nodes);

    return aggregate_function_nodes;
}

AggregateDescriptions extractAggregateDescriptions(const QueryTreeNodes & aggregate_function_nodes, const PlannerContext & planner_context)
{
    QueryTreeNodeToName node_to_name;
    NameSet unique_aggregate_action_node_names;
    AggregateDescriptions aggregate_descriptions;

    for (const auto & aggregate_function_node : aggregate_function_nodes)
    {
        const auto & aggregagte_function_node_typed = aggregate_function_node->as<FunctionNode &>();
        String node_name = calculateActionNodeName(aggregate_function_node, planner_context, node_to_name);
        auto [_, inserted] = unique_aggregate_action_node_names.emplace(node_name);
        if (!inserted)
            continue;

        AggregateDescription aggregate_description;
        aggregate_description.function = aggregagte_function_node_typed.getAggregateFunction();

        const auto & parameters_nodes = aggregagte_function_node_typed.getParameters().getNodes();
        aggregate_description.parameters.reserve(parameters_nodes.size());

        for (const auto & parameter_node : parameters_nodes)
        {
            /// Function parameters constness validated during analysis stage
            aggregate_description.parameters.push_back(parameter_node->getConstantValue().getValue());
        }

        const auto & arguments_nodes = aggregagte_function_node_typed.getArguments().getNodes();
        aggregate_description.argument_names.reserve(arguments_nodes.size());

        for (const auto & argument_node : arguments_nodes)
        {
            String argument_node_name = calculateActionNodeName(argument_node, planner_context, node_to_name);
            aggregate_description.argument_names.emplace_back(std::move(argument_node_name));
        }

        aggregate_description.column_name = node_name;
        aggregate_descriptions.push_back(std::move(aggregate_description));
    }

    return aggregate_descriptions;
}

}
