#include <Analyzer/Passes/GroupingFunctionsResolvePass.h>

#include <Core/ColumnNumbers.h>
#include <Core/Settings.h>

#include <Functions/grouping.h>

#include <Interpreters/Context.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool force_grouping_standard_compatibility;
    extern const SettingsBool group_by_use_nulls;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

enum class GroupByKind : uint8_t
{
    ORDINARY,
    ROLLUP,
    CUBE,
    GROUPING_SETS
};

class GroupingFunctionResolveVisitor : public InDepthQueryTreeVisitorWithContext<GroupingFunctionResolveVisitor>
{
public:
    GroupingFunctionResolveVisitor(GroupByKind group_by_kind_,
        QueryTreeNodePtrWithHashMap<size_t> aggregation_key_to_index_,
        ColumnNumbersList grouping_sets_keys_indices_,
        ContextPtr context_)
        : InDepthQueryTreeVisitorWithContext(std::move(context_))
        , group_by_kind(group_by_kind_)
        , aggregation_key_to_index(std::move(aggregation_key_to_index_))
        , grouping_sets_keys_indexes(std::move(grouping_sets_keys_indices_))
    {
    }

    void enterImpl(const QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "grouping")
            return;

        auto & function_arguments = function_node->getArguments().getNodes();

        ColumnNumbers arguments_indexes;
        arguments_indexes.reserve(function_arguments.size());

        for (const auto & argument : function_arguments)
        {
            auto it = aggregation_key_to_index.find(argument);
            if (it == aggregation_key_to_index.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Argument {} of GROUPING function is not a part of GROUP BY clause",
                    argument->formatASTForErrorMessage());

            arguments_indexes.push_back(it->second);
        }

        FunctionOverloadResolverPtr grouping_function_resolver;
        bool add_grouping_set_column = false;

        bool force_grouping_standard_compatibility = getSettings()[Setting::force_grouping_standard_compatibility];
        size_t aggregation_keys_size = aggregation_key_to_index.size();

        switch (group_by_kind)
        {
            case GroupByKind::ORDINARY:
            {
                auto grouping_ordinary_function
                    = std::make_shared<FunctionGroupingOrdinary>(arguments_indexes, force_grouping_standard_compatibility);
                grouping_function_resolver = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(grouping_ordinary_function));
                break;
            }
            case GroupByKind::ROLLUP:
            {
                auto grouping_rollup_function = std::make_shared<FunctionGroupingForRollup>(
                    arguments_indexes, aggregation_keys_size, force_grouping_standard_compatibility);
                grouping_function_resolver = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(grouping_rollup_function));
                add_grouping_set_column = true;
                break;
            }
            case GroupByKind::CUBE:
            {
                auto grouping_cube_function = std::make_shared<FunctionGroupingForCube>(
                    arguments_indexes, aggregation_keys_size, force_grouping_standard_compatibility);
                grouping_function_resolver = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(grouping_cube_function));
                add_grouping_set_column = true;
                break;
            }
            case GroupByKind::GROUPING_SETS:
            {
                auto grouping_grouping_sets_function = std::make_shared<FunctionGroupingForGroupingSets>(
                    arguments_indexes, grouping_sets_keys_indexes, force_grouping_standard_compatibility);
                grouping_function_resolver = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(grouping_grouping_sets_function));
                add_grouping_set_column = true;
                break;
            }
        }

        if (add_grouping_set_column)
        {
            QueryTreeNodeWeakPtr column_source;
            auto grouping_set_column = NameAndTypePair{"__grouping_set", std::make_shared<DataTypeUInt64>()};
            auto grouping_set_argument_column = std::make_shared<ColumnNode>(std::move(grouping_set_column), std::move(column_source));
            function_arguments.insert(function_arguments.begin(), std::move(grouping_set_argument_column));
        }

        function_node->resolveAsFunction(grouping_function_resolver->build(function_node->getArgumentColumns()));
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return !(child_node->getNodeType() == QueryTreeNodeType::QUERY || child_node->getNodeType() == QueryTreeNodeType::UNION);
    }

private:
    GroupByKind group_by_kind;
    QueryTreeNodePtrWithHashMap<size_t> aggregation_key_to_index;
    ColumnNumbersList grouping_sets_keys_indexes;
};

void resolveGroupingFunctions(QueryTreeNodePtr & query_node, ContextPtr context)
{
    auto & query_node_typed = query_node->as<QueryNode &>();

    size_t aggregation_node_index = 0;
    QueryTreeNodePtrWithHashMap<size_t> aggregation_key_to_index;

    std::vector<QueryTreeNodes> grouping_sets_used_aggregation_keys_list;

    if (query_node_typed.hasGroupBy())
    {
        /// It is expected by execution layer that if there are only 1 grouping set it will be removed
        if (query_node_typed.isGroupByWithGroupingSets() && query_node_typed.getGroupBy().getNodes().size() == 1
            && !context->getSettingsRef()[Setting::group_by_use_nulls])
        {
            auto grouping_set_list_node = query_node_typed.getGroupBy().getNodes().front();
            auto & grouping_set_list_node_typed = grouping_set_list_node->as<ListNode &>();
            query_node_typed.getGroupBy().getNodes() = std::move(grouping_set_list_node_typed.getNodes());
            query_node_typed.setIsGroupByWithGroupingSets(false);
        }

        if (query_node_typed.isGroupByWithGroupingSets())
        {
            for (const auto & grouping_set_keys_list_node : query_node_typed.getGroupBy().getNodes())
            {
                auto & grouping_set_keys_list_node_typed = grouping_set_keys_list_node->as<ListNode &>();

                grouping_sets_used_aggregation_keys_list.emplace_back();
                auto & grouping_sets_used_aggregation_keys = grouping_sets_used_aggregation_keys_list.back();

                QueryTreeNodePtrWithHashSet used_keys_in_set;

                for (auto & grouping_set_key_node : grouping_set_keys_list_node_typed.getNodes())
                {
                    if (used_keys_in_set.contains(grouping_set_key_node))
                        continue;
                    used_keys_in_set.insert(grouping_set_key_node);
                    grouping_sets_used_aggregation_keys.push_back(grouping_set_key_node);

                    if (aggregation_key_to_index.contains(grouping_set_key_node))
                        continue;
                    aggregation_key_to_index.emplace(grouping_set_key_node, aggregation_node_index);
                    ++aggregation_node_index;
                }
            }
        }
        else
        {
            for (auto & group_by_key_node : query_node_typed.getGroupBy().getNodes())
            {
                if (aggregation_key_to_index.contains(group_by_key_node))
                    continue;

                aggregation_key_to_index.emplace(group_by_key_node, aggregation_node_index);
                ++aggregation_node_index;
            }
        }
    }

    /// Indexes of aggregation keys used in each grouping set (only for GROUP BY GROUPING SETS)
    ColumnNumbersList grouping_sets_keys_indexes;

    for (const auto & grouping_set_used_aggregation_keys : grouping_sets_used_aggregation_keys_list)
    {
        grouping_sets_keys_indexes.emplace_back();
        auto & grouping_set_keys_indexes = grouping_sets_keys_indexes.back();

        for (const auto & used_aggregation_key : grouping_set_used_aggregation_keys)
        {
            auto aggregation_node_index_it = aggregation_key_to_index.find(used_aggregation_key);
            if (aggregation_node_index_it == aggregation_key_to_index.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Aggregation key {} in GROUPING SETS is not found in GROUP BY keys",
                    used_aggregation_key->formatASTForErrorMessage());

            grouping_set_keys_indexes.push_back(aggregation_node_index_it->second);
        }
    }

    GroupByKind group_by_kind = GroupByKind::ORDINARY;
    if (query_node_typed.isGroupByWithRollup())
        group_by_kind = GroupByKind::ROLLUP;
    else if (query_node_typed.isGroupByWithCube())
        group_by_kind = GroupByKind::CUBE;
    else if (query_node_typed.isGroupByWithGroupingSets())
        group_by_kind = GroupByKind::GROUPING_SETS;

    GroupingFunctionResolveVisitor visitor(group_by_kind,
        std::move(aggregation_key_to_index),
        std::move(grouping_sets_keys_indexes),
        std::move(context));
    visitor.visit(query_node);
}

class GroupingFunctionsResolveVisitor : public InDepthQueryTreeVisitorWithContext<GroupingFunctionsResolveVisitor>
{
    using Base = InDepthQueryTreeVisitorWithContext<GroupingFunctionsResolveVisitor>;
public:
    explicit GroupingFunctionsResolveVisitor(ContextPtr context_)
        : Base(std::move(context_))
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (node->getNodeType() != QueryTreeNodeType::QUERY)
            return;

        resolveGroupingFunctions(node, getContext());
    }

private:
    ContextPtr context;
};

}

void GroupingFunctionsResolvePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    GroupingFunctionsResolveVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
