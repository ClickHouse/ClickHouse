#include <Analyzer/Passes/GroupingFunctionsResolvePass.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>

#include <Core/ColumnNumbers.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ValidationUtils.h>

#include <ranges>

#include <fmt/ranges.h>

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

struct GroupByKeyComparator
{
    GroupByKeyComparator(QueryTreeNodePtr node_) /// NOLINT
        : node(std::move(node_))
        , hash(node->getTreeHash({.compare_aliases = false}))
    {}

    bool operator==(const GroupByKeyComparator & other) const { return hash == other.hash && compareGroupByKeys(node, other.node); }

    bool operator!=(const GroupByKeyComparator & other) const { return !(*this == other); }

    struct Hasher { size_t operator()(const GroupByKeyComparator & key) const { return key.hash.low64; } };

    QueryTreeNodePtr node = nullptr;
    CityHash_v1_0_2::uint128 hash;
};

template <typename Value>
using AggredationKeyNodeMap = std::unordered_map<GroupByKeyComparator, Value, GroupByKeyComparator::Hasher>;

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
        AggredationKeyNodeMap<size_t> aggregation_key_to_index_,
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
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Argument {} of GROUPING function is not a part of GROUP BY clause [{}]",
                    argument->formatASTForErrorMessage(),
                    fmt::join(aggregation_key_to_index | std::views::transform([](const auto & e) { return e.first.node->formatASTForErrorMessage(); }), ", "));
            }

            arguments_indexes.push_back(it->second);
        }

        bool force_grouping_standard_compatibility = getSettings()[Setting::force_grouping_standard_compatibility];
        size_t aggregation_keys_size = aggregation_key_to_index.size();

        /// The specialization keeps its parameters in trailing constant arguments, and the
        /// registered `groupingFor*` resolvers rebuild the function from them (see
        /// `GroupingSpecializationResolver`). This way a serialized query plan can carry the
        /// function: a peer re-resolves it from the name and the arguments alone.
        String specialization_name;
        QueryTreeNodes state_arguments;

        auto indexes_data = ColumnUInt64::create();
        indexes_data->getData().assign(arguments_indexes.begin(), arguments_indexes.end());
        auto indexes_column = ColumnArray::create(
            std::move(indexes_data), ColumnArray::ColumnOffsets::create(1, arguments_indexes.size()));
        state_arguments.push_back(std::make_shared<ConstantNode>(
            ColumnConst::create(std::move(indexes_column), 1),
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())));

        switch (group_by_kind)
        {
            case GroupByKind::ORDINARY:
            {
                specialization_name = "__groupingOrdinary";
                break;
            }
            case GroupByKind::ROLLUP:
            {
                specialization_name = "__groupingForRollup";
                state_arguments.push_back(std::make_shared<ConstantNode>(
                    ColumnConst::create(ColumnUInt64::create(1, aggregation_keys_size), 1),
                    std::make_shared<DataTypeUInt64>()));
                break;
            }
            case GroupByKind::CUBE:
            {
                specialization_name = "__groupingForCube";
                state_arguments.push_back(std::make_shared<ConstantNode>(
                    ColumnConst::create(ColumnUInt64::create(1, aggregation_keys_size), 1),
                    std::make_shared<DataTypeUInt64>()));
                break;
            }
            case GroupByKind::GROUPING_SETS:
            {
                specialization_name = "__groupingForGroupingSets";
                auto sets_data = ColumnUInt64::create();
                auto sets_offsets = ColumnArray::ColumnOffsets::create();
                UInt64 indexes_total = 0;
                for (const auto & grouping_set : grouping_sets_keys_indexes)
                {
                    for (const auto index : grouping_set)
                        sets_data->getData().push_back(index);
                    indexes_total += grouping_set.size();
                    sets_offsets->getData().push_back(indexes_total);
                }
                auto sets_column = ColumnArray::create(
                    ColumnArray::create(std::move(sets_data), std::move(sets_offsets)),
                    ColumnArray::ColumnOffsets::create(1, grouping_sets_keys_indexes.size()));
                state_arguments.push_back(std::make_shared<ConstantNode>(
                    ColumnConst::create(std::move(sets_column), 1),
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()))));
                break;
            }
        }

        state_arguments.push_back(std::make_shared<ConstantNode>(
            ColumnConst::create(ColumnUInt8::create(1, force_grouping_standard_compatibility), 1),
            std::make_shared<DataTypeUInt8>()));

        if (group_by_kind != GroupByKind::ORDINARY)
        {
            TableExpressionNodeWeakPtr column_source;
            auto grouping_set_column = NameAndTypePair{"__grouping_set", std::make_shared<DataTypeUInt64>()};
            auto grouping_set_argument_column = std::make_shared<ColumnNode>(std::move(grouping_set_column), std::move(column_source));
            function_arguments.insert(function_arguments.begin(), std::move(grouping_set_argument_column));
        }

        function_arguments.insert(function_arguments.end(), state_arguments.begin(), state_arguments.end());

        auto grouping_function_resolver = FunctionFactory::instance().get(specialization_name, getContext());
        function_node->resolveAsFunction(grouping_function_resolver->build(function_node->getArgumentColumns()));
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return !(child_node->getNodeType() == QueryTreeNodeType::QUERY || child_node->getNodeType() == QueryTreeNodeType::UNION);
    }

private:
    GroupByKind group_by_kind;
    AggredationKeyNodeMap<size_t> aggregation_key_to_index;
    ColumnNumbersList grouping_sets_keys_indexes;
};

void resolveGroupingFunctions(QueryTreeNodePtr & query_node, ContextPtr context)
{
    auto & query_node_typed = query_node->as<QueryNode &>();

    size_t aggregation_node_index = 0;
    AggredationKeyNodeMap<size_t> aggregation_key_to_index;

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
