#include <Analyzer/Passes/AggregateFunctionOfGroupByKeysPass.h>

#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/TrivialGroupByLimit.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

#include <Functions/IFunctionAdaptors.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsBool optimize_aggregators_of_group_by_keys;
}

namespace
{

/// Try to eliminate min/max/any/anyLast.
class EliminateFunctionVisitor : public InDepthQueryTreeVisitorWithContext<EliminateFunctionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<EliminateFunctionVisitor>;

    EliminateFunctionVisitor(UsableGroupByKeys usable_keys_, ContextPtr context)
        : Base(std::move(context))
        , usable_keys(usable_keys_)
    {}

    struct GroupByKeys
    {
        QueryTreeNodePtrWithHashSet keys;
        /// Keys that are not in every grouping set, and the subexpressions of all keys. Other keys have default values in the sets
        /// without them, also if they are functions of `keys`: neither `max(negate(x))` nor `negate(max(x))` may become the key
        /// `negate(x)`. And `max(x) + y` may not become the key `x + y` if `y` is such a key.
        QueryTreeNodePtrWithHashSet parts_of_other_keys;
    };

    using GroupByKeysStack = std::vector<GroupByKeys>;

    bool collecting_output_nodes = true;

    void enterImpl(QueryTreeNodePtr & node)
    {
        bool is_filter_root = std::exchange(entering_filter_section, false);

        if (isArrayJoinFunction(node))
            ++array_join_depth;

        if (!getSettings()[Setting::optimize_aggregators_of_group_by_keys])
            return;

        if (node->getNodeType() == QueryTreeNodeType::LAMBDA)
        {
            /// A lambda body is an output position: its type is cached in the higher-order function.
            saved_filter_roots.push_back(std::exchange(filter_root, nullptr));
            return;
        }

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
        {
            if (is_filter_root)
                filter_root = node.get();

            if (collecting_output_nodes)
            {
                if (!filter_root)
                    output_nodes.insert(node.get());
            }
            else if (filter_root && output_nodes.contains(node.get()))
            {
                /// An alias shared with an output position: rewrite its subtree for output.
                saved_filter_roots.push_back(std::exchange(filter_root, nullptr));
                shared_nodes_in_filter.push_back(node.get());
            }
            return;
        }

        saved_filter_roots.push_back(std::exchange(filter_root, nullptr));

        /// Collect group by keys.
        if (!query_node->hasGroupBy())
        {
            group_by_keys_stack.push_back({});
        }
        else if (query_node->isGroupByWithTotals() || query_node->isGroupByWithCube() || query_node->isGroupByWithRollup())
        {
            /// Keep aggregator if group by is with totals/cube/rollup.
            group_by_keys_stack.push_back({});
        }
        else if (usable_keys == UsableGroupByKeys::AFTER_REPLACEMENTS && getTrivialGroupByLimit(*query_node, getSettings()))
        {
            /// Without aggregate functions, `OptimizeTrivialGroupByLimitPass` would have limited the number of groups with settings that
            /// also apply to aggregations in views. With them, the planner limits only the groups of this query. So keep them.
            group_by_keys_stack.push_back({});
        }
        else if (query_node->isGroupByWithGroupingSets() && getSettings()[Setting::group_by_use_nulls])
        {
            /// With `group_by_use_nulls` the keys of grouping sets become `Nullable` in the aggregation's output,
            /// while the argument of the aggregate function keeps its type, so the replacement would not match it.
            group_by_keys_stack.push_back({});
        }
        else if (canEqualGroupByKeysDiffer(*query_node))
        {
            /// Neither of them is present in every set.
            group_by_keys_stack.push_back({});
        }
        else if (std::ranges::any_of(
                     query_node->getOrderBy().getNodes(),
                     [](const QueryTreeNodePtr & sort_node) { return sort_node->as<SortNode &>().withFill(); }))
        {
            /// `WITH FILL` generates different values for the key and for the aggregate function.
            group_by_keys_stack.push_back({});
        }
        else
        {
            QueryTreeNodePtrWithHashSet group_by_keys;
            QueryTreeNodePtrWithHashSet keys_of_some_sets;
            bool first_grouping_set = true;
            for (auto & group_key : query_node->getGroupBy().getNodes())
            {
                /// For grouping sets case collect only keys that are presented in every set.
                if (auto * list = group_key->as<ListNode>())
                {
                    keys_of_some_sets.insert(list->getNodes().begin(), list->getNodes().end());

                    if (first_grouping_set)
                    {
                        for (auto & group_elem : list->getNodes())
                            group_by_keys.insert(group_elem);
                        first_grouping_set = false;
                    }
                    else
                    {
                        QueryTreeNodePtrWithHashSet common_keys_set;
                        for (auto & group_elem : list->getNodes())
                        {
                            if (group_by_keys.contains(group_elem))
                                common_keys_set.insert(group_elem);
                        }
                        group_by_keys = std::move(common_keys_set);
                    }
                }
                else
                {
                    group_by_keys.insert(group_key);
                }
            }

            for (const auto & group_key : group_by_keys)
                keys_of_some_sets.erase(group_key);

            QueryTreeNodePtrWithHashSet parts_of_other_keys;
            QueryTreeNodes nodes_to_visit;
            for (const auto & group_key : keys_of_some_sets)
                nodes_to_visit.push_back(group_key.node);

            if (!keys_of_some_sets.empty())
            {
                for (const auto & group_key : group_by_keys)
                    for (const auto & child : group_key.node->getChildren())
                        if (child)
                            nodes_to_visit.push_back(child);
            }

            while (!nodes_to_visit.empty())
            {
                auto part = std::move(nodes_to_visit.back());
                nodes_to_visit.pop_back();

                if (!parts_of_other_keys.insert(part).second)
                    continue;

                for (const auto & child : part->getChildren())
                    if (child)
                        nodes_to_visit.push_back(child);
            }

            if (usable_keys == UsableGroupByKeys::BEFORE_REPLACEMENTS)
            {
                /// See above for the limit. Aggregate functions of columns are replaced nevertheless, it is cheaper without them.
                const bool limits_groups = getTrivialGroupByLimit(*query_node, getSettings()).has_value();
                std::erase_if(
                    group_by_keys,
                    [&](const auto & group_key)
                    {
                        return isReplaceableWithArguments(group_key.node)
                            || (limits_groups && group_key.node->getNodeType() == QueryTreeNodeType::FUNCTION);
                    });
            }

            group_by_keys_stack.push_back({std::move(group_by_keys), std::move(parts_of_other_keys)});
        }
    }

    /// Now we visit all nodes in QueryNode, we should remove group_by_keys from stack.
    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (isArrayJoinFunction(node))
            --array_join_depth;

        if (!getSettings()[Setting::optimize_aggregators_of_group_by_keys])
            return;

        const auto node_type = node->getNodeType();
        if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::LAMBDA)
        {
            if (node_type == QueryTreeNodeType::QUERY)
                group_by_keys_stack.pop_back();
            filter_root = saved_filter_roots.back();
            saved_filter_roots.pop_back();
            return;
        }

        const IQueryTreeNode * node_before_rewrite = node.get();
        if (!shared_nodes_in_filter.empty() && shared_nodes_in_filter.back() == node_before_rewrite)
        {
            /// The node's own slot is not shared, so it is rewritten in the filter context.
            shared_nodes_in_filter.pop_back();
            filter_root = saved_filter_roots.back();
            saved_filter_roots.pop_back();
        }

        if (!collecting_output_nodes && node_type == QueryTreeNodeType::FUNCTION)
        {
            auto * function_node = node->as<FunctionNode>();
            if (array_join_depth == 0 && aggregationCanBeEliminated(node, group_by_keys_stack.back()))
            {
                /// min(LowCardinality(String)) is String: a filter keeps the bare key for the pushdown,
                /// an output position casts it back to keep the observable type.
                auto original_type = function_node->getResultType();
                auto & key_node = function_node->getArguments().getNodes()[0];

                if (filter_root || original_type->equals(*key_node->getResultType()))
                {
                    node = key_node;
                    if (!node->getResultType()->equals(*original_type))
                        retyped_nodes.insert(node);
                }
                else
                {
                    /// Fold a constant key the way a remote shard re-analyzing the AST does, see `foldConstantCast`.
                    node = foldConstantCast(createCastFunction(key_node, std::move(original_type), getContext()));
                }
            }
            else if (filter_root && function_node->isOrdinaryFunction())
            {
                reresolveIfArgumentsRetyped(node);
            }
        }

        if (node_before_rewrite == filter_root)
            filter_root = nullptr;
    }

    bool needChildVisit(VisitQueryTreeNodeType & parent, VisitQueryTreeNodeType & child)
    {
        /// Skip ArrayJoin.
        if (child->as<ArrayJoinNode>())
            return false;

        /// Compare slots, not nodes: the analyzer shares an alias node between its occurrences.
        auto * query_node = parent->as<QueryNode>();
        entering_filter_section = query_node
            && (&child == &query_node->getPrewhere() || &child == &query_node->getWhere()
                || &child == &query_node->getHaving() || &child == &query_node->getQualify());
        return true;
    }

private:

    static bool isArrayJoinFunction(const QueryTreeNodePtr & node)
    {
        const auto * function_node = node->as<FunctionNode>();
        return function_node && function_node->getFunctionName() == "arrayJoin";
    }

    struct NodeWithInfo
    {
        QueryTreeNodePtr node;
        bool parents_are_only_deterministic = false;
    };

    /// Only arguments retyped by this pass count: the resolved argument types can differ from the
    /// argument nodes anyway (the constant right side of IN is resolved as Set).
    void reresolveIfArgumentsRetyped(QueryTreeNodePtr & node)
    {
        auto & function_node = node->as<FunctionNode &>();
        const auto & resolved_argument_types = function_node.getArgumentTypes();
        auto & argument_nodes = function_node.getArguments().getNodes();
        if (resolved_argument_types.size() != argument_nodes.size())
            return;

        std::vector<size_t> retyped_arguments;
        for (size_t i = 0; i < argument_nodes.size(); ++i)
        {
            if (retyped_nodes.contains(argument_nodes[i]) && !resolved_argument_types[i]->equals(*argument_nodes[i]->getResultType()))
                retyped_arguments.push_back(i);
        }

        if (retyped_arguments.empty())
            return;

        if (!functionIsTransparentToLowCardinality(function_node))
        {
            for (size_t i : retyped_arguments)
                argument_nodes[i] = foldConstantCast(createCastFunction(argument_nodes[i], resolved_argument_types[i], getContext()));
            return;
        }

        auto result_type = function_node.getResultType();
        resolveOrdinaryFunctionNodeByName(function_node, function_node.getFunctionName(), getContext());
        if (!function_node.getResultType()->equals(*result_type))
            retyped_nodes.insert(node);
    }

    static bool functionIsTransparentToLowCardinality(const FunctionNode & function_node)
    {
        const auto & function_base = function_node.getFunction();
        const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(function_base.get());
        if (!adaptor || !adaptor->getFunction())
            return false;
        return adaptor->getFunction()->useDefaultImplementationForLowCardinalityColumns();
    }

    bool aggregationCanBeEliminated(QueryTreeNodePtr & node, const GroupByKeys & group_by_keys)
    {
        if (group_by_keys.keys.empty())
            return false;

        auto * function = node->as<FunctionNode>();
        if (!function || !function->isAggregateFunction())
            return false;

        if (!isAggregateFunctionOfEqualValues(*function))
            return false;

        std::vector<NodeWithInfo> candidates;
        auto & function_arguments = function->getArguments().getNodes();
        if (usable_keys == UsableGroupByKeys::AFTER_REPLACEMENTS && !hasReplaceableWithArguments(function_arguments[0]))
            return false;

        candidates.push_back({ function_arguments[0], true });

        /// Using DFS we traverse function tree and try to find if it uses other keys as function arguments.
        while (!candidates.empty())
        {
            auto [candidate, parents_are_only_deterministic] = candidates.back();
            candidates.pop_back();

            /// Calculate the hash once.
            const QueryTreeNodePtrWithHash candidate_with_hash(candidate);
            if (group_by_keys.parts_of_other_keys.contains(candidate_with_hash))
                return false;

            auto key = group_by_keys.keys.find(candidate_with_hash);
            bool found = key != group_by_keys.keys.end();

            if (found && !areConstantsWrittenAlike(candidate, key->node))
                return false;

            /// For example, `arrayJoin` of a key has several values in a group.
            if (found && !parents_are_only_deterministic)
                return false;

            switch (candidate->getNodeType())
            {
                case QueryTreeNodeType::FUNCTION:
                {
                    auto * func = candidate->as<FunctionNode>();
                    auto & arguments = func->getArguments().getNodes();
                    if (arguments.empty())
                        return false;

                    if (!found)
                    {
                        bool is_deterministic_function = parents_are_only_deterministic &&
                            func->getFunctionOrThrow()->isDeterministicInScopeOfQuery();
                        for (auto it = arguments.rbegin(); it != arguments.rend(); ++it)
                            candidates.push_back({ *it, is_deterministic_function });
                    }
                    break;
                }
                case QueryTreeNodeType::COLUMN:
                    if (!found)
                        return false;
                    break;
                case QueryTreeNodeType::CONSTANT:
                    if (!parents_are_only_deterministic)
                        return false;
                    break;
                default:
                    return false;
            }
        }

        return true;
    }

    const UsableGroupByKeys usable_keys;

    GroupByKeysStack group_by_keys_stack;

    /// Equal `arrayJoin` functions are the same one, so `arrayJoin(max(x))` may not become `arrayJoin(x)`.
    size_t array_join_depth = 0;

    /// Root of the WHERE/PREWHERE/HAVING/QUALIFY section of the current query being visited, if any.
    const IQueryTreeNode * filter_root = nullptr;
    bool entering_filter_section = false;
    std::vector<const IQueryTreeNode *> saved_filter_roots;

    std::unordered_set<const IQueryTreeNode *> output_nodes;
    std::vector<const IQueryTreeNode *> shared_nodes_in_filter;

    /// Filter-subtree nodes whose type differs from what their parent was resolved with.
    std::unordered_set<QueryTreeNodePtr> retyped_nodes;
};

}

void AggregateFunctionOfGroupByKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    EliminateFunctionVisitor eliminator(usable_keys, context);
    eliminator.visit(query_tree_node);
    eliminator.collecting_output_nodes = false;
    eliminator.visit(query_tree_node);
}

};
