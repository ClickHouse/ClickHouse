#include <Analyzer/Passes/AggregateFunctionOfGroupByKeysPass.h>

#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

#include <DataTypes/DataTypeLowCardinality.h>

#include <Functions/IFunctionAdaptors.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_aggregators_of_group_by_keys;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Try to eliminate min/max/any/anyLast.
class EliminateFunctionVisitor : public InDepthQueryTreeVisitorWithContext<EliminateFunctionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<EliminateFunctionVisitor>;
    using Base::Base;

    using GroupByKeysStack = std::vector<QueryTreeNodePtrWithHashSet>;

    bool collecting_output_nodes = true;

    void enterImpl(QueryTreeNodePtr & node)
    {
        bool is_filter_root = std::exchange(entering_filter_section, false);
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
        else
        {
            QueryTreeNodePtrWithHashSet group_by_keys;
            bool first_grouping_set = true;
            for (auto & group_key : query_node->getGroupBy().getNodes())
            {
                /// For grouping sets case collect only keys that are presented in every set.
                if (auto * list = group_key->as<ListNode>())
                {
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
            group_by_keys_stack.push_back(std::move(group_by_keys));
        }
    }

    /// Now we visit all nodes in QueryNode, we should remove group_by_keys from stack.
    void leaveImpl(QueryTreeNodePtr & node)
    {
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
            if (aggregationCanBeEliminated(node, group_by_keys_stack.back()))
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

    bool aggregationCanBeEliminated(QueryTreeNodePtr & node, const QueryTreeNodePtrWithHashSet & group_by_keys)
    {
        if (group_by_keys.empty())
            return false;

        auto * function = node->as<FunctionNode>();
        if (!function || !function->isAggregateFunction())
            return false;

        /// Every aggregate here returns an actual element of its input column, so over a group where
        /// the argument is a GROUP BY key (constant within the group) the result equals that key and
        /// the aggregate can be dropped. Aliases (any_value / first_value -> any, last_value ->
        /// anyLast, *RespectNulls -> *_respect_nulls) are normalized to these canonical names by name
        /// resolution before this pass runs, so matching the canonical names covers them too.
        /// The same holds for the non-interpolating exact quantiles (quantileExact, its alias
        /// medianExact, quantileExactLow, quantileExactHigh), which return an element of the input
        /// at some position, and for the idempotent groupBitAnd / groupBitOr (x & x = x | x = x).
        /// The result type check below rejects any variant whose result type differs from the key.
        /// singleValueOrNull is excluded: it returns NULL unless the group has exactly one distinct
        /// value, so it is not value-preserving; groupBitXor is not idempotent; interpolating
        /// quantiles may return a value that is not an element; argMin/argMax are two-argument.
        const auto & function_name = function->getFunctionName();
        if (!(function_name == "min"
                || function_name == "max"
                || function_name == "any"
                || function_name == "anyLast"
                || function_name == "anyHeavy"
                || function_name == "any_respect_nulls"
                || function_name == "anyLast_respect_nulls"
                || function_name == "quantileExact"
                || function_name == "quantileExactLow"
                || function_name == "quantileExactHigh"
                || function_name == "groupBitAnd"
                || function_name == "groupBitOr"))
            return false;

        std::vector<NodeWithInfo> candidates;
        auto & function_arguments = function->getArguments().getNodes();
        if (function_arguments.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a single argument of function '{}' but received {}", function->getFunctionName(), function_arguments.size());

        if (!recursiveRemoveLowCardinality(function->getResultType())->equals(*recursiveRemoveLowCardinality(function_arguments[0]->getResultType())))
            return false;

        candidates.push_back({ function_arguments[0], true });

        /// Using DFS we traverse function tree and try to find if it uses other keys as function arguments.
        while (!candidates.empty())
        {
            auto [candidate, parents_are_only_deterministic] = candidates.back();
            candidates.pop_back();

            bool found = group_by_keys.contains(candidate);

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

    GroupByKeysStack group_by_keys_stack;

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
    EliminateFunctionVisitor eliminator(context);
    eliminator.visit(query_tree_node);
    eliminator.collecting_output_nodes = false;
    eliminator.visit(query_tree_node);
}

};
