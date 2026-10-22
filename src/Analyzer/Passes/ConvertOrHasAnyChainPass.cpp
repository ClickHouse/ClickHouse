#include <Analyzer/Passes/ConvertOrHasAnyChainPass.h>

#include <algorithm>
#include <unordered_set>
#include <vector>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>

#include <Common/assert_cast.h>

#include <Core/Settings.h>

#include <DataTypes/IDataType.h>

#include <Functions/FunctionFactory.h>
#include <Functions/logical.h>

#include <Interpreters/Context.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_or_has_any_chain;
}

namespace
{

/// Returns true if the subtree contains an ordinary function call that is non-deterministic
/// within a single query (e.g. `rand`). Two structurally equal haystacks of this kind evaluate
/// to different values, so merging their `hasAny` calls would change the result.
bool isExpressionNonDeterministic(const QueryTreeNodePtr & node)
{
    if (!node)
        return false;

    if (auto * function = node->as<FunctionNode>())
        if (function->isOrdinaryFunction())
            if (auto func = function->getFunctionOrThrow(); !func->isDeterministicInScopeOfQuery())
                return true;

    for (const auto & child : node->getChildren())
        if (isExpressionNonDeterministic(child))
            return true;

    return false;
}

/// Removes duplicate values, keeping the first occurrence of each value in its original position.
/// The order is kept because `hasAny` stops at the first matching needle, and the query author may have
/// put frequently matching needles first.
/// Values are compared with `IColumn::compareAt`, as `hasAny` does for non-numeric elements, so a removed
/// value matches the same haystack elements as the kept one. `compareAt` treats NaNs as equal, which is fine
/// because a NaN needle never matches.
ColumnPtr removeDuplicatesKeepingOrder(const ColumnPtr & values)
{
    IColumn::Permutation order;
    values->getPermutation(IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Stable, 0, 1, order);

    /// Equal values are adjacent in `order`, and the first occurrence of each value comes first because the sort is stable.
    IColumn::Filter keep(values->size(), 1);
    bool has_duplicates = false;
    for (size_t i = 1; i < order.size(); ++i)
    {
        if (values->compareAt(order[i], order[i - 1], *values, 1) == 0)
        {
            keep[order[i]] = 0;
            has_duplicates = true;
        }
    }

    if (!has_duplicates)
        return values;
    return values->filter(keep, -1);
}

/// Collects the arguments of `or`, looking through nested `or` calls: `(a OR b) OR c` -> `a, b, c`.
void collectOrArguments(const FunctionNode & or_node, QueryTreeNodes & result)
{
    for (const auto & argument : or_node.getArguments().getNodes())
    {
        const auto * argument_function = argument->as<FunctionNode>();
        if (argument_function && argument_function->getFunctionName() == "or")
            collectOrArguments(*argument_function, result);
        else
            result.push_back(argument);
    }
}

class ConvertOrHasAnyChainVisitor : public InDepthQueryTreeVisitorWithContext<ConvertOrHasAnyChainVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertOrHasAnyChainVisitor>;
    using Base::Base;

    ConvertOrHasAnyChainVisitor(FunctionOverloadResolverPtr or_function_resolver_, ContextPtr context)
        : Base(std::move(context))
        , or_function_resolver(std::move(or_function_resolver_))
    {
    }

    /// In a query with aggregation, the expressions after aggregation (e.g. in `SELECT`, `HAVING` and `ORDER BY`)
    /// may use only the `GROUP BY` keys and aggregate functions, and are matched with the keys by structure.
    /// Neither is rewritten: in `SELECT hasAny(a, [1]) OR hasAny(a, [2]) ... GROUP BY hasAny(a, [1]), hasAny(a, [2])`
    /// the merged `hasAny(a, [1, 2])` would need `a`, which is not available after aggregation.
    /// Arguments of aggregate functions are calculated before aggregation, and subqueries have their own scope,
    /// so they are rewritten.
    /// The analyzer may share one node between several places of a query, e.g. in `SELECT expr AS k ... WHERE k GROUP BY k`
    /// the `WHERE` and the `GROUP BY` key are the same node. Rewriting it (or anything below it) from `WHERE` would also
    /// rewrite the key, so the nodes that are reachable from the expressions after aggregation are not rewritten
    /// wherever they are reached from.
    /// The setting is checked in the context of each subquery, which may enable the optimization when the outer query
    /// disables it, so the whole tree is visited. The nodes after aggregation are collected in every query regardless
    /// of the setting, so that the check of the enclosing queries in `isPostAggregationNode` does not depend on their settings.
    void enterImpl(QueryTreeNodePtr & node)
    {
        bool can_rewrite = can_rewrite_stack.empty() || can_rewrite_stack.back();
        if (node->getNodeType() == QueryTreeNodeType::QUERY || node->getNodeType() == QueryTreeNodeType::UNION)
            can_rewrite = true;
        else if (const auto * function_node = node->as<FunctionNode>(); function_node && function_node->isAggregateFunction())
            can_rewrite = true;
        if (isPostAggregationNode(node.get()))
            can_rewrite = false;
        can_rewrite_stack.push_back(can_rewrite);

        if (auto * query_node = node->as<QueryNode>())
        {
            auto & post_aggregation_nodes = post_aggregation_nodes_stack.emplace_back();
            if (query_node->hasGroupBy() || hasAggregateFunctionNodes(node))
            {
                /// Only the join tree (including `JOIN ON` and `ARRAY JOIN`), `PREWHERE` and `WHERE` are calculated before aggregation.
                for (const auto & child : query_node->getChildren())
                    if (child && child != query_node->getJoinTreeNode() && child != query_node->getPrewhere() && child != query_node->getWhere())
                        collectPostAggregationNodes(child, post_aggregation_nodes);
            }
        }

        if (can_rewrite && isEnabled())
            rewriteOrChain(node);
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        can_rewrite_stack.pop_back();

        if (node->getNodeType() == QueryTreeNodeType::QUERY)
            post_aggregation_nodes_stack.pop_back();
    }

private:
    bool isEnabled() const
    {
        return getSettings()[Setting::optimize_or_has_any_chain];
    }

    /// Collects the nodes of an expression calculated after aggregation, except the arguments of aggregate functions and subqueries.
    static void collectPostAggregationNodes(const QueryTreeNodePtr & node, std::unordered_set<const IQueryTreeNode *> & result)
    {
        if (!node || node->getNodeType() == QueryTreeNodeType::QUERY || node->getNodeType() == QueryTreeNodeType::UNION)
            return;

        if (const auto * function_node = node->as<FunctionNode>(); function_node && function_node->isAggregateFunction())
            return;

        if (!result.insert(node.get()).second)
            return;

        for (const auto & child : node->getChildren())
            collectPostAggregationNodes(child, result);
    }

    bool isPostAggregationNode(const IQueryTreeNode * node) const
    {
        return std::any_of(post_aggregation_nodes_stack.begin(), post_aggregation_nodes_stack.end(),
            [node](const auto & post_aggregation_nodes) { return post_aggregation_nodes.contains(node); });
    }

    void rewriteOrChain(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "or")
            return;

        /// `hasAny` calls of the `OR` that share the haystack and the type of the constant needles array.
        struct Group
        {
            QueryTreeNodePtr haystack;
            DataTypePtr needles_type;
            /// Elements of the needle arrays of all calls in the group. They are copied as columns, not as `Field`s,
            /// because a `Field` does not keep every value exactly (e.g. the types of `JSON` paths).
            MutableColumnPtr needles;
            /// False if any of the needle constants is folded from a non-deterministic expression (e.g. `hostName`).
            bool is_deterministic = true;
            size_t first_argument_index = 0;
            size_t size = 0;
        };

        static constexpr size_t no_group = static_cast<size_t>(-1);

        /// Nested `OR`s (e.g. from parentheses) are flattened, so that `hasAny` calls at different
        /// nesting levels can be merged. The flattened form is used only if some calls are merged.
        QueryTreeNodes arguments;
        collectOrArguments(*function_node, arguments);

        std::vector<Group> groups;
        std::vector<size_t> argument_to_group(arguments.size(), no_group);
        QueryTreeNodePtrWithHashMap<std::vector<size_t>> haystack_to_groups;

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            auto * argument_function = arguments[i]->as<FunctionNode>();
            if (!argument_function || argument_function->getFunctionName() != "hasAny")
                continue;

            const auto & has_any_arguments = argument_function->getArguments().getNodes();
            if (has_any_arguments.size() != 2)
                continue;

            const auto * needles_node = has_any_arguments[1]->as<ConstantNode>();
            if (!needles_node || !isArray(needles_node->getResultType()))
                continue;

            const auto & haystack = has_any_arguments[0];
            if (isExpressionNonDeterministic(haystack))
                continue;

            /// The merged array keeps the type of the original needles. Constants of different types are not merged,
            /// because their common supertype may not exist or may not be comparable with the haystack.
            const auto & needles_type = needles_node->getResultType();
            const auto & needles = assert_cast<const ColumnArray &>(needles_node->getColumn()->getDataColumn());

            auto & haystack_groups = haystack_to_groups[haystack];
            auto group_it = std::find_if(haystack_groups.begin(), haystack_groups.end(),
                [&](size_t group_index) { return groups[group_index].needles_type->equals(*needles_type); });

            size_t group_index = 0;
            if (group_it == haystack_groups.end())
            {
                group_index = groups.size();
                haystack_groups.push_back(group_index);
                groups.push_back(Group{.haystack = haystack, .needles_type = needles_type, .needles = needles.getData().cloneEmpty(), .first_argument_index = i, .size = 0});
            }
            else
            {
                group_index = *group_it;
            }

            auto & group = groups[group_index];
            group.needles->insertRangeFrom(needles.getData(), 0, needles.getSize(0));
            group.is_deterministic &= needles_node->isDeterministic();
            ++group.size;
            argument_to_group[i] = group_index;
        }

        if (std::none_of(groups.begin(), groups.end(), [](const Group & group) { return group.size > 1; }))
            return;

        QueryTreeNodes new_arguments;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const size_t group_index = argument_to_group[i];
            if (group_index == no_group || groups[group_index].size == 1)
            {
                new_arguments.push_back(arguments[i]);
                continue;
            }

            auto & group = groups[group_index];
            if (group.first_argument_index != i)
                continue;

            auto needles = removeDuplicatesKeepingOrder(std::move(group.needles));
            auto needles_array = ColumnArray::create(needles, ColumnArray::ColumnOffsets::create(1, needles->size()));

            auto has_any_function = std::make_shared<FunctionNode>("hasAny");
            auto & has_any_arguments = has_any_function->getArguments().getNodes();
            has_any_arguments.push_back(group.haystack);
            has_any_arguments.push_back(std::make_shared<ConstantNode>(
                ConstantValue{ColumnConst::create(std::move(needles_array), 1), group.needles_type},
                /*source_expression=*/nullptr,
                group.is_deterministic));
            has_any_function->resolveAsFunction(getHasAnyFunctionResolver());
            new_arguments.push_back(std::move(has_any_function));
        }

        if (new_arguments.size() == 1)
        {
            /// All arguments are merged into one `hasAny` call, which replaces the `OR` if the result types match.
            if (new_arguments[0]->getResultType()->equals(*function_node->getResultType()))
            {
                node = std::move(new_arguments[0]);
                return;
            }

            /// Otherwise keep the `OR`, which needs at least two arguments, by adding a stub `0`.
            new_arguments.push_back(std::make_shared<ConstantNode>(static_cast<UInt8>(0), function_node->getResultType()));
        }

        function_node->getArguments().getNodes() = std::move(new_arguments);
        function_node->resolveAsFunction(or_function_resolver);
    }

    const FunctionOverloadResolverPtr & getHasAnyFunctionResolver()
    {
        if (!has_any_function_resolver)
            has_any_function_resolver = FunctionFactory::instance().get("hasAny", getContext());
        return has_any_function_resolver;
    }

    const FunctionOverloadResolverPtr or_function_resolver;
    FunctionOverloadResolverPtr has_any_function_resolver;

    /// Whether an `OR` can be rewritten, for each node on the path from the root to the current node.
    std::vector<bool> can_rewrite_stack;
    /// For each query node on the path, the nodes of its expressions that are calculated after aggregation.
    std::vector<std::unordered_set<const IQueryTreeNode *>> post_aggregation_nodes_stack;
};

}

void ConvertOrHasAnyChainPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    ConvertOrHasAnyChainVisitor visitor(createInternalFunctionOrOverloadResolver(), std::move(context));
    visitor.visit(query_tree_node);
}

}
