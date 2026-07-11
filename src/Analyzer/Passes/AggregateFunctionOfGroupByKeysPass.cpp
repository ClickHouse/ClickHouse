#include <Analyzer/Passes/AggregateFunctionOfGroupByKeysPass.h>

#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

#include <DataTypes/DataTypeLowCardinality.h>


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

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_aggregators_of_group_by_keys])
            return;

        /// Collect group by keys.
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

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

        if (node->getNodeType() == QueryTreeNodeType::FUNCTION)
        {
            auto * function_node = node->as<FunctionNode>();
            if (aggregationCanBeEliminated(node, group_by_keys_stack.back()))
            {
                node = function_node->getArguments().getNodes()[0];
            }
            else if (function_node->isOrdinaryFunction())
            {
                /// A child aggregate may have been replaced by its argument, which for a
                /// LowCardinality group by key has a different type than the eliminated aggregate
                /// (min(LowCardinality(String)) is String, but the key is LowCardinality(String)).
                /// Re-resolve this ordinary function against its current argument types so the type
                /// stays consistent and predicates like `key = 'x'` keep operating directly on the
                /// key column, letting them push down to storage and use skip indexes.
                reresolveIfArgumentTypesChanged(*function_node);
            }
        }
        else if (node->getNodeType() == QueryTreeNodeType::QUERY)
        {
            /// Projection nodes are rewritten before we leave the query node (post-order visit), so
            /// a projection column that was an eliminated/re-resolved aggregate may now have a
            /// different type. Refresh QueryNode::projection_columns so QueryNode::getResultType()
            /// reports the true post-rewrite type; otherwise a correlated scalar subquery over a
            /// LowCardinality key would report the stale analyzed type and
            /// PlannerCorrelatedSubqueries::addStepForResultRenaming would throw on the header mismatch.
            refreshProjectionColumnTypes(*node->as<QueryNode>());
            group_by_keys_stack.pop_back();
        }
    }

    static bool needChildVisit(VisitQueryTreeNodeType & parent [[maybe_unused]], VisitQueryTreeNodeType & child)
    {
        /// Skip ArrayJoin.
        return !child->as<ArrayJoinNode>();
    }

private:

    struct NodeWithInfo
    {
        QueryTreeNodePtr node;
        bool parents_are_only_deterministic = false;
    };

    /// Whether a function argument node has a well-defined result type that can be compared. Only a
    /// correlated QUERY/UNION exposes one (see getResultType() on QueryNode/UnionNode); a plain
    /// (non-correlated) subquery, list, etc. does not.
    static bool argumentExposesResultType(const IQueryTreeNode & argument_node)
    {
        switch (argument_node.getNodeType())
        {
            case QueryTreeNodeType::FUNCTION:
            case QueryTreeNodeType::COLUMN:
            case QueryTreeNodeType::CONSTANT:
                return true;
            case QueryTreeNodeType::QUERY:
                return argument_node.as<QueryNode &>().isCorrelated();
            case QueryTreeNodeType::UNION:
                return argument_node.as<UnionNode &>().isCorrelated();
            default:
                return false;
        }
    }

    /// Re-resolve an ordinary function if any argument's current result type no longer matches the
    /// type the function was resolved with (a child aggregate was replaced by its differently-typed
    /// argument, possibly inside a correlated scalar subquery). Cascades upward as parents are
    /// visited in leaveImpl.
    void reresolveIfArgumentTypesChanged(FunctionNode & function_node)
    {
        const auto & resolved_argument_types = function_node.getArgumentTypes();
        const auto & argument_nodes = function_node.getArguments().getNodes();
        if (resolved_argument_types.size() != argument_nodes.size())
            return;

        bool argument_types_changed = false;
        for (size_t i = 0; i < argument_nodes.size(); ++i)
        {
            /// Skip arguments that do not expose a result type. FUNCTION/COLUMN/CONSTANT always do
            /// and may have been substituted by this pass (an eliminated aggregate is replaced by
            /// its argument). A QUERY/UNION argument exposes a result type only when it is a
            /// correlated scalar subquery, whose type can also flip if its projection was an
            /// eliminated aggregate over a LowCardinality key; a non-correlated subquery (e.g. an
            /// IN subquery) throws from getResultType() and is never rewritten, so skip it. ListNode
            /// and other kinds have no result type either.
            if (!argumentExposesResultType(*argument_nodes[i]))
                continue;

            if (!resolved_argument_types[i] || !resolved_argument_types[i]->equals(*argument_nodes[i]->getResultType()))
            {
                argument_types_changed = true;
                break;
            }
        }

        if (argument_types_changed)
            resolveOrdinaryFunctionNodeByName(function_node, function_node.getFunctionName(), getContext());
    }

    /// Re-derive projection column types from the (already rewritten) projection nodes so the query
    /// node's result-type metadata stays consistent with what the planner will build. Names are kept;
    /// only types are refreshed. No-op for a query node whose projection types did not change.
    static void refreshProjectionColumnTypes(QueryNode & query_node)
    {
        if (!query_node.isResolved())
            return;

        const auto & projection_nodes = query_node.getProjection().getNodes();
        const auto & projection_columns = query_node.getProjectionColumns();
        if (projection_nodes.size() != projection_columns.size())
            return;

        NamesAndTypes refreshed_columns = projection_columns;
        bool changed = false;
        for (size_t i = 0; i < projection_nodes.size(); ++i)
        {
            /// Skip projection nodes without a well-defined result type (e.g. a not-yet-evaluated
            /// non-correlated scalar subquery) so getResultType() below never throws.
            if (!argumentExposesResultType(*projection_nodes[i]))
                continue;

            auto projection_type = projection_nodes[i]->getResultType();
            if (projection_type && !projection_type->equals(*refreshed_columns[i].type))
            {
                refreshed_columns[i].type = std::move(projection_type);
                changed = true;
            }
        }

        if (changed)
            query_node.resolveProjectionColumns(std::move(refreshed_columns));
    }

    bool aggregationCanBeEliminated(QueryTreeNodePtr & node, const QueryTreeNodePtrWithHashSet & group_by_keys)
    {
        if (group_by_keys.empty())
            return false;

        auto * function = node->as<FunctionNode>();
        if (!function || !function->isAggregateFunction())
            return false;

        if (!(function->getFunctionName() == "min"
                || function->getFunctionName() == "max"
                || function->getFunctionName() == "any"
                || function->getFunctionName() == "anyLast"))
            return false;

        std::vector<NodeWithInfo> candidates;
        auto & function_arguments = function->getArguments().getNodes();
        if (function_arguments.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a single argument of function '{}' but received {}", function->getFunctionName(), function_arguments.size());

        /// The aggregate result type must match the argument type up to LowCardinality: aggregate
        /// functions always strip LowCardinality from their result (min(LowCardinality(String)) ->
        /// String), so comparing the raw types would make this pass dead for LowCardinality keys.
        /// The type mismatch introduced by dropping the aggregate is repaired by re-resolving the
        /// parent ordinary functions in leaveImpl.
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
};

}

void AggregateFunctionOfGroupByKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    EliminateFunctionVisitor eliminator(context);
    eliminator.visit(query_tree_node);
}

};
