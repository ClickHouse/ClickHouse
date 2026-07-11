#include <Analyzer/Passes/AggregateFunctionOfGroupByKeysPass.h>

#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/LambdaNode.h>
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

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_aggregators_of_group_by_keys])
            return;

        if (node->getNodeType() == QueryTreeNodeType::LAMBDA)
        {
            /// A lambda body is a computation whose type is cached on LambdaNode::result_type and in
            /// the higher-order parent's DataTypeFunction signature; it is never a storage-pushdown
            /// predicate, even when the lambda syntactically sits inside a filter section. Treat it
            /// like a nested-query boundary: save and reset the filter context so an eliminated
            /// aggregate in the lambda body is cast back to its analyzed type, keeping the lambda body
            /// type (and the parent higher-order signature) unchanged. group_by_keys_stack is left
            /// intact - the lambda body still refers to the enclosing query's GROUP BY keys.
            filter_depth_save_stack.push_back(filter_depth);
            filter_roots_save_stack.push_back(std::move(active_filter_roots));
            filter_depth = 0;
            active_filter_roots.clear();
            return;
        }

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
        {
            /// A filter-section root (WHERE/PREWHERE/HAVING/QUALIFY of the current query) and its
            /// whole subtree is a predicate position: an eliminated aggregate is left as the bare key
            /// there so the predicate pushes down to storage and uses skip indexes. Everywhere else
            /// (projection, ORDER BY, ...) is an output position where the observable type must be
            /// preserved. Track how deep we are inside a filter subtree of the current query.
            if (active_filter_roots.contains(node.get()))
                ++filter_depth;
            return;
        }

        /// A nested query establishes its own output/filter contexts: its projection is an output
        /// position even when the whole subquery sits inside the outer query's WHERE. Save and reset
        /// the filter state so a correlated scalar subquery's projected aggregate is cast back to its
        /// analyzed type (keeping QueryNode::getResultType() equal to the built header).
        filter_depth_save_stack.push_back(filter_depth);
        filter_roots_save_stack.push_back(std::move(active_filter_roots));
        filter_depth = 0;
        active_filter_roots.clear();
        if (query_node->hasPrewhere())
            active_filter_roots.insert(query_node->getPrewhere().get());
        if (query_node->hasWhere())
            active_filter_roots.insert(query_node->getWhere().get());
        if (query_node->hasHaving())
            active_filter_roots.insert(query_node->getHaving().get());
        if (query_node->hasQualify())
            active_filter_roots.insert(query_node->getQualify().get());

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

        /// Capture the node identity and type before any rewrite so a filter-section root that is
        /// itself a function (e.g. WHERE equals(...)) still has its filter_depth decremented below,
        /// and so the decrement stays gated on the pre-rewrite node type.
        const IQueryTreeNode * node_before_rewrite = node.get();
        const auto node_type_before_rewrite = node->getNodeType();

        if (node->getNodeType() == QueryTreeNodeType::FUNCTION)
        {
            auto * function_node = node->as<FunctionNode>();
            if (aggregationCanBeEliminated(node, group_by_keys_stack.back()))
            {
                /// The aggregate result type always strips LowCardinality from the argument
                /// (min(LowCardinality(String)) -> String), so replacing the aggregate with the raw
                /// key would change the node's type from the analyzed type to the key's type.
                auto original_type = function_node->getResultType();
                auto & key_node = function_node->getArguments().getNodes()[0];

                if (filter_depth > 0 || original_type->equals(*key_node->getResultType()))
                {
                    /// In a filter (WHERE/PREWHERE/HAVING/QUALIFY) subtree, leave the bare key so the
                    /// predicate operates directly on the key column and pushes down to storage /
                    /// skip indexes. A type change here is not user-observable. Same bare replacement
                    /// when the types already match (the plain, non-LowCardinality case) - no cast.
                    /// The type flip is absorbed by re-resolving the parent ordinary functions below.
                    node = key_node;
                }
                else
                {
                    /// In an output position (projection, ORDER BY, correlated-subquery projection,
                    /// ...) cast the key back to the aggregate's analyzed type so the observable
                    /// result type is unchanged (e.g. toTypeName(min(s)) stays String, result schemas
                    /// and type-sensitive expressions are preserved, and a correlated scalar
                    /// subquery's QueryNode::getResultType() still matches the built header). Parent
                    /// functions still see the original type, so no re-resolution is needed for them.
                    node = createCastFunction(key_node, std::move(original_type), getContext());
                }
            }
            else if (filter_depth > 0 && function_node->isOrdinaryFunction())
            {
                /// A child aggregate in this filter subtree may have been replaced by its bare key,
                /// whose type differs from the eliminated aggregate (min(LowCardinality(String)) is
                /// String, but the key is LowCardinality(String)). Re-resolve this ordinary function
                /// against its current argument types so it natively consumes the key type and the
                /// predicate keeps pushing down to storage / skip indexes. Cascades upward as parents
                /// are visited. Only needed in filter subtrees; in output positions the key is cast
                /// back to the analyzed type, so parents there never see a changed argument type.
                reresolveIfArgumentTypesChanged(*function_node);
            }
        }
        else if (node->getNodeType() == QueryTreeNodeType::QUERY)
        {
            group_by_keys_stack.pop_back();
            /// Restore the enclosing query's filter context (see enterImpl).
            filter_depth = filter_depth_save_stack.back();
            filter_depth_save_stack.pop_back();
            active_filter_roots = std::move(filter_roots_save_stack.back());
            filter_roots_save_stack.pop_back();
        }
        else if (node->getNodeType() == QueryTreeNodeType::LAMBDA)
        {
            /// Restore the filter context saved when entering the lambda (see enterImpl). The
            /// group_by_keys_stack was not touched, so it is not popped here.
            filter_depth = filter_depth_save_stack.back();
            filter_depth_save_stack.pop_back();
            active_filter_roots = std::move(filter_roots_save_stack.back());
            filter_roots_save_stack.pop_back();
        }

        /// Decrement symmetrically with enterImpl, which increments filter_depth only for nodes that
        /// take the non-QUERY, non-LAMBDA branch. A QUERY or LAMBDA node instead saves and resets the
        /// whole filter context (filter_depth is already restored above), so it never incremented on
        /// entry; decrementing here for such a node when it is itself the outer filter root (e.g.
        /// HAVING is directly a correlated scalar subquery) would underflow filter_depth to
        /// size_t(-1), and the next filter root would wrap it back to 0 and lose the bare-key pushdown.
        if (node_type_before_rewrite != QueryTreeNodeType::QUERY
            && node_type_before_rewrite != QueryTreeNodeType::LAMBDA
            && active_filter_roots.contains(node_before_rewrite))
            --filter_depth;
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

    /// Whether a function argument node exposes a well-defined result type that can be compared.
    /// FUNCTION/COLUMN/CONSTANT always do and are the kinds this pass substitutes (an eliminated
    /// aggregate is replaced by its argument). A QUERY argument exposes one only when it is a
    /// correlated scalar subquery; a non-correlated subquery (e.g. an IN subquery) throws from
    /// getResultType(). UNION never exposes one here: unlike QueryNode, UnionNode has no
    /// getResultType() override and always throws UNSUPPORTED_METHOD (even for a correlated scalar
    /// UNION), so dereferencing it would reintroduce that analyzer failure. This is safe because a
    /// subquery argument's type is never changed by this pass anyway: a nested query resets the
    /// filter context (see enterImpl), so any aggregate eliminated inside a subquery projection is
    /// cast back to its analyzed type and the subquery's result type stays stable. ListNode and
    /// other kinds have no result type.
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
            default:
                return false;
        }
    }

    /// Re-resolve an ordinary function if any argument's current result type no longer matches the
    /// type the function was resolved with (a child aggregate was replaced by its differently-typed
    /// bare key). Cascades upward as parents are visited in leaveImpl. Used only inside filter
    /// subtrees, where the bare key is deliberately left in place to keep predicate pushdown.
    void reresolveIfArgumentTypesChanged(FunctionNode & function_node)
    {
        const auto & resolved_argument_types = function_node.getArgumentTypes();
        auto & argument_nodes = function_node.getArguments().getNodes();
        if (resolved_argument_types.size() != argument_nodes.size())
            return;

        bool argument_types_changed = false;
        for (size_t i = 0; i < argument_nodes.size(); ++i)
        {
            if (!argumentExposesResultType(*argument_nodes[i]))
                continue;

            if (!resolved_argument_types[i] || !resolved_argument_types[i]->equals(*argument_nodes[i]->getResultType()))
            {
                argument_types_changed = true;
                break;
            }
        }

        if (!argument_types_changed)
            return;

        /// A filter section is a predicate position only for functions that are transparent to
        /// LowCardinality (they use the default LC implementation: the framework strips LC, computes
        /// on the nested column and rewraps, so leaving the bare LC key changes only the internal
        /// representation, not the values, and the predicate still pushes down to skip indexes). A
        /// function that OBSERVES LowCardinality (useDefaultImplementationForLowCardinalityColumns()
        /// is false, e.g. toTypeName) would instead change its observable result if re-resolved
        /// against the bare key type - HAVING toTypeName(min(s)) = 'String' would flip from 'String'
        /// to 'LowCardinality(String)' and drop the row. For such a function, cast each changed
        /// argument back to its originally-resolved (LC-stripped) type so the function keeps its
        /// analyzed signature and result; pushdown below the cast is lost, but an LC-observing
        /// function does not push down to storage anyway.
        if (!functionIsTransparentToLowCardinality(function_node))
        {
            for (size_t i = 0; i < argument_nodes.size(); ++i)
            {
                if (!argumentExposesResultType(*argument_nodes[i]))
                    continue;

                if (resolved_argument_types[i] && !resolved_argument_types[i]->equals(*argument_nodes[i]->getResultType()))
                    argument_nodes[i] = createCastFunction(argument_nodes[i], resolved_argument_types[i], getContext());
            }
            return;
        }

        resolveOrdinaryFunctionNodeByName(function_node, function_node.getFunctionName(), getContext());
    }

    /// Whether the resolved ordinary function computes on the LowCardinality nested column (default
    /// LC implementation) rather than observing the LowCardinality wrapper. Transparent functions
    /// (comparisons and most scalar functions) yield the same values on the bare LC key as on the
    /// LC-stripped type, so re-resolving them against the key preserves both results and pushdown.
    /// Non-transparent functions (e.g. toTypeName) must not see the key type. Conservatively treat a
    /// function whose IFunction we cannot reach as non-transparent (fall back to the boundary cast).
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
        /// singleValueOrNull is excluded: it returns NULL unless the group has exactly one distinct
        /// value, so it is not value-preserving; argMin/argMax are two-argument and handled elsewhere.
        const auto & function_name = function->getFunctionName();
        if (!(function_name == "min"
                || function_name == "max"
                || function_name == "any"
                || function_name == "anyLast"
                || function_name == "anyHeavy"
                || function_name == "any_respect_nulls"
                || function_name == "anyLast_respect_nulls"))
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

    /// Roots of the current query's filter sections (WHERE/PREWHERE/HAVING/QUALIFY). filter_depth > 0
    /// means we are inside one of them, i.e. in a predicate position where an eliminated aggregate is
    /// left as the bare key so the filter pushes down to storage. Both are per-query: they are saved
    /// on the stacks below and reset when entering a nested (sub)query, so a correlated subquery's
    /// projection is treated as an output position even inside the outer query's WHERE.
    std::unordered_set<const IQueryTreeNode *> active_filter_roots;
    size_t filter_depth = 0;
    std::vector<std::unordered_set<const IQueryTreeNode *>> filter_roots_save_stack;
    std::vector<size_t> filter_depth_save_stack;
};

}

void AggregateFunctionOfGroupByKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    EliminateFunctionVisitor eliminator(context);
    eliminator.visit(query_tree_node);
}

};
