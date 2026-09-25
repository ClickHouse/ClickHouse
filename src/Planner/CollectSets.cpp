#include <Analyzer/IQueryTreeNode.h>
#include <Planner/CollectSets.h>

#include <Storages/StorageSet.h>
#if CLICKHOUSE_CLOUD
#include <Storages/StorageSharedSetJoin.h>
#endif

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/SetUtils.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <Interpreters/misc.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Set.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerUncorrelatedSubqueries.h>

#include <unordered_set>


namespace DB
{
namespace Setting
{
    extern const SettingsBool transform_null_in;
    extern const SettingsBool validate_enum_literals_in_operators;
}

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class CollectSetsVisitor : public InDepthQueryTreeVisitorWithContext<CollectSetsVisitor>
{
public:
    CollectSetsVisitor(
        PlannerContext & planner_context_, std::vector<QueryTreeNodePtr> & pending_source_expressions_, bool visits_planned_query_)
        : InDepthQueryTreeVisitorWithContext(planner_context_.getQueryContext())
        , planner_context(planner_context_)
        , pending_source_expressions(pending_source_expressions_)
        , visits_planned_query(visits_planned_query_)
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        enterJoinRewriteScope(node);

        if (const auto * constant_node = node->as<ConstantNode>())
            /// Collect sets from source expression as well.
            /// Most likely we will not build them, but those sets could be requested during analysis.
            /// A source expression is not a query tree child, and a chain of them can be arbitrarily
            /// long, so it is queued for a separate visit rather than descended into from here.
            if (constant_node->hasSourceExpression())
                pending_source_expressions.push_back(constant_node->getSourceExpression());

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !isNameOfInFunction(function_node->getFunctionName()))
            return;

        if (function_node->getArguments().getNodes().size() < 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function '{}' is expected to have at least 2 arguments, got {}",
                function_node->getFunctionName(),
                function_node->getArguments().getNodes().size());

        /// The planner evaluates this `IN` with a join, so it needs no set. This is the only occurrence
        /// that leaves without building one: an occurrence in a blocked scope takes the rewrite back and
        /// builds the set on its way through here.
        if (auto in_to_join_rewrite_scope = getUnblockedJoinRewriteScope(node))
        {
            if (canRewriteInToJoin(node, query_node, planner_context))
            {
                planner_context.addInSubqueryForJoinRewrite(*in_to_join_rewrite_scope, node);
                return;
            }
        }

        auto in_first_argument = function_node->getArguments().getNodes().at(0);
        auto in_second_argument = function_node->getArguments().getNodes().at(1);
        auto in_second_argument_node_type = in_second_argument->getNodeType();

        const auto & settings = planner_context.getQueryContext()->getSettingsRef();
        auto & sets = planner_context.getPreparedSets();

        /// Tables and table functions are replaced with subquery at Analysis stage, except special Set table.
        auto * second_argument_table = in_second_argument->as<TableNode>();
        StorageSet * storage_set = second_argument_table != nullptr ? dynamic_cast<StorageSet *>(second_argument_table->getStorage().get()) : nullptr;

        if (storage_set)
        {
            /// Handle storage_set as ready set.
            auto set_key = in_second_argument->getTreeHash({.ignore_cte = true});
            if (sets.findStorage(set_key))
                return;
            auto ast = in_second_argument->toAST();
            sets.addFromStorage(set_key, std::move(ast), storage_set->getSet(), second_argument_table->getStorageID());
        }
        else if (const auto * constant_node = in_second_argument->as<ConstantNode>())
        {
            auto set = getSetElementsForConstantValue(
                in_first_argument->getResultType(), constant_node->getColumn(), constant_node->getResultType(),
                GetSetElementParams{
                    .transform_null_in = settings[Setting::transform_null_in],
                    .forbid_unknown_enum_values = settings[Setting::validate_enum_literals_in_operators],
                });

            if (set.empty())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Function '{}' second argument evaluated to Block with no columns",
                    function_node->getFunctionName());

            DataTypes set_element_types;
            set_element_types.reserve(set.size());
            /// Get the `set_element_types` from `set` instead of `in_first_argument` because
            /// inside `getSetElementsForConstantValue`, we already do necessary transformation including
            /// getting `dictionaryType` from `DataTypeLowCardinality`. Therefore, we can skip some steps here if
            /// we directly use `set` to get the `set_element_types`.
            for (const auto & elem : set)
                set_element_types.push_back(elem.type);

            set_element_types = Set::getElementTypes(std::move(set_element_types), settings[Setting::transform_null_in]);
            auto set_key = in_second_argument->getTreeHash({.ignore_cte = true});

            if (sets.findTuple(set_key, set_element_types))
                return;

            auto ast = in_second_argument->toAST();
#if CLICKHOUSE_CLOUD
            if (storage_set->getName() == "SharedSet")
                sets.addFromStorage(set_key, std::move(ast), static_cast<StorageSharedSet *>(storage_set)->getSet(planner_context.getQueryContext()), second_argument_table->getStorageID());
            else
#endif
            sets.addFromTuple(set_key, std::move(ast), std::move(set), settings);
        }
        else if (in_second_argument_node_type == QueryTreeNodeType::QUERY ||
            in_second_argument_node_type == QueryTreeNodeType::UNION ||
            in_second_argument_node_type == QueryTreeNodeType::TABLE)
        {
            auto set_key = in_second_argument->getTreeHash({.ignore_cte = true});
            const bool external_table_expected = isNameOfGlobalInFunction(function_node->getFunctionName());

            if (auto subquery_set = sets.findSubquery(set_key))
            {
                if (external_table_expected)
                    subquery_set->markExternalTableExpected();
                return;
            }

            auto subquery_to_execute = in_second_argument;
            if (in_second_argument->as<TableNode>())
                subquery_to_execute = buildSubqueryToReadColumnsFromTableExpression(static_pointer_cast<TableNode>(subquery_to_execute), planner_context.getQueryContext());

            auto ast = in_second_argument->toAST({ .set_subquery_cte_name = false });
            auto subquery_set = sets.addFromSubquery(set_key, std::move(ast), std::move(subquery_to_execute), settings);
            if (external_table_expected)
                subquery_set->markExternalTableExpected();
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Function '{}' is supported only if second argument is constant or table expression",
                function_node->getFunctionName());
        }
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        leaveJoinRewriteScope(node);
    }

    static bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr & child_node)
    {
        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

private:
    void enterJoinRewriteScope(const QueryTreeNodePtr & node)
    {
        if (node->as<QueryNode>())
        {
            /// Another query plans its own `IN` with its own context, so this walk leaves them their sets.
            if (visits_planned_query)
                query_node = node;
            return;
        }

        if (!query_node)
            return;

        if (isBlockedScope(node))
            ++blocked_depth;

        /// Every join the rewrite can add is below this scope, so an `IN` here keeps its set - and so does
        /// the same `IN` wherever else the query reads it, which the walk may have taken already.
        if (blocked_depth != 0)
        {
            const auto * function_node = node->as<FunctionNode>();
            if (function_node && isNameOfInFunction(function_node->getFunctionName())
                && blocked_in_subqueries.insert(node.get()).second)
                planner_context.removeInSubqueryForJoinRewrite(node);
            return;
        }

        if (auto step = scopeFor(node))
            scope_stack.push_back(*step);
    }

    void leaveJoinRewriteScope(const QueryTreeNodePtr & node)
    {
        if (!query_node)
            return;

        if (isBlockedScope(node))
        {
            --blocked_depth;
            return;
        }

        if (blocked_depth == 0 && scopeFor(node))
            scope_stack.pop_back();
    }

    /// The step of the chain that computes the expressions of this scope, when it is one the rewrite can insert its join under.
    std::optional<InToJoinScope> scopeFor(const QueryTreeNodePtr & node) const
    {
        const auto & typed_query_node = query_node->as<const QueryNode &>();
        if (node == typed_query_node.getWhere())
            return InToJoinScope::Where;
        if (node == typed_query_node.getGroupByNode())
            return InToJoinScope::Aggregation;
        if (node == typed_query_node.getHaving())
            return InToJoinScope::Having;
        if (node == typed_query_node.getWindowNode())
            return InToJoinScope::Window;
        if (node == typed_query_node.getQualify())
            return InToJoinScope::Qualify;
        if (node == typed_query_node.getProjectionNode())
            return InToJoinScope::Projection;
        if (node == typed_query_node.getOrderByNode())
            return InToJoinScope::OrderBy;
        if (node == typed_query_node.getLimitByNode())
            return InToJoinScope::LimitBy;

        if (const auto * function_node = node->as<FunctionNode>())
        {
            if (function_node->isAggregateFunction())
                return InToJoinScope::Aggregation;
            if (function_node->isWindowFunction())
                return InToJoinScope::Window;
        }

        return {};
    }

    /// The step the rewrite would insert the join for `node` under, or nothing when a scope is blocked.
    std::optional<InToJoinScope> getUnblockedJoinRewriteScope(const QueryTreeNodePtr & node) const
    {
        if (blocked_in_subqueries.contains(node.get()) || scope_stack.empty())
            return {};

        return scope_stack.back();
    }

    /// A scope whose expressions no join the rewrite can add is below, so an `IN` here is evaluated with
    /// a set and cannot be rewritten anywhere else in the query either.
    bool isBlockedScope(const QueryTreeNodePtr & node) const
    {
        /// A lambda parameter is not a column of the plan header, so there is nothing to join on.
        if (node->getNodeType() == QueryTreeNodeType::LAMBDA)
            return true;

        const auto & typed_query_node = query_node->as<const QueryNode &>();
        if (node == typed_query_node.getPrewhere() || node == typed_query_node.getJoinTreeNode()
            || node == typed_query_node.getInterpolate())
            return true;

        const auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return false;

        /// `indexHint` builds its arguments into a dag that has no join above it.
        return function_node->getFunctionName() == "indexHint";
    }

    PlannerContext & planner_context;
    std::vector<QueryTreeNodePtr> & pending_source_expressions;

    /// Whether the visit is rooted at the query this context plans.
    const bool visits_planned_query;
    /// The query being visited.
    QueryTreeNodePtr query_node;
    /// The scopes being walked, innermost last.
    std::vector<InToJoinScope> scope_stack;
    int blocked_depth = 0;
    /// IN subqueries that occurred in a blocked scope.
    std::unordered_set<const IQueryTreeNode *> blocked_in_subqueries;
};

}

void collectSets(const QueryTreeNodePtr & node, PlannerContext & planner_context)
{
    std::vector<QueryTreeNodePtr> pending_source_expressions{node};
    /// Every link of a `DEFAULT` chain is reachable from each link above it, so without this the
    /// worklist would visit link `i` once per link above it: `2 * N^2` root visits for a chain of N
    /// columns, against `4 * N` with it. Skipping a repeat visit changes nothing, because every set
    /// registration in `enterImpl` is already guarded by `findTuple`, `findStorage` or `findSubquery`.
    std::unordered_set<QueryTreeNodePtr> visited_source_expressions;

    while (!pending_source_expressions.empty())
    {
        auto node_to_visit = pending_source_expressions.back();
        pending_source_expressions.pop_back();

        if (!visited_source_expressions.insert(node_to_visit).second)
            continue;

        /// Each pending node is visited as a root: needChildVisit refuses QUERY and UNION
        /// children, so a source expression that is itself a query must start its own visit.
        CollectSetsVisitor visitor(planner_context, pending_source_expressions, node_to_visit == node);
        visitor.visit(node_to_visit);
    }
}

}
