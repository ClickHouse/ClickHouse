#include "MonotonousFunctionsInOrderByPass.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/SortNode.h>

namespace DB
{

namespace
{

void collectGroupByNodes(const QueryNode * query_node, QueryTreeNodes & group_by_nodes)
{
    if (query_node->hasGroupBy())
    {
        const auto & group_by = query_node->getGroupBy().getNodes();
        for (const auto & group_by_ele : group_by)
            /// For grouping set
            if (auto * list = group_by_ele->as<ListNode>())
            {
                for (auto & elem : list->getNodes())
                {
                    group_by_nodes.push_back(elem);
                }
            }
            else
            {
                group_by_nodes.push_back(group_by_ele);
            }
    }
}

class MonotonousFunctionsChecker : public InDepthQueryTreeVisitor<MonotonousFunctionsChecker>
{
public:
    using Base = InDepthQueryTreeVisitor<MonotonousFunctionsChecker>;
    using Base::Base;

    /// Whether function chains is monotonous currently.
    /// For example: function chains f(m(n(x))) and now we prepare to visit n
    /// if both f and m are monotonous then is_monotonic is true else false.
    bool is_monotonic = true;

    /// How many monotonous functions we found.
    size_t monotonous_function_num = 0;

    /// For the monotonous functions we found, whether they provide positive direction.
    /// For example: function chains f(m(x)), f is positive and m is not, then is_positive = false.
    /// It is used to determine whether we should change the sort direction if we remove the functions.
    bool is_positive = true;

    /// Group by nodes for the query.
    const QueryTreeNodes & group_by_nodes;

    /// If order by expression matches the sorting key, do not remove
    /// functions to allow execute reading in order of key.
    ///
    /// For example: sorting_key = 'm(x)' and
    ///     1. order by node = m(x): remove nothing
    ///     2. order by node = f(m(n(x))): remove f()
    bool should_checking_sorting_key;
    const String & sorting_key;

    explicit MonotonousFunctionsChecker(const QueryTreeNodes & group_by_nodes_, bool should_checking_sorting_key_, const String & sorting_key_)
        : group_by_nodes(group_by_nodes_), should_checking_sorting_key(should_checking_sorting_key_), sorting_key(sorting_key_)
    {
    }

    bool needChildVisit(VisitQueryTreeNodeType &, VisitQueryTreeNodeType &) const
    {
        return is_monotonic;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (!is_monotonic)
            return;

        if (auto * function_node = node->as<FunctionNode>())
        {
            if (function_node->getArguments().getNodes().size() != 1)
            {
                is_monotonic = false;
                return;
            }

            if (function_node->isAggregateFunction() || function_node->isWindowFunction())
            {
                is_monotonic = false;
                return;
            }

            const auto & function_base = function_node->getFunction();
            if (!function_base->hasInformationAboutMonotonicity())
            {
                is_monotonic = false;
                return;
            }

            /// Skip if node exists in group by nodes
            for (const auto & group_by_node : group_by_nodes)
            {
                if (group_by_node->isEqual(*node))
                {
                    is_monotonic = false;
                    return;
                }
            }

            /// If order by expression matches the sorting key, do not remove
            /// functions to allow execute reading in order of key.
            IQueryTreeNode::ConvertToASTOptions options{false, false, false};
            if (should_checking_sorting_key && sorting_key == function_node->toAST(options)->getColumnName())
            {
                is_monotonic = false;
                return;
            }

            auto argument_type = function_node->getArguments().getNodes()[0]->getResultType();
            auto monotonicity = function_base->getMonotonicityForRange(*argument_type, Field(), Field());

            is_monotonic = monotonicity.is_monotonic;
            if (monotonicity.is_monotonic)
            {
                monotonous_function_num++;
                is_positive = (is_positive == monotonicity.is_positive);
            }
        }
    }

};

class MonotonousFunctionsVisitor : public InDepthQueryTreeVisitorWithContext<MonotonousFunctionsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<MonotonousFunctionsVisitor>;
    using Base::Base;

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_monotonous_functions_in_order_by)
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        if (!query_node->hasOrderBy())
            return;

        if (auto * table_node = query_node->getJoinTree()->as<TableNode>())
        {
            /// Do not apply optimization for Distributed and Merge storages,
            /// because we can't get the sorting key of their underlying tables
            /// and we can break the matching of the sorting key for `read_in_order`
            /// optimization by removing monotonous functions from the prefix of key.
            if (table_node->getStorage()->isRemote() || table_node->getStorage()->getName() == "Merge")
                return;
        }

        /// Skip if with fill
        auto & order_by_nodes = query_node->getOrderBy().getNodes();
        for (const auto & child : order_by_nodes)
        {
            auto * order_by_ele = child->as<SortNode>();
            if (order_by_ele->withFill())
                return;
        }

        /// Collect group by nodes
        QueryTreeNodes group_by_nodes;
        collectGroupByNodes(query_node, group_by_nodes);

        Names sorting_key_columns;
        if (auto * table_node = query_node->getJoinTree()->as<TableNode>())
            sorting_key_columns = table_node->getStorageSnapshot()->metadata->getSortingKeyColumns();

        bool is_sorting_key_prefix = true;
        for (size_t i = 0; i < order_by_nodes.size(); ++i)
        {
            auto & expr_node = order_by_nodes[i]->as<SortNode>()->getExpression();
            auto * expr_function = expr_node->as<FunctionNode>();

            if (!expr_function)
                continue;

            MonotonousFunctionsChecker checker(
                group_by_nodes,
                is_sorting_key_prefix && i < sorting_key_columns.size(),
                i >= sorting_key_columns.size() ? "" : sorting_key_columns[i]);

            checker.visit(expr_node);

            if (checker.monotonous_function_num > 0)
            {
                /// Replace function with its first argument
                for (size_t num = 0; num < checker.monotonous_function_num; num++)
                {
                    expr_node = expr_function->getArguments().getNodes()[0];
                    expr_function = expr_node->as<FunctionNode>();
                }

                /// Update sort direction.
                if (!checker.is_positive)
                {
                    auto * sort_node = order_by_nodes[i]->as<SortNode>();
                    SortDirection reverted = static_cast<SortDirection>(!static_cast<bool>(sort_node->getSortDirection()));

                    order_by_nodes[i] = std::make_shared<SortNode>(
                        sort_node->getExpression(),
                        reverted,
                        sort_node->getNullsSortDirection(),
                        sort_node->getCollator(),
                        sort_node->withFill());
                }
            }

            IQueryTreeNode::ConvertToASTOptions options{false, false, false}; /// TODO not a strict way
            auto * sort_node = order_by_nodes[i]->as<SortNode>();

            /// We should update is_sorting_key_prefix at last, for node may changed.
            if (i >= sorting_key_columns.size() || sort_node->getExpression()->toAST(options)->getColumnName() != sorting_key_columns[i]
                || sort_node->getSortDirection() != SortDirection::ASCENDING)
                is_sorting_key_prefix = false;
        }

    }
};

}

void MonotonousFunctionsInOrderByPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    MonotonousFunctionsVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
