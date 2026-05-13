#include <Analyzer/Passes/CountDistinctPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Interpreters/Context.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/IStorage.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool count_distinct_optimization;
}

namespace
{

class CountDistinctVisitor : public InDepthQueryTreeVisitorWithContext<CountDistinctVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CountDistinctVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::count_distinct_optimization])
            return;

        auto * query_node = node->as<QueryNode>();

        /// Check that query has only SELECT clause
        if (!query_node || (query_node->hasWith() || query_node->hasPrewhere() || query_node->hasWhere() || query_node->hasGroupBy() ||
            query_node->hasHaving() || query_node->hasWindow() || query_node->hasQualify() || query_node->hasOrderBy() || query_node->hasLimitByLimit() || query_node->hasLimitByOffset() ||
            query_node->hasLimitBy() || query_node->hasLimit() || query_node->hasOffset()))
            return;

        /// Check that query has only single table expression
        auto join_tree_node_type = query_node->getJoinTree()->getNodeType();
        if (join_tree_node_type == QueryTreeNodeType::JOIN || join_tree_node_type == QueryTreeNodeType::CROSS_JOIN || join_tree_node_type == QueryTreeNodeType::ARRAY_JOIN)
            return;

        /// Check only local table. The rewrite must not apply to remote storages
        /// (e.g. `Distributed` or `remote(...)` table function), where distributed
        /// aggregation already handles `count(DISTINCT)` correctly.
        auto & join_tree = query_node->getJoinTree();
        if (auto * table_node = join_tree->as<TableNode>())
        {
            if (table_node->getStorage()->isRemote())
                return;
        }
        else if (auto * table_function_node = join_tree->as<TableFunctionNode>())
        {
            if (table_function_node->getStorageOrThrow()->isRemote())
                return;
        }

        /// Check that query has only single node in projection
        auto & projection_nodes = query_node->getProjection().getNodes();
        if (projection_nodes.size() != 1)
            return;

        /// Check that query single projection node is `countDistinct` function
        auto & projection_node = projection_nodes[0];
        auto * function_node = projection_node->as<FunctionNode>();
        if (!function_node || function_node->hasWindow())
            return;

        auto lower_function_name = Poco::toLower(function_node->getFunctionName());
        if (lower_function_name != "countdistinct" && lower_function_name != "uniqexact")
            return;

        /// Check that `countDistinct` function has single COLUMN argument
        auto & count_distinct_arguments_nodes = function_node->getArguments().getNodes();
        if (count_distinct_arguments_nodes.size() != 1 || count_distinct_arguments_nodes[0]->getNodeType() != QueryTreeNodeType::COLUMN)
            return;

        auto & count_distinct_argument_column = count_distinct_arguments_nodes[0];
        if (count_distinct_argument_column->getNodeType() != QueryTreeNodeType::COLUMN)
            return;
        auto & count_distinct_argument_column_typed = count_distinct_argument_column->as<ColumnNode &>();

        /// Skip numeric-like columns: the `uniqExact` aggregator has highly tuned
        /// open-addressing hash tables for fixed-size numeric values, and the
        /// extra block materialization between the inner `GROUP BY` and the outer
        /// `count()` makes the rewrite slower in multi-threaded execution.
        /// `String`, `FixedString`, `Array`, etc. still benefit because the
        /// specialized `GROUP BY` hash tables outperform the generic ones used
        /// inside `uniqExact` for those types.
        /// Unwrap `Nullable` and `LowCardinality(Nullable(...))` wrappers first:
        /// `DataTypeNullable` does not override `isValueRepresentedByNumber`, so
        /// without this `Nullable(UInt64)` and `LowCardinality(Nullable(UInt32))`
        /// would slip through the numeric gate.
        auto column_type_unwrapped = removeNullableOrLowCardinalityNullable(count_distinct_argument_column_typed.getColumnType());
        if (column_type_unwrapped->isValueRepresentedByNumber())
            return;

        /// Build subquery SELECT count_distinct_argument_column FROM table_expression GROUP BY count_distinct_argument_column
        auto subquery = std::make_shared<QueryNode>(Context::createCopy(query_node->getContext()));
        subquery->getJoinTree() = query_node->getJoinTree();
        subquery->getProjection().getNodes().push_back(count_distinct_argument_column);
        subquery->getGroupBy().getNodes().push_back(count_distinct_argument_column);
        subquery->resolveProjectionColumns({count_distinct_argument_column_typed.getColumn()});

        /// Put subquery into JOIN TREE of initial query
        query_node->getJoinTree() = std::move(subquery);

        /// Replace `countDistinct` of initial query into `count`
        auto result_type = function_node->getResultType();

        function_node->getArguments().getNodes().clear();
        resolveAggregateFunctionNodeByName(*function_node, "count");
    }
};

}

void CountDistinctPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    CountDistinctVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
