#include <Analyzer/Passes/RewriteArrayJoinCountPass.h>

#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

#include <DataTypes/DataTypesNumber.h>

#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/Context.h>

#include <Poco/String.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_unaligned_array_join;
    extern const SettingsBool optimize_functions_to_subcolumns;
}

namespace
{

/// Returns true when the aggregate is a plain `count()` whose result equals the number of rows,
/// regardless of any column value: zero arguments (count(), count(*)) or only non-NULL constant
/// arguments (count(1), count('x')). count(column) is excluded because it counts non-NULL values,
/// and count(NULL) is excluded because it always returns 0 (a NULL argument is never counted).
bool isPlainRowCount(const FunctionNode & function_node)
{
    if (!function_node.isAggregateFunction() || function_node.isWindowFunction())
        return false;
    if (Poco::toLower(function_node.getFunctionName()) != "count")
        return false;

    for (const auto & argument : function_node.getArguments().getNodes())
    {
        const auto * constant_node = argument->as<ConstantNode>();
        if (!constant_node || constant_node->getValue().isNull())
            return false;
    }
    return true;
}

class RewriteArrayJoinCountVisitor : public InDepthQueryTreeVisitorWithContext<RewriteArrayJoinCountVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteArrayJoinCountVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        const auto & settings = getSettings();
        if (!settings[Setting::optimize_functions_to_subcolumns])
            return;
        /// With unaligned array join, the row count is the maximum length across joined arrays, not the
        /// single array's length, so PruneArrayJoinColumnsPass leaves multiple expressions in place.
        if (settings[Setting::enable_unaligned_array_join])
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        /// Only a bare `SELECT count() FROM ... ARRAY JOIN ...`: any other clause could make the row
        /// count depend on values (GROUP BY, DISTINCT, HAVING) or the exploded element be referenced
        /// (WHERE/PREWHERE/ORDER BY/window/qualify), so we require none of them.
        if (query_node->hasWith() || query_node->hasPrewhere() || query_node->hasWhere()
            || query_node->hasGroupBy() || query_node->hasHaving() || query_node->hasWindow()
            || query_node->hasQualify() || query_node->hasOrderBy() || query_node->hasInterpolate()
            || query_node->hasLimitByLimit() || query_node->hasLimitByOffset() || query_node->hasLimitBy()
            || query_node->hasLimit() || query_node->hasOffset() || query_node->isDistinct())
            return;

        /// Exactly one projection column, which is a plain count().
        auto & projection_nodes = query_node->getProjection().getNodes();
        if (projection_nodes.size() != 1)
            return;

        auto * count_function = projection_nodes[0]->as<FunctionNode>();
        if (!count_function || !isPlainRowCount(*count_function))
            return;

        /// The join tree must be a single ARRAY JOIN directly over a table.
        auto * array_join_node = query_node->getJoinTree()->as<ArrayJoinNode>();
        if (!array_join_node)
            return;

        auto * table_node = array_join_node->getTableExpression()->as<TableNode>();
        if (!table_node)
            return;

        /// Exactly one surviving joined expression. For a value-unused count(), PruneArrayJoinColumnsPass
        /// (which runs before this pass) has already collapsed a multi-expression ARRAY JOIN to a single
        /// expression, so this both matches the target case and declines any residual multi-expression
        /// shape, preserving the upstream row-multiplication semantics.
        auto & join_expressions = array_join_node->getJoinExpressions().getNodes();
        if (join_expressions.size() != 1)
            return;

        /// The joined expression is an outer alias ColumnNode whose child expression holds the actual
        /// joined-over expression. Only rewrite when that inner expression is a plain physical Array/Map
        /// column of the joined table (a computed expression is left untouched).
        auto * join_alias_column = join_expressions[0]->as<ColumnNode>();
        if (!join_alias_column || !join_alias_column->hasExpression())
            return;

        auto * physical_column = join_alias_column->getExpression()->as<ColumnNode>();
        if (!physical_column || physical_column->hasExpression())
            return;

        if (physical_column->getColumnSourceOrNull().get() != array_join_node->getTableExpression().get())
            return;

        if (!getArrayJoinDataType(physical_column->getColumnType()))
            return;

        /// Build length(<physical array/map column>). The subsequent FunctionToSubcolumnsPass folds this
        /// into the lightweight <column>.size0 subcolumn so only offsets are read from storage.
        auto length_function = std::make_shared<FunctionNode>("length");
        length_function->getArguments().getNodes().push_back(join_alias_column->getExpression());
        resolveOrdinaryFunctionNodeByName(*length_function, "length", getContext());

        QueryTreeNodePtr per_row_expression = std::move(length_function);

        /// LEFT ARRAY JOIN emits one row for an empty array, so an empty array contributes 1, not 0.
        if (array_join_node->isLeft())
        {
            auto greatest_function = std::make_shared<FunctionNode>("greatest");
            greatest_function->getArguments().getNodes().push_back(std::move(per_row_expression));
            greatest_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(static_cast<UInt64>(1)));
            resolveOrdinaryFunctionNodeByName(*greatest_function, "greatest", getContext());
            per_row_expression = std::move(greatest_function);
        }

        /// count() over the ARRAY JOIN becomes sum() over the per-row lengths.
        auto sum_function = std::make_shared<FunctionNode>("sum");
        sum_function->getArguments().getNodes().push_back(std::move(per_row_expression));
        resolveAggregateFunctionNodeByName(*sum_function, "sum");

        QueryTreeNodePtr new_projection_node = std::move(sum_function);

        /// Keep the projection column type identical to the original count() (UInt64) so the projection
        /// column metadata (name and type) stays valid without touching projection_columns.
        const auto & count_result_type = count_function->getResultType();
        if (!new_projection_node->getResultType()->equals(*count_result_type))
            new_projection_node = createCastFunction(new_projection_node, count_result_type, getContext());

        projection_nodes[0] = std::move(new_projection_node);

        /// Drop the ARRAY JOIN: the row multiplication is now expressed by sum(length(...)).
        query_node->getJoinTree() = array_join_node->getTableExpression();
    }
};

}

void RewriteArrayJoinCountPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteArrayJoinCountVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
