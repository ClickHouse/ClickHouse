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

#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageProxy.h>

#include <Poco/String.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsBool enable_unaligned_array_join;
    extern const SettingsBool optimize_functions_to_subcolumns;
}

namespace
{

/// Returns true when the aggregate is one of the following forms: `count()`, `count(*)`, `count(1)`, `count('x')`.
/// count(column) and count(NULL) are excluded and return false
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
        /// With unaligned array join, the row count is the maximum length across joined arrays,
        if (settings[Setting::enable_unaligned_array_join])
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        /// Only a bare `SELECT count() FROM ... ARRAY JOIN ...`
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

        /// Only a non-LEFT ARRAY JOIN can leave the aggregation with no input while the table still has
        /// rows (every array empty), and `sum` over the base rows cannot reproduce that. Whether every
        /// array is empty is unknown here, so decline for the whole setting.
        if (!array_join_node->isLeft() && settings[Setting::empty_result_for_aggregation_by_empty_set])
            return;

        auto * table_node = array_join_node->getTableExpression()->as<TableNode>();
        if (!table_node)
            return;

        /// Exactly one surviving joined expression
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

        /// The checks below establish that the analyzed declared type is the type the read will
        /// execute against; dropping the ARRAY JOIN also drops its type check, so anything less is a
        /// wrong result. Buffer, View and MaterializedView remain uncovered here: they diverge on
        /// master already for a bare sum(length(arr)), tracked in <ISSUE_URL_PLACEHOLDER>.

        /// A storage that opts out of subcolumn optimization may execute the read against a different
        /// schema than the one analyzed here.
        if (!table_node->getStorage()->supportsOptimizationToSubcolumns())
            return;

        /// Transparent wrappers forward both the capability predicate and the read, so the Merge check
        /// below must see the storage that actually executes.
        auto resolved_storage = table_node->getStorage();
        size_t wrapper_hops = 0;
        for (; wrapper_hops < 4; ++wrapper_hops)
        {
            StoragePtr next;
            if (const auto * alias = typeid_cast<const StorageAlias *>(resolved_storage.get()))
                next = alias->tryGetTargetTable();
            else if (const auto * proxy = dynamic_cast<const StorageProxy *>(resolved_storage.get()))
                next = proxy->getNested();
            else
                break;
            if (!next)
                return;
            resolved_storage = std::move(next);
        }
        if (wrapper_hops == 4)
            return;

        /// supportsOptimizationToSubcolumns() on a Merge is the AND over its children, and a MergeTree
        /// child satisfies it whatever its column types are, so it is blind to type heterogeneity. A
        /// Merge declares its own column list, which need not equal any child's.
        if (const auto * storage_merge = typeid_cast<const StorageMerge *>(resolved_storage.get()))
        {
            const auto & analyzed_type = physical_column->getColumnType();
            const auto & column_name = physical_column->getColumnName();
            auto context = getContext();
            /// hasChildTable stops at the first match, so the predicate means "this child is not
            /// provably type-identical" and the whole expression means "some child is unsafe".
            if (storage_merge->hasChildTable([&](const StoragePtr & child)
                {
                    /// Direct children only, so a nested Merge -- or a wrapper around one -- could hide
                    /// a mismatched grandchild. Decline rather than descend.
                    if (typeid_cast<const StorageMerge *>(child.get())
                        || typeid_cast<const StorageAlias *>(child.get())
                        || dynamic_cast<const StorageProxy *>(child.get()))
                        return true;
                    auto child_metadata = child->getInMemoryMetadataPtr(context, false);
                    if (!child_metadata)
                        return true;
                    auto child_column = child_metadata->getColumns().tryGetColumn(
                        GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), column_name);
                    return !child_column || !child_column->type->equals(*analyzed_type);
                }))
                return;
        }

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
