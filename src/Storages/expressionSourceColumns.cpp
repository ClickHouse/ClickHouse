#include <Storages/expressionSourceColumns.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableNode.h>
#include <Interpreters/Context.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/PlannerContext.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageDummy.h>

namespace DB
{

namespace
{

/// folding turns e.g. toTypeName(c) into a constant, which keeps the folded expression, and that expression still binds c
void collectFoldedSourceColumns(const QueryTreeNodePtr & node, PlannerContextPtr & planner_context, bool keep_alias_columns)
{
    /// a folded scalar subquery is not correlated, and a kept alias pins its own base columns
    auto node_type = node->getNodeType();
    if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION || node_type == QueryTreeNodeType::COLUMN)
        return;

    if (const auto * constant = node->as<ConstantNode>(); constant && constant->hasSourceExpression())
    {
        auto source_expression = constant->getSourceExpression();
        /// the source of a folded scalar subquery is the subquery
        auto source_type = source_expression->getNodeType();
        if (source_type == QueryTreeNodeType::QUERY || source_type == QueryTreeNodeType::UNION)
            return;

        collectSourceColumns(source_expression, planner_context, keep_alias_columns);
        collectFoldedSourceColumns(source_expression, planner_context, keep_alias_columns);
        return;
    }

    for (const auto & child : node->getChildren())
        if (child)
            collectFoldedSourceColumns(child, planner_context, keep_alias_columns);
}

/// columns that subqueries, also folded ones, read from an accepted table;
/// an alias column pins only itself, because RENAME rewrites its expression
void collectReadsFrom(
    const QueryTreeNodePtr & node,
    const QueryTreeNodePtr & outer_table,
    const std::function<bool(const StorageID &)> & reads_from,
    Names & columns)
{
    if (const auto * column = node->as<ColumnNode>())
    {
        const auto source = column->getColumnSourceOrNull();
        const auto * table = source ? source->as<TableNode>() : nullptr;
        if (table && source != outer_table && reads_from(table->getStorageID()))
            columns.push_back(column->getColumnName());
        return;
    }

    if (const auto * constant = node->as<ConstantNode>(); constant && constant->hasSourceExpression())
        collectReadsFrom(constant->getSourceExpression(), outer_table, reads_from, columns);

    for (const auto & child : node->getChildren())
        if (child)
            collectReadsFrom(child, outer_table, reads_from, columns);
}

}

Names expressionSourceColumns(
    const ASTPtr & ast,
    const ColumnsDescription & columns,
    const ContextPtr & context,
    bool keep_alias_columns,
    const StorageSnapshotPtr & storage_snapshot,
    const std::optional<StorageID> & table_id,
    const std::function<bool(const StorageID &)> & reads_from)
{
    if (!ast)
        return {};

    auto analysis_context = Context::createCopy(context);
    /// the optimization replaces a scalar subquery of a subquery by `__getScalar`, which hides its reads
    if (reads_from)
        analysis_context->setSetting("enable_scalar_subquery_optimization", false);
    auto expression = buildQueryTree(ast, analysis_context);

    auto name = table_id ? *table_id : (storage_snapshot ? storage_snapshot->storage.getStorageID() : StorageID{"dummy", "dummy"});
    auto storage = std::make_shared<StorageDummy>(name, columns, storage_snapshot);
    auto table_node = std::make_shared<TableNode>(storage, analysis_context);

    QueryAnalyzer analyzer(/*only_analyze=*/ true);
    analyzer.resolve(expression, table_node, analysis_context);

    auto global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
    auto planner_context = std::make_shared<PlannerContext>(analysis_context, global_planner_context, SelectQueryOptions{});
    collectSetsAndSourceColumns(expression, planner_context, keep_alias_columns);
    if (reads_from)
        collectFoldedSourceColumns(expression, planner_context, keep_alias_columns);

    Names result;
    /// a kept alias is registered as selected and its base columns as not selected, an expanded one the other way round
    if (const auto * table_expression_data = planner_context->getTableExpressionDataOrNull(table_node))
        result = keep_alias_columns ? table_expression_data->getSelectedColumnsNames() : table_expression_data->getColumnNames();

    if (reads_from)
        collectReadsFrom(expression, table_node, reads_from, result);
    return result;
}

}
