#include <Storages/expressionSourceColumns.h>

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

}

Names expressionSourceColumns(
    const ASTPtr & ast,
    const ColumnsDescription & columns,
    const ContextPtr & context,
    bool keep_alias_columns,
    const StorageSnapshotPtr & storage_snapshot,
    const std::optional<StorageID> & table_id)
{
    if (!ast)
        return {};

    auto analysis_context = Context::createCopy(context);
    auto expression = buildQueryTree(ast, analysis_context);

    auto name = table_id ? *table_id : (storage_snapshot ? storage_snapshot->storage.getStorageID() : StorageID{"dummy", "dummy"});
    auto storage = std::make_shared<StorageDummy>(name, columns, storage_snapshot);
    auto table_node = std::make_shared<TableNode>(storage, analysis_context);

    QueryAnalyzer analyzer(/*only_analyze=*/ true);
    analyzer.resolve(expression, table_node, analysis_context);

    auto global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
    auto planner_context = std::make_shared<PlannerContext>(analysis_context, global_planner_context, SelectQueryOptions{});
    collectSetsAndSourceColumns(expression, planner_context, keep_alias_columns);
    collectFoldedSourceColumns(expression, planner_context, keep_alias_columns);

    const auto * table_expression_data = planner_context->getTableExpressionDataOrNull(table_node);
    if (!table_expression_data)
        return {};

    /// a kept alias is registered as selected and its base columns as not selected, an expanded one the other way round
    return keep_alias_columns ? table_expression_data->getSelectedColumnsNames() : table_expression_data->getColumnNames();
}

}
