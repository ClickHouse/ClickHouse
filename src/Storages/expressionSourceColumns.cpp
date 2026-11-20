#include <Storages/expressionSourceColumns.h>

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

Names expressionSourceColumns(
    const ASTPtr & ast, const ColumnsDescription & columns, const ContextPtr & context, bool keep_alias_columns, const StorageID & table_id)
{
    if (!ast)
        return {};

    auto analysis_context = Context::createCopy(context);
    auto expression = buildQueryTree(ast, analysis_context);

    auto storage = std::make_shared<StorageDummy>(table_id, columns);
    auto table_node = std::make_shared<TableNode>(storage, analysis_context);

    QueryAnalyzer analyzer(/*only_analyze=*/ true);
    analyzer.resolve(expression, table_node, analysis_context);

    auto global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
    auto planner_context = std::make_shared<PlannerContext>(analysis_context, global_planner_context, SelectQueryOptions{});
    collectSetsAndSourceColumns(expression, planner_context, keep_alias_columns);

    const auto * table_expression_data = planner_context->getTableExpressionDataOrNull(table_node);
    if (!table_expression_data)
        return {};

    /// a kept alias is registered as selected and its base columns as not selected, an expanded one the other way round
    return keep_alias_columns ? table_expression_data->getSelectedColumnsNames() : table_expression_data->getColumnNames();
}

}
