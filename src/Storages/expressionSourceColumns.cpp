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

    /// kept alias columns are only in the selected list, expanded ones only register their base columns
    Names result = table_expression_data->getColumnNames();
    NameSet seen(result.begin(), result.end());
    for (const auto & name : table_expression_data->getSelectedColumnsNames())
        if (seen.insert(name).second)
            result.push_back(name);
    return result;
}

}
