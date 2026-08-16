#include <Interpreters/expressionSourceColumns.h>

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

NamesAndTypes expressionSourceColumns(const ASTPtr & ast, const ColumnsDescription & columns, const ContextPtr & context)
{
    if (!ast)
        return {};

    auto analysis_context = Context::createCopy(context);
    auto expression = buildQueryTree(ast, analysis_context);

    auto storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, columns);
    auto table_node = std::make_shared<TableNode>(storage, analysis_context);

    QueryAnalyzer analyzer(/*only_analyze=*/ true);
    analyzer.resolve(expression, table_node, analysis_context);

    auto global_planner_context
        = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
    auto planner_context = std::make_shared<PlannerContext>(analysis_context, global_planner_context, SelectQueryOptions{});
    collectSetsAndSourceColumns(expression, planner_context, /*keep_alias_columns=*/ false);

    if (const auto * table_expression_data = planner_context->getTableExpressionDataOrNull(table_node))
    {
        /// The columns that are read, not getSelectedColumnsNames(), which reports an ALIAS column under
        /// its own name instead of the columns its expression reads.
        return table_expression_data->getColumns();
    }

    return {};
}

Names expressionSourceColumnNames(const ASTPtr & ast, const ColumnsDescription & columns, const ContextPtr & context)
{
    Names result;
    for (const auto & column : expressionSourceColumns(ast, columns, context))
        result.push_back(column.name);

    return result;
}

Names expressionSourceColumnsInStorage(const ASTPtr & ast, const ColumnsDescription & columns, const ContextPtr & context)
{
    Names result;
    NameSet added;
    for (const auto & column : expressionSourceColumns(ast, columns, context))
    {
        /// Subcolumns are reported as columns of their own, so ask the table which column each is stored in.
        auto resolved = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, column.name);
        const auto & name_in_storage = resolved ? resolved->getNameInStorage() : column.getNameInStorage();

        if (added.emplace(name_in_storage).second)
            result.push_back(name_in_storage);
    }

    return result;
}

}
