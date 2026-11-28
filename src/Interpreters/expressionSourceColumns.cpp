#include <Interpreters/expressionSourceColumns.h>

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

struct ResolvedExpression
{
    QueryTreeNodePtr expression;
    QueryTreeNodePtr table_node;
    ContextMutablePtr context;
};

/// `ast` resolved by the query analyzer over a fake table with `columns`.
ResolvedExpression resolveExpression(const ASTPtr & ast, const ColumnsDescription & columns, const ContextPtr & context)
{
    auto analysis_context = Context::createCopy(context);
    auto expression = buildQueryTree(ast, analysis_context);

    auto storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, columns);
    auto table_node = std::make_shared<TableNode>(storage, analysis_context);

    QueryAnalyzer analyzer(/*only_analyze=*/ true);
    analyzer.resolve(expression, table_node, analysis_context);

    return {std::move(expression), std::move(table_node), std::move(analysis_context)};
}

/// Calls `callback` for every column of the table `node` refers to, including through an ALIAS column (its
/// expression is a child of its node) and through a function folded to a constant (`toTypeName(x)`, whose
/// source expression is not a child). A lambda argument has the lambda as its source and is skipped.
template <typename Callback>
void forEachTableColumn(const QueryTreeNodePtr & node, const QueryTreeNodePtr & table_node, Callback && callback)
{
    if (!node)
        return;

    if (const auto * column_node = node->as<ColumnNode>(); column_node && column_node->getColumnSourceOrNull().get() == table_node.get())
        callback(*column_node);
    else if (const auto * constant_node = node->as<ConstantNode>(); constant_node && constant_node->hasSourceExpression())
        forEachTableColumn(constant_node->getSourceExpression(), table_node, callback);

    for (const auto & child : node->getChildren())
        forEachTableColumn(child, table_node, callback);
}

}

NamesAndTypes expressionSourceColumns(const ASTPtr & ast, const ColumnsDescription & columns, const ContextPtr & context)
{
    if (!ast)
        return {};

    auto [expression, table_node, analysis_context] = resolveExpression(ast, columns, context);

    auto global_planner_context
        = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
    auto planner_context = std::make_shared<PlannerContext>(analysis_context, global_planner_context, SelectQueryOptions{});
    collectSetsAndSourceColumns(expression, planner_context, /*keep_alias_columns=*/ false);

    NamesAndTypes result;
    NameSet seen;
    /// The columns that are read, not getSelectedColumnsNames(), which reports an ALIAS column under its own name.
    if (const auto * table_expression_data = planner_context->getTableExpressionDataOrNull(table_node))
    {
        result = table_expression_data->getColumns();
        for (const auto & column : result)
            seen.insert(column.name);
    }

    /// The read set omits the columns of a folded constant.
    forEachTableColumn(expression, table_node, [&](const ColumnNode & column)
    {
        if (!column.hasExpression() && seen.emplace(column.getColumnName()).second)
            result.emplace_back(column.getColumnName(), column.getColumnType());
    });

    return result;
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

bool expressionReadsColumn(const ASTPtr & ast, const ColumnsDescription & columns, const String & column_name, const ContextPtr & context)
{
    if (!ast)
        return false;

    auto [expression, table_node, analysis_context] = resolveExpression(ast, columns, context);

    bool found = false;
    forEachTableColumn(expression, table_node, [&](const ColumnNode & column)
    {
        if (found)
            return;

        const auto & name = column.getColumnName();
        if (name == column_name)
        {
            found = true;
            return;
        }

        /// A subcolumn of `column_name` (`a.x`), but not a column of its own that is named with a dot (`a.v`).
        auto resolved = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name);
        found = resolved && resolved->isSubcolumn() && resolved->getNameInStorage() == column_name;
    });

    return found;
}

}
