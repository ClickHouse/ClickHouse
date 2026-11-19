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

/// `QueryAnalyzer` folds a type-dependent function (e.g. `toTypeName(x)`) to a `ConstantNode`, keeping the
/// original expression in its source expression - which is not a child node, so the read set collected by
/// `collectSetsAndSourceColumns` misses the columns it reads. Walk those source expressions so a decision
/// spanning two schemas (is this expression affected by a type change?) still sees the real dependency.
void collectColumnsThroughConstantSources(
    const QueryTreeNodePtr & node, const ColumnsDescription & columns, NamesAndTypes & result, NameSet & seen)
{
    if (!node)
        return;

    if (const auto * column_node = node->as<ColumnNode>())
    {
        /// An ALIAS column keeps its expression as a child, so the generic descent below expands it.
        if (!column_node->hasExpression() && columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, column_node->getColumnName())
            && seen.emplace(column_node->getColumnName()).second)
            result.emplace_back(column_node->getColumnName(), column_node->getColumnType());
    }
    else if (const auto * constant_node = node->as<ConstantNode>(); constant_node && constant_node->hasSourceExpression())
    {
        collectColumnsThroughConstantSources(constant_node->getSourceExpression(), columns, result, seen);
    }

    for (const auto & child : node->getChildren())
        collectColumnsThroughConstantSources(child, columns, result, seen);
}

}

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

    NamesAndTypes result;
    NameSet seen;
    if (const auto * table_expression_data = planner_context->getTableExpressionDataOrNull(table_node))
    {
        /// The columns that are read, not getSelectedColumnsNames(), which reports an ALIAS column under
        /// its own name instead of the columns its expression reads.
        result = table_expression_data->getColumns();
        for (const auto & column : result)
            seen.insert(column.name);
    }

    /// Add columns folded into constants (e.g. toTypeName(x)) that the read set above omits.
    collectColumnsThroughConstantSources(expression, columns, result, seen);

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

}
