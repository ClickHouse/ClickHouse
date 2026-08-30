#include <Interpreters/Streaming/Utils.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>

#include <Parsers/IAST.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>

#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/TableNode.h>

#include <Planner/CollectTableExpressionData.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>

#include <Storages/StorageDummy.h>

namespace DB
{

bool isIdleExpired(
    const std::chrono::steady_clock::time_point & now,
    const std::chrono::steady_clock::time_point & last_active,
    const WatermarkSettingsPtr & watermark)
{
    if (watermark->idle_timeout.count() <= 0)
        return false;

    return now > last_active + watermark->idle_timeout;
}

ActionsDAG buildWatermarkActionsDAG(
    const ASTPtr & watermark_expression,
    const Block & header,
    const ContextPtr & context)
{
    chassert(watermark_expression);

    auto execution_context = Context::createCopy(context);
    auto dummy_storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, ColumnsDescription(header.getNamesAndTypesList()));
    auto fake_table = std::make_shared<TableNode>(std::move(dummy_storage), execution_context);

    auto expression = buildQueryTree(watermark_expression->clone(), execution_context);
    QueryAnalyzer(/*only_analyze=*/true).resolve(expression, fake_table, execution_context);

    auto planner_context = std::make_shared<PlannerContext>(
        execution_context,
        std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}),
        SelectQueryOptions{});
    collectSourceColumns(expression, planner_context, /*keep_alias_columns=*/false);

    auto [dag, _] = buildActionsDAGFromExpressionNode(expression, header.getColumnsWithTypeAndName(), planner_context, {});
    return std::move(dag);
}

Names collectWatermarkSourceColumns(
    const ASTPtr & watermark_expression,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context)
{
    Block header;
    for (const auto & column : available_columns)
        header.insert({column.type->createColumn(), column.type, column.name});

    return buildWatermarkActionsDAG(watermark_expression, header, context).getRequiredColumnsNames();
}


}
