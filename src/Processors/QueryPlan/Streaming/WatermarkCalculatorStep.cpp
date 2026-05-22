#include <Processors/QueryPlan/Streaming/WatermarkCalculatorStep.h>

#include <Processors/Port.h>

#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>

#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableNode.h>

#include <Planner/CollectTableExpressionData.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Processors/Streaming/WatermarkCalculatorTransform.h>

#include <Storages/StorageDummy.h>

#include <Core/Block.h>

namespace DB
{

namespace
{

ITransformingStep::Traits getCalculatorTraits()
{
    return ITransformingStep::Traits
    {
        .data_stream_traits = {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        .transform_traits = {
            .preserves_number_of_rows = false,
        },
    };
}

ActionsDAG buildWatermarkActionsDAG(
    const QueryTreeNodePtr & watermark_expression,
    const Block & header,
    const ContextPtr & context)
{
    chassert(watermark_expression);

    auto execution_context = Context::createCopy(context);
    auto dummy_storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, ColumnsDescription(header.getNamesAndTypesList()));
    QueryTreeNodePtr fake_table = std::make_shared<TableNode>(std::move(dummy_storage), execution_context);

    auto expression = watermark_expression->clone();
    QueryAnalyzer(/*only_analyze=*/true).resolve(expression, fake_table, execution_context);

    auto planner_context = std::make_shared<PlannerContext>(
        execution_context,
        std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}),
        SelectQueryOptions{});
    collectSourceColumns(expression, planner_context, /*keep_alias_columns=*/false);

    auto [dag, _] = buildActionsDAGFromExpressionNode(expression, header.getColumnsWithTypeAndName(), planner_context, {});
    return std::move(dag);
}

}

WatermarkCalculatorStep::WatermarkCalculatorStep(SharedHeader input_header_, WatermarkSettingsPtr watermark_, ContextPtr context_)
    : ITransformingStep(input_header_, input_header_, getCalculatorTraits())
    , watermark(std::move(watermark_))
    , context(std::move(context_))
{
}

void WatermarkCalculatorStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (!watermark)
        return;

    pipeline.addSimpleTransform([&] (const SharedHeader & header)
    {
        auto watermark_expression = buildWatermarkActionsDAG(watermark->expression, *input_headers.front(), context);
        return std::make_shared<WatermarkCalculatorTransform>(header, watermark->column, std::move(watermark_expression), context);
    });
}

void WatermarkCalculatorStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

QueryPlanStepPtr WatermarkCalculatorStep::clone() const
{
    return std::make_unique<WatermarkCalculatorStep>(input_headers.front(), watermark, context);
}

}
