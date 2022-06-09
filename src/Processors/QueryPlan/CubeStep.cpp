#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/Transforms/CubeTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

CubeStep::CubeStep(const DataStream & input_stream_, AggregatingTransformParamsPtr params_)
    : ITransformingStep(input_stream_, appendGroupingSetColumn(params_->getHeader()), getTraits())
    , keys_size(params_->params.keys_size)
    , params(std::move(params_))
{
    /// Aggregation keys are distinct
    for (auto key : params->params.keys)
        output_stream->distinct_columns.insert(params->params.src_header.getByPosition(key).name);
}

ProcessorPtr addGroupingSetForTotals(const Block & header, const BuildQueryPipelineSettings & settings, UInt64 grouping_set_number)
{
    auto dag = std::make_shared<ActionsDAG>(header.getColumnsWithTypeAndName());

    auto grouping_col = ColumnUInt64::create(1, grouping_set_number);
    const auto * grouping_node = &dag->addColumn(
        {ColumnPtr(std::move(grouping_col)), std::make_shared<DataTypeUInt64>(), "__grouping_set"});

    grouping_node = &dag->materializeNode(*grouping_node);
    auto & index = dag->getIndex();
    index.insert(index.begin(), grouping_node);

    auto expression = std::make_shared<ExpressionActions>(dag, settings.getActionsSettings());
    return std::make_shared<ExpressionTransform>(header, expression);
}

void CubeStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipelineBuilder::StreamType::Totals)
            return addGroupingSetForTotals(header, settings, (UInt64(1) << keys_size) - 1);

        return std::make_shared<CubeTransform>(header, std::move(params));
    });
}

const Aggregator::Params & CubeStep::getParams() const
{
    return params->params;
}

}
