#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/Transforms/CubeTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

CubeStep::CubeStep(const Header & input_header_, Aggregator::Params params_, bool final_, bool use_nulls_)
    : ITransformingStep(input_header_, generateOutputHeader(params_.getHeader(input_header_, final_), params_.keys, use_nulls_), getTraits())
    , keys_size(params_.keys_size)
    , params(std::move(params_))
    , final(final_)
    , use_nulls(use_nulls_)
{
}

ProcessorPtr addGroupingSetForTotals(const Block & header, const Names & keys, bool use_nulls, const BuildQueryPipelineSettings & settings, UInt64 grouping_set_number)
{
    ActionsDAG dag(header.getColumnsWithTypeAndName());
    auto & outputs = dag.getOutputs();

    if (use_nulls)
    {
        auto to_nullable = FunctionFactory::instance().get("toNullable", nullptr);
        for (const auto & key : keys)
        {
            const auto * node = dag.getOutputs()[header.getPositionByName(key)];
            if (node->result_type->canBeInsideNullable())
            {
                dag.addOrReplaceInOutputs(dag.addFunction(to_nullable, { node }, node->result_name));
            }
        }
    }

    auto grouping_col = ColumnUInt64::create(1, grouping_set_number);
    const auto * grouping_node = &dag.addColumn(
        {ColumnPtr(std::move(grouping_col)), std::make_shared<DataTypeUInt64>(), "__grouping_set"});

    grouping_node = &dag.materializeNode(*grouping_node);
    outputs.insert(outputs.begin(), grouping_node);

    auto expression = std::make_shared<ExpressionActions>(std::move(dag), settings.getActionsSettings());
    return std::make_shared<ExpressionTransform>(header, expression);
}

void CubeStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipelineBuilder::StreamType::Totals)
            return addGroupingSetForTotals(header, params.keys, use_nulls, settings, (UInt64(1) << keys_size) - 1);

        auto transform_params = std::make_shared<AggregatingTransformParams>(header, std::move(params), final);
        return std::make_shared<CubeTransform>(header, std::move(transform_params), use_nulls);
    });
}

const Aggregator::Params & CubeStep::getParams() const
{
    return params;
}

void CubeStep::updateOutputHeader()
{
    output_header = generateOutputHeader(params.getHeader(input_headers.front(), final), params.keys, use_nulls);
}
}
