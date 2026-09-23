#include <Processors/QueryPlan/GPUAggregatingStep.h>

#if USE_GPU

#include <AggregateFunctions/IAggregateFunction.h>
#include <GPU/GPUTypeMapping.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Transforms/GPUAggregatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

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

std::optional<std::vector<GPU::GPUAggregationKind>> gpuAggregationsOf(const Aggregator::Params & params)
{
    std::vector<GPU::GPUAggregationKind> aggregations;
    aggregations.reserve(params.aggregates.size());

    for (const auto & aggregate : params.aggregates)
    {
        if (!aggregate.parameters.empty() || aggregate.argument_names.size() != 1)
            return {};

        const auto aggregation = GPU::aggregationOf(aggregate.function->getName());
        if (!aggregation)
            return {};

        aggregations.push_back(*aggregation);
    }

    return aggregations;
}

GPUAggregatingStep::GPUAggregatingStep(const SharedHeader & input_header_, Aggregator::Params params_, size_t batch_bytes_)
    : ITransformingStep(
        input_header_,
        std::make_shared<const Block>(params_.getHeader(*input_header_, /*final=*/true)),
        getTraits())
    , params(std::move(params_))
    , batch_bytes(batch_bytes_)
{
}

bool GPUAggregatingStep::canRunOnDevice(const Block & input_header, const Aggregator::Params & params)
{
    if (params.only_merge || params.overflow_row)
        return false;

    if (params.max_rows_to_group_by != 0)
        return false;

    if (params.aggregates.empty())
        return false;

    const auto aggregations = gpuAggregationsOf(params);
    if (!aggregations)
        return false;

    DataTypes argument_types;
    DataTypes result_types;
    argument_types.reserve(params.aggregates.size());
    result_types.reserve(params.aggregates.size());

    for (const auto & aggregate : params.aggregates)
    {
        const auto * argument = input_header.findByName(aggregate.argument_names.front());
        if (!argument)
            return false;

        argument_types.push_back(argument->type);
        result_types.push_back(aggregate.function->getResultType());
    }

    if (params.keys.empty())
    {
        for (size_t i = 0; i < argument_types.size(); ++i)
        {
            if (!GPU::canReduceOnDevice(*argument_types[i], *result_types[i], (*aggregations)[i]))
                return false;
        }

        return true;
    }

    DataTypes key_types;
    key_types.reserve(params.keys.size());

    for (const auto & key : params.keys)
    {
        const auto * key_column = input_header.findByName(key);
        if (!key_column)
            return false;

        key_types.push_back(key_column->type);
    }

    return GPU::canGroupByReduceOnDevice(key_types, argument_types, result_types, *aggregations);
}

void GPUAggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.dropTotalsAndExtremes();

    pipeline.resize(1);

    pipeline.addTransform(std::make_shared<GPUAggregatingTransform>(
        pipeline.getSharedHeader(), getOutputHeader(), params, batch_bytes, input_grouped));
}

void GPUAggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings);
    settings.out << settings.detail_prefix << "Batch: " << batch_bytes << " bytes\n";
    if (input_grouped)
        settings.out << settings.detail_prefix << "Input grouped by the read\n";
}

void GPUAggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
    map.add("Batch Bytes", batch_bytes);
    map.add("Input Grouped", input_grouped);
}

void GPUAggregatingStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(params.getHeader(*input_headers.front(), /*final=*/true));
}

}

#endif
