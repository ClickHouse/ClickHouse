#include <Processors/QueryPlan/GPUAggregatingStep.h>

#if USE_GPU

#include <AggregateFunctions/IAggregateFunction.h>
#include <GPU/GPUAggregation.h>
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
    /// A merge-only aggregator is handed states rather than values, and an overflow row is a
    /// second output row this step cannot produce.
    if (params.only_merge || params.overflow_row)
        return false;

    /// A group-count limit cannot fire over the single group of a keyless aggregation, and over the
    /// groups of a keyed one it is the `Aggregator` that establishes it - `no_more_keys` and the
    /// overflow row are its machinery. So do not take the aggregation over while it is set, on
    /// either path.
    if (params.max_rows_to_group_by != 0)
        return false;

    if (params.aggregates.empty())
        return false;

    DataTypes argument_types;
    DataTypes result_types;
    argument_types.reserve(params.aggregates.size());
    result_types.reserve(params.aggregates.size());

    for (const auto & aggregate : params.aggregates)
    {
        /// `sum` and nothing else - and by its own name, so that a combinator (`sumIf`,
        /// `sumDistinct`) or a parametric form does not slip through. A `Nullable` argument keeps
        /// the name, and is turned away by the type checks below.
        if (aggregate.function->getName() != "sum" || !aggregate.parameters.empty() || aggregate.argument_names.size() != 1)
            return false;

        const auto * argument = input_header.findByName(aggregate.argument_names.front());
        if (!argument)
            return false;

        argument_types.push_back(argument->type);
        result_types.push_back(aggregate.function->getResultType());
    }

    /// Without keys the device reduces each argument column to a scalar, with them it groups - two
    /// different pieces of cuDF, each with its own type support, so they are asked separately.
    if (params.keys.empty())
    {
        for (size_t i = 0; i < argument_types.size(); ++i)
        {
            if (!GPU::canSumOnDevice(*argument_types[i], *result_types[i]))
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

    return GPU::canGroupBySumOnDevice(key_types, argument_types, result_types);
}

void GPUAggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// An aggregation computes its own totals and extremes, if it has any; this one has none,
    /// since `WITH TOTALS` is not eligible for it.
    pipeline.dropTotalsAndExtremes();

    /// Everything read has to reach the one accumulator, which then produces the query's result.
    /// Aggregating per stream and merging the partial results afterwards - what the CPU path does
    /// with its per-thread hash tables - is the obvious next step and not needed to begin with:
    /// the device is a single resource no matter how many streams feed it, and the reading below
    /// keeps its threads either way. It would also need a merge of the partial results above this
    /// step, which for the keyed case is a second groupby on the device rather than a sum on the
    /// host - the accumulator already does exactly that between its own batches.
    pipeline.resize(1);

    pipeline.addTransform(std::make_shared<GPUAggregatingTransform>(
        pipeline.getSharedHeader(), getOutputHeader(), params, batch_bytes));
}

void GPUAggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings);
    settings.out << settings.detail_prefix << "Batch: " << batch_bytes << " bytes\n";
}

void GPUAggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
    map.add("Batch Bytes", batch_bytes);
}

void GPUAggregatingStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(params.getHeader(*input_headers.front(), /*final=*/true));
}

}

#endif
