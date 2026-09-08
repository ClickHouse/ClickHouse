#include <Processors/QueryPlan/ReadFromTimeSeries.h>

#include <Interpreters/AggregatedDataVariants.h>
#include <Interpreters/Aggregator.h>
#include <Processors/IInflatingTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// A non-`FINAL` read may return several rows for a series. Assemble each input block independently
/// so a `LIMIT` can stop the samples read even when its sorting key does not start with `id`.
class TimeSeriesAggregationTransform final : public IInflatingTransform
{
public:
    TimeSeriesAggregationTransform(SharedHeader input_header, SharedHeader output_header, const Aggregator::Params & params_)
        : IInflatingTransform(input_header, output_header)
        , params(params_)
        , aggregator(*input_header, params)
        , key_columns(params.keys_size)
        , aggregate_columns(params.aggregates_size)
    {
    }

    String getName() const override { return "TimeSeriesAggregationTransform"; }

private:
    void consume(Chunk chunk) override
    {
        if (!chunk.getNumRows())
            return;

        AggregatedDataVariants data;
        bool no_more_keys = false;
        auto rows = chunk.getNumRows();
        if (!aggregator.executeOnBlock(chunk.detachColumns(), 0, rows, data, key_columns, aggregate_columns, no_more_keys, nullptr))
            input.close();
        chunks = aggregator.convertToChunks(data, /* final= */ true);
    }

    bool canGenerate() override { return !chunks.empty(); }

    Chunk generate() override
    {
        auto chunk = std::move(chunks.front().chunk);
        chunks.pop_front();
        return chunk;
    }

    Aggregator::Params params;
    Aggregator aggregator;
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;
    Aggregator::AggregatedChunks chunks;
};

class TimeSeriesAggregationStep final : public ITransformingStep
{
public:
    TimeSeriesAggregationStep(const SharedHeader & input_header, const AggregatingStep & aggregation)
        : ITransformingStep(
            input_header,
            std::make_shared<const Block>(aggregation.getParams().getHeader(*input_header, /* final= */ true)),
            {{.returns_single_stream = false, .preserves_number_of_streams = true, .preserves_sorting = false},
             {.preserves_number_of_rows = false}})
        , params(aggregation.getParams())
        , serialized_aggregation(aggregation.clone())
    {
        /// There is no cross-block merge, spill, or adaptive aggregation. Each transform owns one
        /// block's states, finalizes them, and releases them before accepting the next block.
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
        params.max_bytes_before_external_group_by = 0;
        params.enable_adaptive_aggregator = false;
    }

    String getName() const override { return "TimeSeriesAggregation"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        pipeline.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<TimeSeriesAggregationTransform>(header, output_header, params);
        });
    }

    QueryPlanStepPtr clone() const override
    {
        return std::make_unique<TimeSeriesAggregationStep>(input_headers.front(), assert_cast<const AggregatingStep &>(*serialized_aggregation));
    }

    /// Reuse the aggregation payload and its settings so serialized plans preserve the input
    /// types, aggregate functions, and limits without maintaining a second encoding of them.
    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override
    {
        serialized_aggregation->serializeSettings(settings, version);
    }

    void serialize(Serialization & ctx) const override { serialized_aggregation->serialize(ctx); }
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx)
    {
        auto aggregation = AggregatingStep::deserialize(ctx);
        const auto & step = assert_cast<const AggregatingStep &>(*aggregation);
        if (!step.isFinal() || step.getParams().only_merge)
            throw Exception(ErrorCodes::INCORRECT_DATA, "TimeSeriesAggregation requires raw samples and finalized aggregate results");
        return std::make_unique<TimeSeriesAggregationStep>(step.getInputHeaders().front(), step);
    }

private:
    void updateOutputHeader() override
    {
        output_header = std::make_shared<const Block>(params.getHeader(*input_headers.front(), /* final= */ true));
    }

    Aggregator::Params params;
    QueryPlanStepPtr serialized_aggregation;
};

void aggregateSamplesInBlocks(QueryPlan & plan)
{
    /// In the generated read query the samples subquery anchors the left side of every join.
    /// Replace only its aggregation, before optimization can reorder joins. Stop there: target
    /// tables can themselves be views with aggregations whose semantics must be preserved.
    auto * node = plan.getRootNode();
    while (node)
    {
        if (const auto * aggregation = typeid_cast<const AggregatingStep *>(node->step.get()))
        {
            chassert(aggregation->isFinal());
            node->step = std::make_unique<TimeSeriesAggregationStep>(aggregation->getInputHeaders().front(), *aggregation);
            return;
        }
        node = node->children.empty() ? nullptr : node->children.front();
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing samples aggregation in the generated TimeSeries read plan");
}

}

ReadFromTimeSeriesStep::ReadFromTimeSeriesStep(QueryPlanPtr query_plan_, ContextPtr read_context_, bool aggregate_samples_in_blocks)
    : ISourceStep(query_plan_->getCurrentHeader())
    , query_plan(std::move(query_plan_))
    , read_context(std::move(read_context_))
{
    if (aggregate_samples_in_blocks)
        aggregateSamplesInBlocks(*query_plan);
}

void ReadFromTimeSeriesStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} shouldn't be called: the step must be replaced with its plan during optimization", __PRETTY_FUNCTION__);
}

QueryPlanPtr ReadFromTimeSeriesStep::extractQueryPlan()
{
    chassert(query_plan);
    auto qp = std::move(query_plan);
    query_plan.reset();
    return qp;
}

void registerTimeSeriesAggregationStep(QueryPlanStepRegistry & registry);
void registerTimeSeriesAggregationStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("TimeSeriesAggregation", TimeSeriesAggregationStep::deserialize);
}

}
