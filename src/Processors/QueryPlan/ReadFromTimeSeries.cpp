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
        , aggregator(*input_header, params_)
        , key_columns(params_.keys_size)
        , aggregate_columns(params_.aggregates_size)
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

    Aggregator aggregator;
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;
    Aggregator::AggregatedChunks chunks;
};

class TimeSeriesAggregationStep final : public ITransformingStep
{
public:
    explicit TimeSeriesAggregationStep(QueryPlanStepPtr aggregation_)
        : ITransformingStep(
            aggregation_->getInputHeaders().front(), aggregation_->getOutputHeader(),
            {{.returns_single_stream = false, .preserves_number_of_streams = true, .preserves_sorting = false},
             {.preserves_number_of_rows = false}})
        , aggregation(std::move(aggregation_))
    {
        const auto & step = assert_cast<const AggregatingStep &>(*aggregation);
        if (!step.isFinal() || step.getParams().only_merge)
            throw Exception(ErrorCodes::INCORRECT_DATA, "TimeSeriesAggregation requires raw samples and finalized aggregate results");
    }

    String getName() const override { return "TimeSeriesAggregation"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        /// Finalize each block without merging, spilling, or adaptive aggregation.
        auto params = assert_cast<const AggregatingStep &>(*aggregation).getParams();
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
        params.max_bytes_before_external_group_by = 0;
        params.enable_adaptive_aggregator = false;
        pipeline.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<TimeSeriesAggregationTransform>(header, output_header, params);
        });
    }

    QueryPlanStepPtr clone() const override { return std::make_unique<TimeSeriesAggregationStep>(aggregation->clone()); }

    /// Reuse `AggregatingStep`'s serialization of types, functions, and limits.
    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override
    {
        aggregation->serializeSettings(settings, version);
    }

    void serialize(Serialization & ctx) const override { aggregation->serialize(ctx); }
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx)
    {
        return std::make_unique<TimeSeriesAggregationStep>(AggregatingStep::deserialize(ctx));
    }

private:
    void updateOutputHeader() override
    {
        aggregation->updateInputHeaders(input_headers);
        output_header = aggregation->getOutputHeader();
    }

    QueryPlanStepPtr aggregation;
};

}

ReadFromTimeSeriesStep::ReadFromTimeSeriesStep(QueryPlanPtr query_plan_, ContextPtr read_context_, bool aggregate_samples_in_blocks)
    : ISourceStep(query_plan_->getCurrentHeader())
    , query_plan(std::move(query_plan_))
    , read_context(std::move(read_context_))
{
    if (!aggregate_samples_in_blocks)
        return;

    /// Samples are on the left of every generated join. Replace only their aggregation;
    /// target views can have their own aggregations, which must remain intact.
    auto * node = query_plan->getRootNode();
    while (node)
    {
        if (typeid_cast<const AggregatingStep *>(node->step.get()))
        {
            node->step = std::make_unique<TimeSeriesAggregationStep>(std::move(node->step));
            return;
        }
        node = node->children.empty() ? nullptr : node->children.front();
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing samples aggregation in the generated TimeSeries read plan");
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
