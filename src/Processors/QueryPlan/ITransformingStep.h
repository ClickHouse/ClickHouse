#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Step which has single input and single output data stream.
/// It doesn't mean that pipeline has single port before or after such step.
class ITransformingStep : public IQueryPlanStep
{
public:
    /// This flags are used to automatically set properties for output stream.
    /// They are specified in constructor and cannot be changed.
    struct DataStreamTraits
    {
        /// True if pipeline has single output port after this step.
        /// Examples: MergeSortingStep, AggregatingStep
        bool returns_single_stream;

        /// Won't change the number of ports for pipeline.
        /// Examples: true for ExpressionStep, false for MergeSortingStep
        bool preserves_number_of_streams;

        /// Doesn't change row order.
        /// Examples: true for FilterStep, false for PartialSortingStep
        bool preserves_sorting;
    };

    /// This flags are used by QueryPlan optimizers.
    /// They can be changed after some optimizations.
    struct TransformTraits
    {
        /// Won't change the total number of rows.
        /// Examples: true for ExpressionStep (without join or array join), false for FilterStep
        bool preserves_number_of_rows;
    };

    struct Traits
    {
        DataStreamTraits data_stream_traits;
        TransformTraits transform_traits;
    };

    ITransformingStep(DataStream input_stream, Block output_header, Traits traits, bool collect_processors_ = true);

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    virtual void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) = 0;

    const TransformTraits & getTransformTraits() const { return transform_traits; }
    const DataStreamTraits & getDataStreamTraits() const { return data_stream_traits; }

    /// Updates the input stream of the given step. Used during query plan optimizations.
    /// It won't do any validation of a new stream, so it is your responsibility to ensure that this update doesn't break anything
    /// (e.g. you update data stream traits or correctly remove / add columns).
    void updateInputStream(DataStream input_stream)
    {
        input_streams.clear();
        input_streams.emplace_back(std::move(input_stream));

        updateOutputStream();
    }

    void describePipeline(FormatSettings & settings) const override;

    /// Enforcement is supposed to be done through the special settings that will be taken into account by remote nodes during query planning (e.g. force_aggregation_in_order).
    /// Should be called only if data_stream_traits.can_enforce_sorting_properties_in_distributed_query == true.
    virtual void adjustSettingsToEnforceSortingPropertiesInDistributedQuery(ContextMutablePtr) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }

protected:
    /// Create output stream from header and traits.
    static DataStream createOutputStream(
            const DataStream & input_stream,
            Block output_header,
            const DataStreamTraits & stream_traits);

    TransformTraits transform_traits;

private:
    virtual void updateOutputStream() = 0;

    /// If we should collect processors got after pipeline transformation.
    bool collect_processors;

    const DataStreamTraits data_stream_traits;
};

}
