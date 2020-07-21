#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

/// Step which has single input and single output data stream.
/// It doesn't mean that pipeline has single port before or after such step.
class ITransformingStep : public IQueryPlanStep
{
public:
    struct DataStreamTraits
    {
        /// Keep distinct_columns unchanged.
        /// Examples: true for LimitStep, false for ExpressionStep with ARRAY JOIN
        /// It some columns may be removed from result header, call updateDistinctColumns
        bool preserves_distinct_columns;

        /// True if pipeline has single output port after this step.
        /// Examples: MergeSortingStep, AggregatingStep
        bool returns_single_stream;

        /// Won't change the number of ports for pipeline.
        /// Examples: true for ExpressionStep, false for MergeSortingStep
        bool preserves_number_of_streams;
    };

    ITransformingStep(DataStream input_stream, Block output_header, DataStreamTraits traits, bool collect_processors_ = true);

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines) override;

    virtual void transformPipeline(QueryPipeline & pipeline) = 0;

    void describePipeline(FormatSettings & settings) const override;

protected:
    /// Clear distinct_columns if res_header doesn't contain all of them.
    static void updateDistinctColumns(const Block & res_header, NameSet & distinct_columns);

private:
    /// We collect processors got after pipeline transformation.
    Processors processors;
    bool collect_processors;
};

}
